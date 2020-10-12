/**********************************************************************************************/
/*Program: pl_annual_build.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*modified for PL: Heidi Cohen
/*Date: 05/2018 09/2019
/*Purpose: Generate the annual PL TAF using the monthly MCP TAF tables
/*Mod: 
/*Notes: Program includes annual_macro.sas to create each
/*       of the five annual tables
/**********************************************************************************************/
/*get T-MSIS configuration */
%let tms_config_macro="/sasdata/users/&sysuserid/tmsislockdown/config/tms_config.sas";
%include &tms_config_macro;

/*get data analytics specific configuration */
%let da_config_macro="/sasdata/users/&sysuserid/tmsislockdown/config/da_config.sas";
%include &da_config_macro;

*********************************************************************
*  SET MACRO PARAMETER FOR TMSISSHARE/TMSISLOCKDOWN                 *
*********************************************************************;

%global TMSIS;

%macro setENV;

	%if %symexist(_SASPROGRAMFILE) %then 
	%do;
		%if (%sysfunc(find(&_SASPROGRAMFILE,%str(lock))) GE 1) %then 
		   %let TMSIS = tmsislockdown; %else
		%if (%sysfunc(find(&_SASPROGRAMFILE,%str(shar))) GE 1) %then 
			   %let TMSIS =  tmsisshare; 
	%end; 
	%else
	%if %sysevalf(%superq(TMSIS)=,boolean) %then %do;
	       %let TMSIS = tmsislockdown;
	%end;

%mend setENV;

%setENV; 

%put TMSIS=&TMSIS;
%put "This is the value of TMSIS macro: &TMSIS.";

options
sastrace=',,,ds'
sastraceloc=saslog no$stsuffix
dbidirectexec
sqlgeneration=dbms
msglevel=i
sql_ip_trace=source
mergenoby=error
varinitchk=error
nosqlremerge
minoperator
;

%INCLUDE "/sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/programs/AWS_Shared_Macros.sas";

options noerrorabend;

%let basedir=/sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/ann_pl;

/* Include the annual macros program and the program with the macro for each segment */

%include "&basedir./programs/annual_macros_pl.sas";
%include "&basedir./programs/001_base_pl.sas"; /* accreditation is an expanded array in the base */
%include "&basedir./programs/003_lctn.sas";
%include "&basedir./programs/004_sarea.sas";
%include "&basedir./programs/005_oa.sas";
%include "&basedir./programs/006_enrlmt.sas";

/* Set the global macros :
	 - REPORTING_PERIOD: Date value from which we will take the last 4 characters to determine year
                         (read from job control table)
     - YEAR: Year of annual file (created from REPORTING_PERIOD)
     - RUNDATE: Date of run
     - VERSION: Version, in format of P1, P2, F1, F2, etc.. through P9/F9 (read from job control table)
     - DA_RUN_ID: sequential run ID, increments by 1 for each run of the monthly/annual TAF (read from
                  job control table)
     - ROWCOUNT: # of records in the final tables, which will be assigned after the creation of
                 each table and then inserted into the metadata table
     - TMSIS_SCHEMA: TMSIS schema (e.g. dev, val, prod) in which the program is being run, assigned
                     in the tmsis_config_macro above
     - DA_SCHEMA: Data Analytic schema (e.g. dev, val, prod) in which the program is being run, assigned
                  in the da_config_macro above
     - ST_FILTER: List of states to run (if no states are listed, then take all) (read from job control
                  table)
     - PYEARS: Prior years (all years from 2014 to current year minus 1)
     - GETPRIOR: Indicator for whether there are ANY records in the prior yeara to do prior year lookback.
                 If yes, set = 1 and look to prior yeara to get demographic information if current year
                 is missing for each enrollee/demographic column. This will be determined with the macro
                 count_prior_year
     - NMCSLOTS: # of monthly slots for MC IDs/types (currently 16, set below) 
     - NWAIVSLOTS: # of waiver slots for waiver IDs/types (currently 10, set below)
     - MONTHSB: List of months backwards from December to January (to loop through when needed) */


%GLOBAL REPORTING_PERIOD;
%GLOBAL YEAR;
%GLOBAL RUNDATE;
%GLOBAL VERSION;
%GLOBAL DA_RUN_ID;
%GLOBAL ROWCOUNT;
%GLOBAL TMSIS_SCHEMA;
%GLOBAL DA_SCHEMA;
%GLOBAL ST_FILTER;
%GLOBAL GETPRIOR;
%GLOBAL PYEARS;
%GLOBAL NMCSLOTS;
%GLOBAL NWAIVSLOTS;

/* these macro variables allow some code to be used by both PL and PR annual builds */

%let FIL_TYP=PL;
%let LFIL_TYP=%qlowcase(&fil_typ);
%let main_id = MC_PLAN_ID; /* main_id= MC_PLAN_ID for MCP and SUBMTG_STATE_PRVDR_ID for PRV */

/* Check lookup table to get macro parameters, and update lookup table with job start time */

PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(a&lfil_typ.,&DA_SCHEMA.);   

	   sysecho 'in update for start timestamp';
	      
	   %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

%let DA_RUN_ID=&DA_RUN_ID.;

proc printto log="&basedir./logs/&lfil_typ._annual_build_&da_run_id..log" new;
run;

/* Create YEAR macro parm from REPORTING_PERIOD, and set other needed macro parms */

data _null_;
	call symput('YEAR',year(input(strip("&REPORTING_PERIOD"),YYMMDD12.)));
run;

%let YEAR=&YEAR.;

%let MONTHSB=12 11 10 09 08 07 06 05 04 03 02 01;

options nosymbolgen nomlogic nomprint nospool;

proc sql ;
	%tmsis_connect;

     /* Create tables with the max da_run_id for each month for the given state/year,
	    for current year.
	    Insert into a metadata table to be able to link back to this run. */

     %max_run_id(file=MCP)

	/* Create annual location segment and insert into permanent table. 
	   The temp table LCTN_SPLMTL_YR will be kept to join to the base.*/

	%create_segment(LCTN, LCM);

	/* Create annual service area segment and insert into permanent table.
	   The temp table SAREA_SPLMTL_YR will be kept to join to the base. */

	%create_segment(SAREA, SAM);

	/* Create the enrolled population segment and insert into permanent table.
	   The temp table ENRLMT_SPLMTL_YR will be kept to join to the base. */

	%create_segment(ENRLMT, EPM);

	/* Create the operating authority segment and insert into permanent table.
	   The temp table OPRTG_AUTHRTY_SPLMTL_YR will be kept to join to the base. */

	%create_segment(OA, OAM);

	 /* Create the base file - note this is created last because it joins to the
	    flags for plan id active in monthly files and the _SPLMTL flags
	    created in the other macros. */

	%create_segment(BASE, BSM);

	/* Update job control table with end time */

	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);

	%tmsis_disconnect;

quit;

proc printto;
run;

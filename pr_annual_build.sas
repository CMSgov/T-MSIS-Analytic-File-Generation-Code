/**********************************************************************************************/
/*Program: pr_annual_build.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*modified for PR: Heidi Cohen
/*Date: 05/2018 10/2019
/*Purpose: Generate the annual PR TAF using the monthly PRV TAF tables
/*Mod: 
/*Notes: Program includes annual_macro_pr.sas to create each
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

%let basedir=/sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/ann_pr;

/* Include the annual macros program and the program with the macro for each segment */

%include "&basedir./programs/annual_macros_pr.sas";
%include "&basedir./programs/001_base_pr.sas"; /* accreditation is an expanded array in the base */
%include "&basedir./programs/003_lctn_pr.sas";
%include "&basedir./programs/004_lcns.sas";
%include "&basedir./programs/005_id.sas";
%include "&basedir./programs/006_txnmy.sas";
%include "&basedir./programs/007_enrlmt.sas";
%include "&basedir./programs/008_grp.sas";
%include "&basedir./programs/009_pgm.sas";
%include "&basedir./programs/010_bed.sas";

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
     - PYEARS: Prior years (all years from 2014 to current year minus 1) note: not used for PR TAF
     - GETPRIOR: Indicator for whether there are ANY records in the prior yeara to do prior year lookback.
                 If yes, set = 1 and look to prior yeara to get demographic information if current year
                 is missing for each enrollee/demographic column. This will be determined with the macro
                 count_prior_year note: not used for PR TAF
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

/* these macro variables allow some code to be used by both PR and PR annual builds */

%let FIL_TYP=PR;
%let LFIL_TYP=%qlowcase(&fil_typ);
%let main_id = SUBMTG_STATE_PRVDR_ID; /* main_id= MC_PLAN_ID for MCP and SUBMTG_STATE_PRVDR_ID for PRV */
%let loc_id = PRVDR_LCTN_ID;

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

     %max_run_id(file=PRV)

	/* Create annual location segment and insert into permanent table. 
	   The temp table LCTN_SPLMTL_YR will be kept to join to the base.*/

	%create_segment(LCTN, LCP);

	/* Create annual licensing segment and insert into permanent table.
	   The temp table LCNS_SPLMTL_YR will be kept to join to the base. */

	%create_segment(LCNS, LIC);

	/* Create annual identifier segment and insert into permanent table.
	   The temp table ID_SPLMTL_YR will be kept to join to the base. */

	%create_segment(ID, IDP);

	/* Create annual taxonomy segment and insert into permanent table.
	   The temp table TXNMY_SPLMTL_YR will be kept to join to the base. */

	%create_segment(TXNMY, TAX);

	/* Create the enrollment segment and insert into permanent table.
	   The temp table ENRLMT_SPLMTL_YR will be kept to join to the base. */

	%create_segment(ENRLMT, ENP);

	/* Create the affiliated groups segment and insert into permanent table.
	   The temp table GRP_SPLMTL_YR will be kept to join to the base. */

	%create_segment(GRP, GRP);

	/* Create the affiliated programs segment and insert into permanent table.
	   The temp table PGM_SPLMTL_YR will be kept to join to the base. */

	%create_segment(PGM, PGM);

	/* Create the bed types segment and insert into permanent table.
	   The temp table BED_SPLMTL_YR will be kept to join to the base. */

	%create_segment(BED, BED);

	/* Create the base file - note this is created last because it joins to the
	    flags for plan id active in monthly files and the _SPLMTL flags
	    created in the other macros. */

	%create_segment(BASE, BSP);

	/* Update job control table with end time */

	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);

	%tmsis_disconnect;

quit;

proc printto;
run;

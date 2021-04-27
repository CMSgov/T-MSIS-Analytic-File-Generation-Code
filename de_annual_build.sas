/**********************************************************************************************/
/*Program: de_annual_build.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual DE TAF using the monthly BSF TAF tables
/*Mod: 
/*Notes: Program calls annual_macro.sas and 001_base.sas -- 009_disability_need.sas to create each
/*       of the nine annual tables
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


%let basedir=/sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/ann_de;

/* Include the annual macros program and the program with the macro for each segment */

%include "&basedir./programs/annual_macros.sas";
%include "&basedir./programs/001_base.sas";
%include "&basedir./programs/002_eligibility_dates.sas" ;
%include "&basedir./programs/003_address_phone.sas";
%include "&basedir./programs/005_managed_care.sas" ;
%include "&basedir./programs/006_waiver.sas" ;
%include "&basedir./programs/007_mfp.sas" ;
%include "&basedir./programs/008_hh_spo.sas" ;
%include "&basedir./programs/009_disability_need.sas";

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


/* Check lookup table to get macro parameters, and update lookup table with job start time */


PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(ade,&DA_SCHEMA.);   

	   sysecho 'in update for start timestamp';
	      
	   %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

%let DA_RUN_ID=&DA_RUN_ID.;

proc printto log="&basedir./logs/de_annual_build_&da_run_id..log" new;
run;

/* Create YEAR macro parm from REPORTING_PERIOD, and set other needed macro parms */

data _null_;
	call symput('YEAR',year(input(strip("&REPORTING_PERIOD"),YYMMDD12.)));
run;

%let YEAR=&YEAR.;

%let NMCSLOTS=16;
%let NWAIVSLOTS=10;
%let MONTHSB=12 11 10 09 08 07 06 05 04 03 02 01;

** Call the create_pyears macro to create the variable PYEARS, which = list of all years from 2014 to current year minus 1;

%create_pyears;

options nosymbolgen nomlogic nomprint nospool;

proc sql ;
	%tmsis_connect;

     /* Create tables with the max da_run_id for each month for the given state/year,
	    for both current and prior years. When inyear is not specified, current year is run.
	    For claims, run for current year only.

	    Insert into a metadata table to be able to link back to this run. */
	
     %max_run_id(file=BSF, tbl=taf_mon_bsf);

	 %max_run_id(file=IP)
	 %max_run_id(file=LT)
	 %max_run_id(file=OT)
	 %max_run_id(file=RX)

	 %macro priorids;

		 %if &pyears. ne  %then %do;
		    
			%do p=1 %to %sysfunc(countw(&pyears.));
		 		%let pyear=%scan(&pyears.,&p.);

		 		%max_run_id(file=BSF, tbl=taf_mon_bsf, inyear=&pyear.)

			%end;

		 %end;

	   %mend priorids;
	   %priorids;

	 /* Identify whether there are records for the prior years, and if there are ANY records, set getprior=1.
	    This will allow us to pull prior year data, if needed, for specific demographic elements on
	    Base segment and Address/phone segment. */

	 %any_pyear;

	/* Create the eligibility dates segment and insert into permanent table.
	   The temp table enrolled_days_YR will be kept to join to base. */

	%create_segment(ELDTS, DTS);

	/* Create the address and phone segment and insert into permanent table. */

	%create_segment(CNTCT_DTLS, ADR);

	/* Create the managed care segment and insert into permanent table.
	   The temp table MNGD_CARE_SPLMTL_YR will be kept to join to the base. */

	%create_segment(MC, MCR);

	/* Create the waiver segment and insert into permanent table.
	   The temp table WAIVER_SPLMTL_YR will be kept to join to the base. */

	%create_segment(WVR, WVR);

	/* Create the MFP segment and insert into permanent table.
	   The temp table MFP_SPLMTL_YR will be kept to join to the base. */

	%create_segment(MFP, MFP);

	/* Create the HH and SPO segment and insert into permanent table.
	   The temp table HH_SPO_SPLMTL_YR will be kept to join to the base. */

	%create_segment(HHSPO, HSP);

	/* Create the disability and need segment and insert into permanent table.
	   The temp table DIS_NEED_SPLMTLS_YR will be kept to join to the base. */

	%create_segment(DSBLTY, DSB);

	 /* Create the base file - note this is created last because it joins to the
	    days enrolled columns created in the ELDTS macro, and the _SPLMTL flags
	    created in the other macros. */

	%create_segment(BASE, BSE);

	/* Update job control table with end time */

	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);

	%tmsis_disconnect;

quit;

proc printto;
run;

/**********************************************************************************************/
/*Program: up_annual_build.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Generate the annual UP TAF
/*Mod: 
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

%let basedir=/sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/ann_up;

** Include the annual macros program and programs for base and top segments ;

%include "&basedir./programs/annual_up_macros.sas";
%include "&basedir./programs/000_up_base_de.sas";
%include "&basedir./programs/001_up_base_hdr.sas";
%include "&basedir./programs/002_up_base_hdr_comb.sas";
%include "&basedir./programs/003_up_base_line.sas";
%include "&basedir./programs/004_up_base_line_comb.sas";
%include "&basedir./programs/005_up_base_lt.sas";
%include "&basedir./programs/006_up_base_ip.sas";
%include "&basedir./programs/007_up_base_deliv.sas";
%include "&basedir./programs/008_up_base_fnl.sas";
%include "&basedir./programs/009_up_top.sas";
%include "&basedir./programs/010_up_top_fnl.sas";

** Set the global macros :
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
     - PYEAR: Prior year (to roll up IP to stay-level only)
     - PYEAR2: Two prior years (to roll up IP to stay-level only)
     - FYEAR: Following year (to roll up IP to stay-level only)
     - FLTYPES: Four claims file types to loop over
     - INDS1: List of MDCD SCHIP indicator names 
     - INDS2: List of XOVR and NXOVR indicator names 
     - LEAP: Indicator for whether year is leap year
     - TOTDAYS: Total # of days in year, = 366 for leap year, 365 otherwise ;


%GLOBAL REPORTING_PERIOD;
%GLOBAL YEAR;
%GLOBAL RUNDATE;
%GLOBAL VERSION;
%GLOBAL DA_RUN_ID;
%GLOBAL ROWCOUNT;
%GLOBAL TMSIS_SCHEMA;
%GLOBAL DA_SCHEMA;
%GLOBAL ST_FILTER;
%GLOBAL PYEAR;
%GLOBAL PYEAR2;
%GLOBAL FYEAR;
%GLOBAL INDS1;
%GLOBAL INDS2;
%GLOBAL FLTYPES;
%GLOBAL LEAP;
%GLOBAL TOTDAYS;

%GLOBAL ffsval;
%GLOBAL capval;
%GLOBAL mcval;
%GLOBAL suppval;

** Check lookup table to get macro parameters, and update lookup table with job start time;


PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(aup,&DA_SCHEMA.);   

	   sysecho 'in update for start timestamp';
	      
	   %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

%let DA_RUN_ID=&DA_RUN_ID.;
** Create YEAR macro parm from REPORTING_PERIOD, and set other needed macro parms;


data _null_;
	call symput('YEAR',year(input(strip("&REPORTING_PERIOD"),YYMMDD12.)));
run;

%let YEAR=&YEAR.;

proc printto log="&basedir./logs/up_annual_build_&year._&da_run_id..log" new;
run;

%let PYEAR=%eval(&YEAR.-1);
%let PYEAR2=%eval(&YEAR.-2);
%let FYEAR=%eval(&YEAR.+1);

%let FLTYPES=IP LT OT RX;
%let INDS1=MDCD SCHIP;
%let INDS2=XOVR NON_XOVR;

** List of HCBS values corresponding to values of 1-7 (to loop over to create indicators);

%let HCBSVALS=1915I 1915J 1915K 1915C 1115 OTHR_ACUTE_CARE OTHR_LT_CARE;

** List of CHIP_CD values corresponding to values of 1-3 (to loop over to create indicators);

%let CHIPMOS = nonchip_mdcd mchip schip;


proc sql ;
	%tmsis_connect;


	%macro build_up;

	     ** Create tables with the max da_run_id for all file types for each month for the given state/year.
		    When inyear is not specified, current year is run.
		    For IP only, must also look to prior and following years (last three of prior and first three of following).
		    Insert into a metadata table to be able to link back to this run. ;

		%max_run_id(file=DE,tbl=taf_ann_de_base)

		%max_run_id(file=IP)
		%max_run_id(file=IP,inyear=&pyear.)
		%max_run_id(file=IP,inyear=&fyear.)
		%max_run_id(file=LT)
		%max_run_id(file=OT)
		%max_run_id(file=RX);

		** 0: Run macro pullde (in 000_pull_de) to pull all needed demographic/eligibility cols from the DE, creating new cols based on all
		   monthly cols where needed;

		%pullde;

		** Use max RUN IDs from above to pull in all claims files, keeping needed cols for each, and subsetting to only desired claim types,
		   non-null MSIS ID, and MSIS ID not beginning with '&'.
		   Only specify cols below that are not pulled in from ALL claims files (universal cols will be hard-coded into pullclaims macro).
		   The resulting tables from the macros will be header- and line-level .

		   For IP only, pull prior and following years also;

		%do ipyear=&pyear. %to &fyear.;

			%pullclaims(IP,
			            hcols=ip_mh_dx_ind
		                      ip_sud_dx_ind
		                      ip_mh_txnmy_ind
		                      ip_sud_txnmy_ind
		                      admsn_dt
		                      dschrg_dt
	                          ptnt_stus_cd
                              blg_prvdr_num
                              blg_prvdr_npi_num
                              admtg_dgns_cd
							  admtg_dgns_cd_ind
							  %do i=1 %to 12;
							 	 dgns_&i._cd
								 dgns_&i._cd_ind
							  %end;
							  %do i=1 %to 6;
							  	  prcdr_&i._cd
							  %end;,
					   lcols=rev_cd,
                       inyear=&ipyear.);

		%end;


		%pullclaims(LT,
		           hcols=lt_mh_dx_ind
	                     lt_sud_dx_ind
	                     lt_mh_txnmy_ind
	                     lt_sud_txnmy_ind
	                     srvc_bgnng_dt
	                     admsn_dt
                         srvc_endg_dt
                         dschrg_dt);

		%pullclaims(OT,
		            hcols=ot_mh_dx_ind
	                      ot_sud_dx_ind
	                      ot_mh_txnmy_ind
	                      ot_sud_txnmy_ind
                          dgns_1_cd
					      dgns_2_cd
					      dgns_1_cd_ind
					      dgns_2_cd_ind,

                    lcols=hcbs_srvc_cd
                          prcdr_cd
                          rev_cd);

		%pullclaims(RX);


		******* 1: Go to raw claims (header and line-level) to create temp tables with cols to be used for BASE segment ***** ;

		** 1a: Loop through all four file types to run base_hdr_byfile (macro in 001_up_base_hdr), which rolls each header-level file to
		   the bene-level and creates aggregate cols across the headers;

		%do f=1 %to 4;
			%let file=%scan(&FLTYPES.,&f.);

			%base_hdr_byfile(file=&file.);

		%end;

		** 1b: Call the macro base_hdr_comb (macro in 002_up_base_hdr_comb) to union the above four bene-level claims tables and aggregate
		   across the four file types, and then join that bene-level table to each of the above four bene-level claims tables to get 
		   file-specific aggregated cols.
		   The output table (bene-level) is hdr_bene_base;

		%base_hdr_comb;


		** 1c: Loop through all four file types to run base_line_byfile (macro in 003_up_base_line), which rolls each line-level file to
		   the header-level and creates aggregate cols across the lines;

		%do f=1 %to 4;
			%let file=%scan(&FLTYPES.,&f.);

			%base_line_byfile(file=&file.);


		%end;

		** Drop RXL only (no longer needed);

		%drop_tables(rxl_&year.);

		** 1d: Call the macro base_line_comb (macro in 004_up_base_line_comb) to union the above four header-level claims tables and aggregate
		   across the four file types.
		   The output table (bene-level) is line_bene_base;

		%base_line_comb;

		** 1e: Call the macro base_lt_days (macro in 005_up_base_lt) to roll up the LT lines to the header-level, taking the min service
		       begin date (if header-level begin date is null) to then calculate unique LT days by MDCD/SCHIP, NXOVR/XOVR, and FFS/MC;

		%base_lt_days;

		** 1f: Call the macro base_ip_stays (macro in 006_up_base_ip) to roll up the IP lines to header-level, and then
		       create IP stays. Calculate the # of stays by MDCD/SCHIP, NXOVR/XOVR, and FFS/MC;

		%base_ip_stays;

		** 1g: Call the macro base_deliv (macro in 007_up_base_deliv) to read in IP and OT claims to identify benes with
		       any delivery claim;

		%base_deliv;
	
		** 1h: Combine all the above bene-level tables to insert into permanent base table;

		%up_base_fnl;
	
		** Insert record counts into permanent metadata table;

		%create_efts_metadata(tblname=BASE, FIL_4TH_NODE=BSU)

		******* 2: Go to raw claims (header-level) to create temp tables with cols to be used for TOP segment ***** ;

		** 2a: Loop through all four file types to run 009_up_top - the resulting table from each run will be at
		       the bene/pgm_type_cd/file_type/clm_type_cd level, so just need to be unioned to be output
		       into the TOP file ;

		%do f=1 %to 4;
			%let file=%scan(&FLTYPES.,&f.);

			%top_by_file(file=&file.,f=&f.);

		%end;

		** 2b: Call the macro 010_up_top_fnl to union the above four summarized files and then output to permanent table;

		%up_top_fnl;

		** Insert record counts into permanent metadata table;

		%create_efts_metadata(tblname=TOP, FIL_4TH_NODE=TOP);

		** Update job control table with end time ; 

		sysecho 'in jobcntl updt2';
		%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.); 
		
	%mend build_up;

	%build_up;

	%tmsis_disconnect;

quit;

proc printto;
run;

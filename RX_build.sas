/**********************************************************************************************/
/* Program:     RX_build.sas					                                              */
/* Author:      Deo B. and Christina A.     			                                      */
/* Date:        12/2/2016                                                                     */
/* Purpose:     Program produces RX analytic files	                          	              */
/*              This is the main program for use in building the RX file                      */
/*              Program calls the following programs:		                                  */
/*		Tmsis_macros.sas --> contains macros to set up processing environment	              */
/*		Project_formats.sas --> contains formats and recode values for project	              */
/*		AWS_Project_macros.sas --> contains macros used in creating views & tables for        */
/* 		each segment to be used for the project and programming code that 	                  */
/*		"massages" each file segment prior to being joined with other segments                */
/*		to create the final RX file.                                                          */
/*		                                                                                      */
/* Modified: by Christina Alva 2/27/2017                                                      */
/*                                                                                            */
/**********************************************************************************************/
options noquotelenmax mprint symbolgen spool formdlim="~" ls=max nocenter compress=yes;


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
%if (%sysfunc(find(&_SASPROGRAMFILE,%str(lock))) GE 1) %then %do;
   %let TMSIS = tmsislockdown; %end; %else
%if (%sysfunc(find(&_SASPROGRAMFILE,%str(shar))) GE 1) %then %do;
	   %let TMSIS =  tmsisshare; %end;
%if %sysevalf(%superq(TMSIS)=,boolean) %then %do;
      %let TMSIS = tmsislockdown;
%end; 
%mend setENV;
%setENV; %put TMSIS=&TMSIS;

%put "This is the value of TMSIS macro: &TMSIS.";

%let path = /sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/rx;

libname RX "&path./data";

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
;

*********************************************************************
*  INCLUDE PROJECT FORMATS AND MACROS                               *
*********************************************************************;
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/AWS_Shared_Macros.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/AWS_Grouper_Macro.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/rx/programs/AWS_RX_Macros.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/Fasc.sas";

********************************************************************
* GLOBAL MACROS AND LOCAL MACROS                                   *
********************************************************************;
%GLOBAL REPORTING_PERIOD;
%GLOBAL REP_YR;
%GLOBAL REP_MO;
%GLOBAL RUNDATE;
%GLOBAL TAF_FILE_DATE;
%GLOBAL VERSION;
%GLOBAL DA_RUN_ID;
%GLOBAL ROWCOUNT;
%GLOBAL TMSIS_SCHEMA;
%GLOBAL DA_SCHEMA;
%GLOBAL DA_SCHEMA_INDB;
%GLOBAL TABLE_NAME_INDB;
%GLOBAL FL;
%GLOBAL ST_FILTER;
%GLOBAL HEADER_CT; 
%GLOBAL LINE_CT; 
%LET FL = RX;

**********************************************************************
* 1. CHECK LOOKUP TABLE TO GET PARAMETERS                            *
* 2. UPDATE LOOKUP TABLE WITH JOB START TIME                         *
**********************************************************************;

PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(trx,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

PROC SQL;
   sysecho 'in update for start timestamp';
   %TMSIS_CONNECT;
      
      %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;


/* set up environment variables */
 data _null_;
 length rep_date 6  ;
 
 rep_date = input(strip("&REPORTING_PERIOD"),YYMMDD12.);
 FILE_DATE = input(put(input("&REPORTING_PERIOD", ANYDTDTE10.),yymmn6.),6.); 
 call symput ('TAF_FILE_DATE', put(FILE_DATE,6.));

 call symputx ('rep_yr', year(rep_date));
 call symputx ('rep_mo', month(rep_date));
 call symput ('rpt_prd', put(rep_date,date9.));
 call symput('DARUNID',compress(&DA_RUN_ID.));
run;

%PUT DA_RUN_ID=&DA_RUN_ID, RPT_PRD=&REPORTING_PERIOD, VRSN=&VERSION;

%MACRO PROCESS (); 

proc printto 
log="&path./logs/RX_Build_&reporting_period._&DARUNID..log" new;
run;


proc sql;
%tmsis_connect;
    sysecho 'in max run ID for state';
    %AWS_MAXID_pull (&TMSIS_SCHEMA., TMSIS_FIL_PRCSG_JOB_CNTL); /* PULLS MAX RUN IDS FROM FILE HEADER - 01 SEGMENT*/
    sysecho 'in claims family';
    %AWS_Claims_Family_Table_Link (&TMSIS_SCHEMA.,CRX00002, TMSIS_CLH_REC_&fl, &fl, a.RX_FILL_DT);  /* IP: a.DSCHRG_DT, LT: a.SRVC_ENDG_DT, RX: a.FILL_DT, OT: a.SRVC_ENDG_DT */
	sysecho 'in process_nppes';
	%process_nppes();						
	sysecho 'in process ccs';	
    %process_ccs(); 	
    sysecho 'in extract line';
	%AWS_Extract_Line_&fl (&TMSIS_SCHEMA., &fl, CRX00003, TMSIS_CLL_REC_&fl, a.RX_FILL_DT);  /* PULL IN LINE VARIABLES*/
	sysecho "in build &fl";	
    %BUILD_&fl();	
 	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);
	%let TABLE_NAME = TAF_&fl.H;
	%let FIL_4TH_NODE = &fl.H;
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &HEADER_CT., &FIL_4TH_NODE.); 
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., AWS_RX_Macros, 1.1 AWS_Extract_Line_RX, %nrstr(&FL._HEADER), new_submtg_state_cd);
    %let TABLE_NAME = TAF_&fl.L;
	%let FIL_4TH_NODE = &fl.L;
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &LINE_CT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., AWS_RX_Macros, 1.1 AWS_Extract_Line_RX, %nrstr(&FL._LINE), new_submtg_state_cd_line);
	sysecho 'Update EFTS Meta info';
	%FINAL_CONTROL_INFO(&DA_RUN_ID., &DA_SCHEMA.);	
    
	%tmsis_disconnect;

quit;


proc printto; run;

%mend process;

/* call Process macro to produce tables */
*********************************************************************
*  INCLUDE FILE USED TO SPECIFY WHICH MONTH TO RUN                  *
*********************************************************************;

%Process (); 


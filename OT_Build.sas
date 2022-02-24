/**********************************************************************************************/
/* Program:     OT_build.sas									                              */
/* Author:      Deo S. Bencio  					                                              */
/* Date:        2/28/2017                                                                     */
/* Purpose:     Program produces OT analytic files						            	      */
/* Mod:			8/4/17 by CA for interim solution											  */ 						  
/*                                                                                            */                                                                                                    
/**********************************************************************************************/
*********************************************************************
*     QSSI PROVIDED MACROS FOR ENVIRONMENT                          *                           
*     TMSIS_SCHEMA - OPERATIONAL_SCHEMA                             *       
*     DA_SCHEMA - DATA_ANALYTICS_SCHEMA                             *
*********************************************************************;
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
	%let TMSIS = tmsisshare; %end;
%if %sysevalf(%superq(TMSIS)=,boolean) %then %do;
	%let TMSIS = tmsislockdown;
%end;
%mend setENV;
%setENV; %put TMSIS=&TMSIS;

%put "This is the value of TMSIS macro: &TMSIS.";

*********************************************************************
*  INCLUDE PROJECT FORMATS AND MACROS                               *
*********************************************************************;
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/AWS_Shared_Macros.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/AWS_Grouper_Macro.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/ot/programs/AWS_OT_Macros.sas";
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
%GLOBAL ST_INDEX;
%GLOBAL HEADER_CT; %LET HEADER_CT = 0;
%GLOBAL LINE_CT; %LET LINE_CT = 0;
%LET FL = OT;


**********************************************************************
* 1. CHECK LOOKUP TABLE TO GET PARAMETERS                            *
* 2. UPDATE LOOKUP TABLE WITH JOB START TIME                         *
**********************************************************************;

PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(tot,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

%PUT &DA_RUN_ID &REPORTING_PERIOD &VERSION &ST_FILTER;

PROC SQL;
   sysecho 'in update for start timestamp';
   %TMSIS_CONNECT;
      
      %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;



%MACRO PROCESS ();

 options nosymbolgen nomprint  nosource  nosource2;


/* set up environment variables */
 data _null_;
 length rep_date $ 6 FILE_DATE 6;
 
 rep_date = put(year(input("&REPORTING_PERIOD",ANYDTDTE10.)),4.) || put(month(input("&REPORTING_PERIOD",ANYDTDTE10.)),Z2.); 
 call symput ('rep_date', rep_date);
 call symput ('rundate', rep_date);
 call symput ('rep_yr', substr(rep_date,1,4));
 call symput ('rep_mo', substr(rep_date,5,2));
 FILE_DATE = input(put(input("&REPORTING_PERIOD", ANYDTDTE10.),yymmn6.),6.); 
 call symput ('TAF_FILE_DATE', put(FILE_DATE,6.));
 call symput('DARUNID',compress(&DA_RUN_ID.));
run;

proc printto 
              log="/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/ot/logs/OT_Build_&reporting_period._&DARUNID..log"
              new; 
run;

proc sql;
%tmsis_connect;
  %AWS_MAXID_pull (&TMSIS_SCHEMA.,  TMSIS_FIL_PRCSG_JOB_CNTL); /* PULLS MAX RUN IDS FROM FILE HEADER - 01 SEGMENT*/
%tmsis_disconnect;
quit;

%macro RUN_STATES;

%DO ST_INDEX=1 %to %SYSFUNC(COUNTW(&STATE_IDS));
  %let RUN_ID = %SCAN(&RUN_IDS,&ST_INDEX);
  %let STATE_ID = %SCAN(&STATE_IDS,&ST_INDEX);

proc sql;

	%tmsis_connect;    
    sysecho "in claims family -  &STATE_ID";
	%AWS_Claims_Family_Table_Link (&TMSIS_SCHEMA., COT00002, TMSIS_CLH_REC_OTHR_TOC, OTHR_TOC, a.SRVC_ENDG_DT);  /* OT: a.SRVC_ENDG_DT */
	sysecho 'in process_nppes';
	%process_nppes();						
	sysecho 'in process ccs';	
    %process_ccs(); 	
    sysecho "in extract line - &STATE_ID"; 
	%AWS_Extract_Line_OT (&TMSIS_SCHEMA., &fl, OTHR_TOC, COT00003, TMSIS_CLL_REC_OTHR_TOC, a.SRVC_ENDG_DT);
    sysecho "in assign grouper data - &STATE_ID";  
	%AWS_ASSIGN_GROUPER_DATA_CONV(filetyp=&fl, clm_tbl=&fl._HEADER, line_tbl=&fl._LINE, analysis_date=a.SRVC_ENDG_DT_HEADER,MDC=NO,IAP=YES,PHC=YES,MH_SUD=YES,TAXONOMY=YES); 
	sysecho "in build ot - &STATE_ID";	
    %BUILD_OT();	
	
%tmsis_disconnect;
quit;

%END;
%MEND RUN_STATES;

%RUN_STATES;
%put HEADER_CT = &HEADER_CT;
%put LINE_CT = &LINE_CT;

proc sql;
%tmsis_connect;
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);
	sysecho 'in get cnt';
	%let TABLE_NAME = TAF_OTH;
	%let FIL_4TH_NODE = OTH;
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &HEADER_CT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., AWS_OT_Macros, 1.1 AWS_Extract_Line_OT, %nrstr(&FL2._HEADER), new_submtg_state_cd);

    %let TABLE_NAME = TAF_OTL;
	%let FIL_4TH_NODE = OTL;
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &LINE_CT., &FIL_4TH_NODE.);
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., AWS_OT_Macros, 1.1 AWS_Extract_Line_OT, %nrstr(&FL2._LINE), new_submtg_state_cd_line);
 
    sysecho 'In Control Info';
	%FINAL_CONTROL_INFO(&DA_RUN_ID., &DA_SCHEMA.);	    
	%tmsis_disconnect;

quit;

proc printto; run;
%mend Process;

%Process ();



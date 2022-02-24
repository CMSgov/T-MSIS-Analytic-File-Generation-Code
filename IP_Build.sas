/**********************************************************************************************/
/* Program:     IP_build.sas									                              */
/* Author:      Deo S. Bencio  					                                              */
/* Date:        8/15/2016                                                                     */
/* Purpose:     Program produces IP analytic files						            	      */
/*              This is the main program for use in building the IP file                      */
/*              Program calls the following programs:										  */
/*				Tmsis_macros.sas --> contains macros to set up processing environment		  */
/*				Project_formats.sas --> contains formats and recode values for project	  	  */
/*				AWS_Shared_Macros.sas --> contains macros used in extracting and joining	  */
/* 					final action claims, header records, and other macros used across all 	  */
/*					claim types												 		 		  */
/*				AWS_IP_Macros.sas --> contains macros used specifically for IP claims process */
/*				AWS_Grouper_macro.sas --> contains macros used in grouper coding for MDC, IAP,*/
/*					HCC																		  */
/*				QA_tabulation.sas --> contains SAS code to do QA Tabs, Dumps, and Contents    */
/*				IP_Freqlists.sas --> contains list of variables to produce freqs for in QA    */
/*																							  */
/**********************************************************************************************/
*********************************************************************
*     QSSI PROVIDED MACROS FOR ENVIRONMENT                          *                           
*     TMSIS_SCHEMA - OPERATIONAL_SCHEMA                             *       
*     DA_SCHEMA - DATA_ANALYTICS_SCHEMA                             *
*********************************************************************;
*options mprint source2;
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
	%let TMSIS =tmsislockdown;
%end;
%mend setENV;  
%SETenv;  %put TMSIS=&TMSIS;

%put "This is the value of TMSIS macro: &TMSIS.";

*********************************************************************
*  INCLUDE PROJECT FORMATS AND MACROS                               *
*********************************************************************;
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/AWS_Shared_Macros.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/AWS_Grouper_Macro.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/ip/programs/AWS_IP_Macros.sas";
%INCLUDE "/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/programs/Fasc.sas";

libname tempdata "/sasdata/users/&sysuserid/&tmsis./Deo/temp";
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
%LET FL = IP;


**********************************************************************
* 1. CHECK LOOKUP TABLE TO GET PARAMETERS                            *
* 2. UPDATE LOOKUP TABLE WITH JOB START TIME                         *
**********************************************************************;

PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(tip,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

%PUT &DA_RUN_ID &REPORTING_PERIOD &VERSION;

PROC SQL;
   sysecho 'in update for start timestamp';
   %TMSIS_CONNECT;
      
      %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;
options noerrabend;

%MACRO PROCESS ();

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
              log="/sasdata/users/&sysuserid/&tmsis./&sub_env./data_analytics/taf/ip/logs/IP_build_&reporting_period._&DARUNID..log"
              new; 
run;

proc sql;
  %tmsis_connect;
    sysecho 'in max run ID for state';
	%AWS_MAXID_pull (&TMSIS_SCHEMA., TMSIS_FIL_PRCSG_JOB_CNTL); /* PULLS MAX RUN IDS FROM FILE HEADER - 01 SEGMENT*/
    sysecho 'in claims family';
	%AWS_Claims_Family_Table_Link (&TMSIS_SCHEMA., CIP00002, TMSIS_CLH_REC_&fl, &fl, DSCHRG_DT);   /* IP: a.DSCHRG_DT, LT: a.SERVICE_END_DT, RX: a.FILL_DT, OT: a.SERVICE_END_DT */
	sysecho 'in process_nppes';
	%process_nppes();						
	sysecho 'in process ccs';	
    %process_ccs(); 	
    sysecho 'in extract line';
	%AWS_Extract_Line_IP (&TMSIS_SCHEMA., &fl,&fl,CIP00003, TMSIS_CLL_REC_&fl, DSCHRG_DT);
    sysecho 'in assign grouper data';  
	%AWS_ASSIGN_GROUPER_DATA_CONV(filetyp=&fl, clm_tbl=&fl._HEADER, line_tbl=&fl._LINE, analysis_date=DSCHRG_DT,MDC=YES,IAP=YES,PHC=YES,MH_SUD=YES,TAXONOMY=YES); 
	sysecho 'in build ip';	
    %BUILD_IP();	
 	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);
	%let TABLE_NAME = TAF_IPH;
	%let FIL_4TH_NODE = IPH;
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &HEADER_CT., &FIL_4TH_NODE.);  
    %CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., AWS_IP_Macros, 1.1 AWS_Extract_Line_IP, %nrstr(&FL._HEADER), new_submtg_state_cd);   
    %let TABLE_NAME = TAF_IPL;
	%let FIL_4TH_NODE = IPL;
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &LINE_CT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., AWS_IP_Macros, 1.1 AWS_Extract_Line_IP, %nrstr(&FL._LINE), new_submtg_state_cd_line);
    sysecho 'In Control Info';
	%FINAL_CONTROL_INFO(&DA_RUN_ID., &DA_SCHEMA.);	
    
	%tmsis_disconnect;

quit;

proc printto; run;
%mend Process;

%Process ();



       

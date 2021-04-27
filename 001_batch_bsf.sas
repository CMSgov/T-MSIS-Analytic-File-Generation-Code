/**********************************************************************************************/
/*Program: 001_batch_bsf.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Generate monthly Beneficiary Summary File for TMSIS.
/*Mod: 
/* 		 3/4/2021: MACBIS-1683: DB mod to add file date to log file names
/*Notes: 1. Set REPORTING_DATE and RUNDATE macros to control output.
/*       2. Program calls 000_bsf_macros.sas and 002 through 023, which initializes macros
/*          to make calls against Redshift to create 20 individual tables, which are then 
/*          combined to create the final BSF summary file (name controled by above macro vars).
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
%setENV; %put TMSIS=&TMSIS;

%put "This is the value of TMSIS macro: &TMSIS.";

%let path = /sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/bsf;

libname BSF "&path./data";

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

%INCLUDE "/sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/programs/AWS_Shared_Macros.sas";

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
%GLOBAL ST_FILTER;
%GLOBAL MAX_KEEP; %LET MAX_KEEP = 0;
%LET TABLE_NAME = TAF_MON_BSF;
%LET FIL_4TH_NODE = BSF;


**********************************************************************
* 1. CHECK LOOKUP TABLE TO GET PARAMETERS                            *
* 2. UPDATE LOOKUP TABLE WITH JOB START TIME                         *
**********************************************************************;

PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
      
       %JOB_CONTROL_RD(mbsf,&DA_SCHEMA.);   


    sysecho 'in update for start timestamp';
      
      %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);   
      
   %TMSIS_DISCONNECT;
   
QUIT;

data _null_;
	begmon =put(intnx('month',input("&REPORTING_PERIOD",YYMMDD12.),0,"BEGINNING"),date9.);
    call symputx('begmon',upcase(begmon));
	call symputx('st_dt',upcase(cats("'",begmon,"'")));

	BSF_FILE_DATE = input(put(input("&REPORTING_PERIOD",YYMMDD12.),yymmn6.),6.);
	call symputx('TAF_FILE_DATE',BSF_FILE_DATE);

    RPT_PRD = cats("'",put(input(strip("&REPORTING_PERIOD"),YYMMDD12.),date9.),"'");
	call symputx('RPT_PRD',RPT_PRD);

	RPT_OUT=compress(RPT_PRD,"'");
	call symputx('RPT_OUT',RPT_OUT);

	FILE_DT_END = put(intnx('month',input("&sysdate",date9.),0,"END"),date9.);
	call symputx('FILE_DT_END',cats("'",FILE_DT_END,"'"));

    call symputx('DARUNID',compress(&DA_RUN_ID.));

	numdays=day(input("&REPORTING_PERIOD",YYMMDD12.));
	call symputx('DAYS_IN_MONTH',numdays);

run; 

proc printto
log="&path./logs/001_batch_bsf_&reporting_period._&DARUNID..log" new;
run;

%put begmon=&begmon st_dt = &st_dt TAF_FILE_DATE=&TAF_FILE_DATE;
%put RPT_PRD=&RPT_PRD RPT_OUT=&RPT_OUT FILE_DT_END=&FILE_DT_END;
%LET TEST_RUN=NO;
%let BSF_FILE_DATE = &TAF_FILE_DATE;


options noquotelenmax mprint symbolgen spool formdlim="~" ls=max nocenter compress=yes;
options nomprint nosymbolgen nomlogic ;
%let prog_path = &path./programs;

%include "&prog_path./000_bsf_macros.sas" /source2;
%include "&prog_path./002_bsf_ELG00002.sas" /source2;
%include "&prog_path./003_bsf_ELG00003.sas" /source2;
%include "&prog_path./004_bsf_ELG00004.sas" /source2;
%include "&prog_path./005_bsf_ELG00005.sas" /source2;
%include "&prog_path./006_bsf_ELG00006.sas" /source2;
%include "&prog_path./007_bsf_ELG00007.sas" /source2;
%include "&prog_path./008_bsf_ELG00008.sas" /source2;
%include "&prog_path./009_bsf_ELG00009.sas" /source2;
%include "&prog_path./010_bsf_ELG00010.sas" /source2;
%include "&prog_path./011_bsf_ELG00011.sas" /source2;
%include "&prog_path./012_bsf_ELG00012.sas" /source2;
%include "&prog_path./013_bsf_ELG00013.sas" /source2;
%include "&prog_path./014_bsf_ELG00014.sas" /source2;
%include "&prog_path./015_bsf_ELG00015.sas" /source2;
%include "&prog_path./016_bsf_ELG00016.sas" /source2;
%include "&prog_path./017_bsf_ELG00017.sas" /source2;
%include "&prog_path./018_bsf_ELG00018.sas" /source2;
%include "&prog_path./020_bsf_ELG00020.sas" /source2;
%include "&prog_path./021_bsf_ELG00021.sas" /source2;
%include "&prog_path./022b_bsf_ELG00022.sas" /source2;
%include "&prog_path./022_bsf_TPL00002.sas" /source2;
%include "&prog_path./023_bsf_final.sas" /source2;

options noerrabend;
proc sql verbose;

%tmsis_connect;

 %AWS_MAXID_pull_BSF;

%create_initial_table (ELG00002, TMSIS_PRMRY_DMGRPHC_ELGBLTY, PRMRY_DMGRPHC_ELE_EFCTV_DT, PRMRY_DMGRPHC_ELE_END_DT);
%create_initial_table (ELG00002A, TMSIS_PRMRY_DMGRPHC_ELGBLTY, PRMRY_DMGRPHC_ELE_EFCTV_DT, PRMRY_DMGRPHC_ELE_END_DT);
%create_ELG00002 (ELG00002, TMSIS_PRMRY_DMGRPHC_ELGBLTY, PRMRY_DMGRPHC_ELE_EFCTV_DT, PRMRY_DMGRPHC_ELE_END_DT);

%create_initial_table (ELG00003, TMSIS_VAR_DMGRPHC_ELGBLTY, VAR_DMGRPHC_ELE_EFCTV_DT, VAR_DMGRPHC_ELE_END_DT);
%create_initial_table (ELG00003A, TMSIS_VAR_DMGRPHC_ELGBLTY, VAR_DMGRPHC_ELE_EFCTV_DT, VAR_DMGRPHC_ELE_END_DT);
%create_ELG00003 (ELG00003, TMSIS_VAR_DMGRPHC_ELGBLTY, VAR_DMGRPHC_ELE_EFCTV_DT, VAR_DMGRPHC_ELE_END_DT);

%create_initial_table (ELG00004, TMSIS_ELGBL_CNTCT, ELGBL_ADR_EFCTV_DT, ELGBL_ADR_END_DT,orderby=%str(msis_ident_num,ELGBL_ADR_TYPE_CD));
%create_ELG00004 (ELG00004, TMSIS_ELGBL_CNTCT, ELGBL_ADR_EFCTV_DT, ELGBL_ADR_END_DT);

%create_initial_table (ELG00005, TMSIS_ELGBLTY_DTRMNT, ELGBLTY_DTRMNT_EFCTV_DT, ELGBLTY_DTRMNT_END_DT,orderby=%str(msis_ident_num,PRMRY_ELGBLTY_GRP_IND));
%create_ELG00005 (ELG00005, TMSIS_ELGBLTY_DTRMNT, ELGBLTY_DTRMNT_EFCTV_DT, ELGBLTY_DTRMNT_END_DT);

%create_initial_table (ELG00006, TMSIS_HH_SNTRN_PRTCPTN_INFO, HH_SNTRN_PRTCPTN_EFCTV_DT, HH_SNTRN_PRTCPTN_END_DT);
%create_ELG00006 (ELG00006, TMSIS_HH_SNTRN_PRTCPTN_INFO, HH_SNTRN_PRTCPTN_EFCTV_DT, HH_SNTRN_PRTCPTN_END_DT);

%create_initial_table (ELG00007, TMSIS_HH_SNTRN_PRVDR, HH_SNTRN_PRVDR_EFCTV_DT, HH_SNTRN_PRVDR_END_DT);
%create_ELG00007 (ELG00007, TMSIS_HH_SNTRN_PRVDR, HH_SNTRN_PRVDR_EFCTV_DT, HH_SNTRN_PRVDR_END_DT);

%create_initial_table (ELG00008, TMSIS_HH_CHRNC_COND, HH_CHRNC_EFCTV_DT, HH_CHRNC_END_DT);
%create_ELG00008 (ELG00008, TMSIS_HH_CHRNC_COND, HH_CHRNC_EFCTV_DT, HH_CHRNC_END_DT);

%create_initial_table (ELG00009, TMSIS_LCKIN_INFO, LCKIN_EFCTV_DT, LCKIN_END_DT);
%create_ELG00009 (ELG00009, TMSIS_LCKIN_INFO, LCKIN_EFCTV_DT, LCKIN_END_DT);
%create_initial_table (ELG00010, TMSIS_MFP_INFO, MFP_ENRLMT_EFCTV_DT, MFP_ENRLMT_END_DT);
%create_ELG00010 (ELG00010, TMSIS_MFP_INFO, MFP_ENRLMT_EFCTV_DT, MFP_ENRLMT_END_DT);

%create_initial_table (ELG00011, TMSIS_STATE_PLAN_PRTCPTN, STATE_PLAN_OPTN_EFCTV_DT, STATE_PLAN_OPTN_END_DT);
%create_ELG00011 (ELG00011, TMSIS_STATE_PLAN_PRTCPTN, STATE_PLAN_OPTN_EFCTV_DT, STATE_PLAN_OPTN_END_DT);

%create_initial_table (ELG00012, TMSIS_WVR_PRTCPTN_DATA, WVR_ENRLMT_EFCTV_DT, WVR_ENRLMT_END_DT);
%create_ELG00012 (ELG00012, TMSIS_WVR_PRTCPTN_DATA, WVR_ENRLMT_EFCTV_DT, WVR_ENRLMT_END_DT);

%create_initial_table (ELG00013, TMSIS_LTSS_PRTCPTN_DATA, LTSS_ELGBLTY_EFCTV_DT, LTSS_ELGBLTY_END_DT);
%create_ELG00013 (ELG00013, TMSIS_LTSS_PRTCPTN_DATA, LTSS_ELGBLTY_EFCTV_DT, LTSS_ELGBLTY_END_DT);

%create_initial_table (ELG00014, TMSIS_MC_PRTCPTN_DATA, MC_PLAN_ENRLMT_EFCTV_DT, MC_PLAN_ENRLMT_END_DT);
%create_ELG00014 (ELG00014, TMSIS_MC_PRTCPTN_DATA, MC_PLAN_ENRLMT_EFCTV_DT, MC_PLAN_ENRLMT_END_DT);

%create_initial_table (ELG00015, TMSIS_ETHNCTY_INFO, ETHNCTY_DCLRTN_EFCTV_DT, ETHNCTY_DCLRTN_END_DT);
%create_ELG00015 (ELG00015, TMSIS_ETHNCTY_INFO, ETHNCTY_DCLRTN_EFCTV_DT, ETHNCTY_DCLRTN_END_DT);

%create_initial_table (ELG00016, TMSIS_RACE_INFO, RACE_DCLRTN_EFCTV_DT, RACE_DCLRTN_END_DT);
%create_ELG00016 (ELG00016, TMSIS_RACE_INFO, RACE_DCLRTN_EFCTV_DT, RACE_DCLRTN_END_DT);

%create_initial_table (ELG00017, TMSIS_DSBLTY_INFO, DSBLTY_TYPE_EFCTV_DT, DSBLTY_TYPE_END_DT);
%create_ELG00017 (ELG00017, TMSIS_DSBLTY_INFO, DSBLTY_TYPE_EFCTV_DT, DSBLTY_TYPE_END_DT);
%create_initial_table (ELG00018, TMSIS_SECT_1115A_DEMO_INFO, SECT_1115A_DEMO_EFCTV_DT, SECT_1115A_DEMO_END_DT);
%create_ELG00018 (ELG00018, TMSIS_SECT_1115A_DEMO_INFO, SECT_1115A_DEMO_EFCTV_DT, SECT_1115A_DEMO_END_DT);

%create_initial_table (ELG00020, TMSIS_HCBS_CHRNC_COND_NON_HH, NDC_UOM_CHRNC_NON_HH_EFCTV_DT, NDC_UOM_CHRNC_NON_HH_END_DT);
%create_ELG00020 (ELG00020, TMSIS_HCBS_CHRNC_COND_NON_HH, NDC_UOM_CHRNC_NON_HH_EFCTV_DT, NDC_UOM_CHRNC_NON_HH_END_DT);

%create_initial_table (ELG00021, TMSIS_ENRLMT_TIME_SGMT_DATA, ENRLMT_EFCTV_DT, ENRLMT_END_DT,orderby=%str(msis_ident_num,ENRLMT_TYPE_CD));
%create_ELG00021 (ELG00021, TMSIS_ENRLMT_TIME_SGMT_DATA, ENRLMT_EFCTV_DT, ENRLMT_END_DT);

%create_initial_table (ELG00022, TMSIS_ELGBL_ID, ELGBL_ID_EFCTV_DT, ELGBL_ID_END_DT);
%create_ELG00022 (ELG00022, TMSIS_ELGBL_ID, ELGBL_ID_EFCTV_DT, ELGBL_ID_END_DT);

%create_initial_table (TPL00002, TMSIS_TPL_MDCD_PRSN_MN, ELGBL_PRSN_MN_EFCTV_DT, ELGBL_PRSN_MN_END_DT);
%create_TPL00002 (TPL00002, TMSIS_TPL_MDCD_PRSN_MN, ELGBL_PRSN_MN_EFCTV_DT, ELGBL_PRSN_MN_END_DT);

%BSF_Final;

	sysecho 'in build BSF';
    %BUILD_BSF();
 	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);
	sysecho 'in get cnt';
	%GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 023_bsf_ELG00023, 0.1. create_initial_table, %nrstr(BSF_&RPT_OUT._&BSF_FILE_DATE), submtg_state_cd);
	sysecho 'In Control Info';
	%FINAL_CONTROL_INFO(&DA_RUN_ID., &DA_SCHEMA.);
     
  
%tmsis_disconnect;

quit;

proc printto; run;

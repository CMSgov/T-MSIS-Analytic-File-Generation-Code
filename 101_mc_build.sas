** ========================================================================== 
** program documentation 
** program     : 101_mc_build.sas
** description : drives the managed care TAF build process and validates and recodes T-MSIS vaiables
**               and creates constructed variables
** input data  : [RedShift T-MSIS Tables]
**                File_Header_Record_Managed_Care (segment 01)
**                Managed_Care_Main (segment 02)
**                Managed_care_location_and_contact_info (segment 03)
**                Managed_care_service_area (segment 04)
**                Managed_care_operating_authority (segment 05)
**                Managed_care_plan_population_enrolled (segment 06)
**                Managed_care_accreditation_organization (segment 07)
** -------------------------------------------------------------------------- 
** history 
** date        | action 
** ------------+------------------------------------------------------------- 
** 06/16/2017  | program written (H. Cohen)
** 07/17/2017  | program updated (H. Cohen) 
** 09/01/2017  | program updated (H. Cohen) 
** 11/03/2017  | program updated (H. Cohen) 
** 07/05/2018  | program updated (H. Cohen) CCB changes
** 10/08/2018  | program updated (H. Cohen) CCB changes
** 03/20/2019  | program updated (H. Cohen) CCB changes
** 02/19/2020  | program updated (H. Cohen) CCB changes
** 06/09/2020  | program updated (H. Cohen) CCB changes
** 12/16/2020  | program updated (H. Cohen) CCB changes
** ==========================================================================;

%let taf = mcp;

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

%let base_path = /sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf;
%let path = /sasdata/users/&sysuserid/&TMSIS/&sub_env/data_analytics/taf/&taf;

%INCLUDE "&base_path/programs/AWS_Shared_Macros.sas";

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

**********************************************************************
* 1. CHECK LOOKUP TABLE TO GET PARAMETERS                            *
* 2. UPDATE LOOKUP TABLE WITH JOB START TIME                         *
**********************************************************************;

PROC SQL;
   sysecho 'in read';
   %TMSIS_CONNECT;
   
       %JOB_CONTROL_RD(&taf.,&DA_SCHEMA.);
%put DA_RUN_ID=&DA_RUN_ID, RPTNG_PRD=&REPORTING_PERIOD, VRSN=&VERSION;
   sysecho 'in update for start timestamp';

      %JOB_CONTROL_UPDT(&DA_RUN_ID.,&DA_SCHEMA.);
	  
   %TMSIS_DISCONNECT;
QUIT;

data _null_;
	begmon =put(intnx('month',input("&REPORTING_PERIOD",YYMMDD12.),0,"BEGINNING"),date9.);
    call symputx('begmon',upcase(begmon));
	call symputx('st_dt',upcase(cats("'",begmon,"'")));
	
	TAF_FILE_DATE = input(put(input("&REPORTING_PERIOD",YYMMDD12.),yymmn6.),6.);
	call symputx('TAF_FILE_DATE',TAF_FILE_DATE);
	
    RPT_PRD = cats("'",put(input(strip("&REPORTING_PERIOD"),YYMMDD12.),date9.),"'");
	call symputx('RPT_PRD',RPT_PRD);
	
    call symputx('DARUNID',compress(&DA_RUN_ID.));

    monyrout = put(year(input("&REPORTING_PERIOD",ANYDTDTE10.)),4.) || put(month(input("&REPORTING_PERIOD",ANYDTDTE10.)),Z2.);
    call symputx('monyrout',upcase(monyrout));

run; 

proc printto log="&path./logs/MC_build_&reporting_period._&DARUNID..log" new;
run;

%put DA_RUN_ID=&DA_RUN_ID, RPTNG_PRD=&REPORTING_PERIOD, VRSN=&VERSION;
%put begmon=&begmon st_dt = &st_dt TAF_FILE_DATE=&TAF_FILE_DATE RPT_PRD=&RPT_PRD;

options noquotelenmax spool formdlim="~" ls=max nocenter compress=yes;

title1 'MACBIS - MC TAF';
title2 'MC Build';
footnote1 "(&sysdate): MACBIS MC TAF";

%INCLUDE "&path/programs/*.sas";

** array sizes;
%let opamax = 15;  /* operating authority variables */
%let acdmax = 3;   /* accreditation variables */

page;
** ==========================================================================;
** redshift TAF build;
proc sql;
  %tmsis_connect;

** get formats for validation and recode ----------------------------------;
  execute( 
   create temp table MC_formats_sm as
    select frmt_name_txt as FMTNAME,
            frmt_name_val as _MCSTART,
            frmt_end_val as _MCEND,
            label as _MCLABEL,
            dtype as _MCTYPE 
            from &DA_SCHEMA..mc_frmt_name_rng order by frmt_name_txt;
  ) by tmsis_passthrough;

  %process_01_MCheader(outtbl=#MC01_Header);
  %process_02_MCmain(maintbl=#MC01_Header, 
                     outtbl=#MC02_Main_RAW);

  ** extract T-MSIS data apply and validate and recode source variables (as needed), use TAF variable names, add linking variables;

  %let srtlist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num;

  execute( 
    %recode_notnull (intbl=#MC02_Main_RAW,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_rcd, 
                    outtbl=#MC02_Main_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  execute( 
    %recode_lookup (intbl=#MC02_Main_STV,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='STFIPN', 
                    srcvar=SUBMTG_STATE_rcd,
                    newvar=State, 
                    outtbl=#MC02_Main_ST,
                    fldtyp=C,
					fldlen=7);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#MC02_Main_STV);

  execute( 
    %recode_lookup (intbl=#MC02_Main_ST,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='REGION', 
                    srcvar=State,
                    newvar=REG_FLAG, 
                    outtbl=#MC02_Main_RG,
                    fldtyp=N);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#MC02_Main_ST);

  execute( 
    %recode_lookup (intbl=#MC02_Main_RG,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='SAREASW', 
                    srcvar=managed_care_service_area,
                    newvar=SAREA_STATEWIDE_IND, 
                    outtbl=#MC02_Main_SW,
                    fldtyp=N);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_RG);

  execute( 
    %recode_lookup (intbl=#MC02_Main_SW,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='SAREAV', 
                    srcvar=managed_care_service_area,
                    newvar=MC_SAREA_CD, 
                    outtbl=#MC02_Main_SA,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_SW);

  execute( 
    %recode_lookup (intbl=#MC02_Main_SA,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='REIMBV', 
                    srcvar=reimbursement_arrangement,
                    newvar=REIMBRSMT_ARNGMT_CD, 
                    outtbl=#MC02_Main_RAC,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_SA);
  
  execute( 
    %recode_lookup (intbl=#MC02_Main_RAC,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='REIMB', 
                    srcvar=REIMBRSMT_ARNGMT_CD,
                    newvar=reimbrsmt_arngmt_CAT,
                    outtbl=#MC02_Main_RA,
                    fldtyp=N);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_RAC);
  
  execute( 
    %recode_lookup (intbl=#MC02_Main_RA,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='PROFV', 
                    srcvar=managed_care_profit_status,
                    newvar=MC_PRFT_STUS_CD, 
                    outtbl=#MC02_Main_PSC,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_RA);
  
  execute( 
    %recode_lookup (intbl=#MC02_Main_PSC,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='CBSA', 
                    srcvar=core_based_statistical_area_code,
                    newvar=CBSA_CD, 
                    outtbl=#MC02_Main_CBSA,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_PSC);
  
  execute( 
    %recode_lookup (intbl=#MC02_Main_CBSA,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='MCPLNTV', 
                    srcvar=managed_care_plan_type,
                    newvar=MC_PLAN_TYPE_CD, 
                    outtbl=#MC02_Main_PTC,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_CBSA);
  
  execute( 
    %recode_lookup (intbl=#MC02_Main_PTC,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='MCPLNTY', 
                    srcvar=MC_PLAN_TYPE_CD,
                    newvar=MC_plan_type_CAT, 
                    outtbl=#MC02_Main_PT,
                    fldtyp=N);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_PTC);
  
  execute( 
    %recode_lookup (intbl=#MC02_Main_PT,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='PGMCDV', 
                    srcvar=managed_care_program,
                    newvar=MC_PGM_CD, 
                    outtbl=#MC02_Main_PRC,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC02_Main_PT);

  execute(
    create table #MC02_Main_CNST
             diststyle key distkey(state_plan_id_num) as
      select &srtlist,
	         tms_run_id as TMSIS_RUN_ID,
             &TAF_FILE_DATE :: varchar(10) as MCP_FIL_DT,
             %nrbquote('&VERSION.') :: varchar(2) as MCP_VRSN,
             &DA_RUN_ID :: integer as DA_RUN_ID,
	         SUBMTG_STATE_CD,
             managed_care_contract_eff_date as MC_CNTRCT_EFCTV_DT,
             MC_CNTRCT_END_DT,
			 REG_FLAG,
			 %upper_case(managed_care_name) as MC_NAME, 
			 MC_PGM_CD,
             MC_PLAN_TYPE_CD,
			 REIMBRSMT_ARNGMT_CD,
			 MC_PRFT_STUS_CD,
			 CBSA_CD,
             percent_business as BUSNS_PCT,
			 MC_SAREA_CD,
             MC_plan_type_CAT,
			 reimbrsmt_arngmt_CAT,
			 SAREA_STATEWIDE_IND,
			 cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY
      from #MC02_Main_PRC
      order by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, recodes_MC02);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, constructed_MC02);
  
  %DROP_temp_tables(#MC02_Main_PRC);


  ** extract T-MSIS data apply subsetting criteria and validate data or create constructed variable for segment MCR00003 ;
  %process_03_location(maintbl=#MC02_Main_RAW, 
                      outtbl=#MC03_Location);

  %let srtlistl = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num,
				 managed_care_location_id;

  execute( 
    %recode_notnull (intbl=#MC03_Location,
                    srtvars=srtlistl,
                    fmttbl=mc_formats_sm,
                    fmtnm='STCDN', 
                    srcvar=managed_care_state,
                    newvar=managed_care_state2, 
                    outtbl=#MC03_Location_STM,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  execute( 
    %recode_lookup (intbl=#MC03_Location_STM,
                    srtvars=srtlistl,
                    fmttbl=mc_formats_sm,
                    fmtnm='STFIPV', 
                    srcvar=managed_care_state2,
                    newvar=MC_STATE_CD, 
                    outtbl=#MC03_Location_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#MC03_Location_STM);

  execute(
    create table #MC03_Location_CNST 
                 diststyle key distkey(mc_plan_id)
                 compound sortkey (TMSIS_RUN_ID, SUBMTG_STATE_CD, mc_plan_id) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as MCP_FIL_DT,
             %nrbquote('&VERSION.') :: varchar(2) as MCP_VRSN,
             tms_run_id as TMSIS_RUN_ID,            
	         SUBMTG_STATE_CD,
             state_plan_id_num as mc_plan_id,
             managed_care_location_id as MC_LCTN_ID, 
             managed_care_location_and_contact_info_eff_date as MC_LCTN_CNTCT_EFCTV_DT,
             managed_care_location_and_contact_info_end_date as MC_LCTN_CNTCT_END_DT,
             managed_care_addr_ln1 as MC_LINE_1_ADR, 
             %upper_case(managed_care_addr_ln2) as MC_LINE_2_ADR,
             %upper_case(managed_care_addr_ln3) as MC_LINE_3_ADR,
             %upper_case(managed_care_city) as MC_CITY_NAME,
             MC_STATE_CD,
             nullif(trim(managed_care_zip_code),'') as MC_ZIP_CD,
             nullif(trim(managed_care_county),'') as MC_CNTY_CD
      from #MC03_Location_STV 
      order by TMSIS_RUN_ID, 
                 SUBMTG_STATE_CD, 
                 mc_plan_id,
				 MC_LCTN_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, recodes_MC03);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, constructed_MC03);

  execute( 
    drop table #MC03_Location;
    drop table #MC03_Location_STV;
  ) by tmsis_passthrough;

  ** extract T-MSIS data apply subsetting criteria and validate data or create constructed variable for segment MCR00004 ;
  %process_04_service_area (maintbl=#MC02_Main_RAW,
                            outtbl=#MC04_Service_Area);

  ** create TAF segment for service area ;
  execute(
    create table #MC04_Service_Area_CNST
                 diststyle key distkey(mc_plan_id)
                 compound sortkey (TMSIS_RUN_ID, SUBMTG_STATE_CD, mc_plan_id) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as MCP_FIL_DT,
             %nrbquote('&VERSION.') :: varchar(2) as MCP_VRSN,
             tms_run_id as TMSIS_RUN_ID,
             SUBMTG_STATE_CD,
			 state_plan_id_num as mc_plan_id,
             managed_care_service_area_eff_date as MC_SAREA_EFCTV_DT,
             managed_care_service_area_end_date as MC_SAREA_END_DT,
             managed_care_service_area_name as MC_SAREA_NAME
      from #MC04_Service_Area
      order by TMSIS_RUN_ID, 
                 SUBMTG_STATE_CD, 
                 mc_plan_id;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, constructed_MC04);

  execute( 
    drop table #MC04_Service_Area;
  ) by tmsis_passthrough;


  ** extract T-MSIS data apply subsetting criteria and validate data or create constructed variable for segment MCR00005 ;
  %process_05_operating_authority (maintbl=#MC02_Main_RAW, 
                                   outtbl=#MC05_Operating_Authority);
                         
  execute( 
    %recode_lookup (intbl=#MC05_Operating_Authority,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='AUTHV', 
                    srcvar=operating_authority,
                    newvar=OPRTG_AUTHRTY_IND, 
                    outtbl=#MC05_Operating_Authority_IND,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  ** map operating authority var arrays;
  execute(
    create table #MC05_Operating_Authority2
                 diststyle key distkey(state_plan_id_num) as
      select &srtlist, 
             managed_care_op_authority_eff_date as MC_OP_AUTH_EFCTV_DT,
             managed_care_op_authority_end_date as MC_OP_AUTH_END_DT,
             case when OPRTG_AUTHRTY_IND='01' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1115_DEMO_IND,
             case when OPRTG_AUTHRTY_IND='02' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915B_IND,
             case when OPRTG_AUTHRTY_IND='03' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_IND,
             case when OPRTG_AUTHRTY_IND='04' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915A_IND,
             case when OPRTG_AUTHRTY_IND='05' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915BC_CONC_IND,
             case when OPRTG_AUTHRTY_IND='06' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915AC_CONC_IND,
             case when OPRTG_AUTHRTY_IND='07' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_1915C_IND,
             case when OPRTG_AUTHRTY_IND='08' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_PACE_IND,
             case when OPRTG_AUTHRTY_IND='09' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1905T_IND,
             case when OPRTG_AUTHRTY_IND='10' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1937_IND,
             case when OPRTG_AUTHRTY_IND='11' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1902A70_IND,
             case when OPRTG_AUTHRTY_IND='12' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915BI_CONC_IND,
             case when OPRTG_AUTHRTY_IND='13' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915AI_CONC_IND,
             case when OPRTG_AUTHRTY_IND='14' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_1915I_IND,
             case when OPRTG_AUTHRTY_IND='15' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1945_HH_IND,
             OPRTG_AUTHRTY_IND as OPRTG_AUTHRTY,
             waiver_id as WVR_ID,
             /* grouping code */
             row_number() over (
               partition by &srtlist
               order by record_number, OPRTG_AUTHRTY_IND, waiver_id asc
             ) as _ndx
      from #MC05_Operating_Authority_IND
	  where OPRTG_AUTHRTY_IND is not null or waiver_id is not null
      order by &srtlist;
  ) by tmsis_passthrough;

  execute (
    create table #MC05_Operating_Authority_Mapped
                 diststyle key distkey(state_plan_id_num) as
      select &srtlist,
             max(OPRTG_AUTHRTY_1115_DEMO_IND) as OPRTG_AUTHRTY_1115_DEMO_IND,
             max(OPRTG_AUTHRTY_1915B_IND) as OPRTG_AUTHRTY_1915B_IND,
             max(OPRTG_AUTHRTY_1932A_IND) as OPRTG_AUTHRTY_1932A_IND,
             max(OPRTG_AUTHRTY_1915A_IND) as OPRTG_AUTHRTY_1915A_IND,
             max(OPRTG_AUTHRTY_1915BC_CONC_IND) as OPRTG_AUTHRTY_1915BC_CONC_IND,
             max(OPRTG_AUTHRTY_1915AC_CONC_IND) as OPRTG_AUTHRTY_1915AC_CONC_IND,
             max(OPRTG_AUTHRTY_1932A_1915C_IND) as OPRTG_AUTHRTY_1932A_1915C_IND,
             max(OPRTG_AUTHRTY_PACE_IND) as OPRTG_AUTHRTY_PACE_IND,
             max(OPRTG_AUTHRTY_1905T_IND) as OPRTG_AUTHRTY_1905T_IND,
             max(OPRTG_AUTHRTY_1937_IND) as OPRTG_AUTHRTY_1937_IND,
             max(OPRTG_AUTHRTY_1902A70_IND) as OPRTG_AUTHRTY_1902A70_IND,
             max(OPRTG_AUTHRTY_1915BI_CONC_IND) as OPRTG_AUTHRTY_1915BI_CONC_IND,
             max(OPRTG_AUTHRTY_1915AI_CONC_IND) as OPRTG_AUTHRTY_1915AI_CONC_IND,
             max(OPRTG_AUTHRTY_1932A_1915I_IND) as OPRTG_AUTHRTY_1932A_1915I_IND,
             max(OPRTG_AUTHRTY_1945_HH_IND) as OPRTG_AUTHRTY_1945_HH_IND
             %map_arrayvars (varnm=WVR_ID, N=&opamax, fldtyp=C)
             %map_arrayvars (varnm=OPRTG_AUTHRTY, N=&opamax, fldtyp=C)
      from #MC05_Operating_Authority2
      group by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, recodes_MC05);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, constructed_MC05);

 execute( 
    drop table #MC05_Operating_Authority;
    drop table #MC05_Operating_Authority_IND;
    drop table #MC05_Operating_Authority2;
  ) by tmsis_passthrough;

  ** extract T-MSIS data apply subsetting criteria and validate data or create constructed variable for segment MCR00006 ;
  %process_06_population (maintbl=#MC02_Main_RAW, 
                          outtbl=#MC06_Population_RAW);

    execute( 
    %recode_lookup (intbl=#MC06_Population_RAW,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ELIG2V', 
                    srcvar=managed_care_plan_pop,
                    newvar=MC_PLAN_POP, 
                    outtbl=#MC06_Population1,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#MC06_Population_RAW);

  execute( 
    %recode_lookup (intbl=#MC06_Population1,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ELIG2FM', 
                    srcvar=MC_PLAN_POP,
                    newvar=POP_ENR_CAT, 
                    outtbl=#MC06_Population2,
                    fldtyp=N);
  ) by tmsis_passthrough;

  ** create TAF segment for enrolled population ;
  execute(
    create table #MC06_Population
                 diststyle key distkey(mc_plan_id)
                 compound sortkey (TMSIS_RUN_ID, SUBMTG_STATE_CD, mc_plan_id) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as MCP_FIL_DT,
             %nrbquote('&VERSION.') :: varchar(2) as MCP_VRSN,
             tms_run_id as TMSIS_RUN_ID,
             SUBMTG_STATE_CD,
			 state_plan_id_num as mc_plan_id,
             managed_care_plan_pop_eff_date as MC_PLAN_POP_EFCTV_DT,
             managed_care_plan_pop_end_date as MC_PLAN_POP_END_DT,
             MC_PLAN_POP
      from #MC06_Population1
	  where MC_PLAN_POP is not null
      order by TMSIS_RUN_ID, 
                 SUBMTG_STATE_CD, 
                 mc_plan_id;
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#MC06_Population1);

  ** map enrolled population indicators for Base ;
  execute(
    create table #MC06_Population_CNST
                 diststyle key distkey(state_plan_id_num) as
      select &srtlist, 
             managed_care_plan_pop_eff_date as MC_PLAN_POP_EFCTV_DT,
             managed_care_plan_pop_end_date as MC_PLAN_POP_END_DT,
             case 
               when POP_ENR_CAT=1 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_MDCD_MAND_COV_ADLT_IND,
             case
               when POP_ENR_CAT=2 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_MDCD_MAND_COV_ABD_IND,
             case
               when POP_ENR_CAT=3 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_MDCD_OPTN_COV_ADLT_IND,
             case
               when POP_ENR_CAT=4 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_MDCD_OPTN_COV_ABD_IND,             
             case
               when POP_ENR_CAT=5 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_MDCD_MDCLY_NDY_ADLT_IND,
             case
               when POP_ENR_CAT=6 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_MDCD_MDCLY_NDY_ABD_IND,
             case
               when POP_ENR_CAT=7 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_CHIP_COV_CHLDRN_IND,
             case
               when POP_ENR_CAT=8 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_CHIP_OPTN_CHLDRN_IND,
             case
               when POP_ENR_CAT=9 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_CHIP_OPTN_PRGNT_WMN_IND,
             case
               when POP_ENR_CAT=10 then 1
			   when POP_ENR_CAT is null then null
               else 0 
             end :: smallint as POP_1115_EXPNSN_IND,
             case
               when POP_ENR_CAT between 1 and 11 then 0
               else -1
             end :: smallint as POP_UNK_IND
      from #MC06_Population2
	  where MC_PLAN_POP is not null
      order by &srtlist;
  ) by tmsis_passthrough;
 
  execute (
    create table #MC06_Population_Mapped
                 diststyle key distkey(state_plan_id_num) as
      select &srtlist,
             max(POP_MDCD_MAND_COV_ADLT_IND) as POP_MDCD_MAND_COV_ADLT_IND,
             max(POP_MDCD_MAND_COV_ABD_IND) as POP_MDCD_MAND_COV_ABD_IND,
             max(POP_MDCD_OPTN_COV_ADLT_IND) as POP_MDCD_OPTN_COV_ADLT_IND,
             max(POP_MDCD_OPTN_COV_ABD_IND) as POP_MDCD_OPTN_COV_ABD_IND,
             max(POP_MDCD_MDCLY_NDY_ADLT_IND) as POP_MDCD_MDCLY_NDY_ADLT_IND,
             max(POP_MDCD_MDCLY_NDY_ABD_IND) as POP_MDCD_MDCLY_NDY_ABD_IND,
             max(POP_CHIP_COV_CHLDRN_IND) as POP_CHIP_COV_CHLDRN_IND,
             max(POP_CHIP_OPTN_CHLDRN_IND) as POP_CHIP_OPTN_CHLDRN_IND,
             max(POP_CHIP_OPTN_PRGNT_WMN_IND) as POP_CHIP_OPTN_PRGNT_WMN_IND,
             max(POP_1115_EXPNSN_IND) as POP_1115_EXPNSN_IND,
             (-1)*max(POP_UNK_IND) as POP_UNK_IND
      from #MC06_Population_CNST
      group by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, segment_MC06);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, recodes_MC06);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, constructed_MC06);
 
  execute( 
    drop table #MC06_Population2;
    drop table #MC06_Population_CNST;
  ) by tmsis_passthrough;


  ** extract T-MSIS data apply subsetting criteria and validate data or create constructed variable for segment MCR00007 ;
  %process_07_accreditation (maintbl=#MC02_Main_RAW, 
                             outtbl=#MC07_Accreditation);

  execute( 
    %recode_lookup (intbl=#MC07_Accreditation,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ACRVAL', 
                    srcvar=accreditation_organization,
                    newvar=ACRDTN_ORG, 
                    outtbl=#MC07_Accreditation_val,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
 
  execute (
    create table #MC07_Accreditation2
                 diststyle key distkey(state_plan_id_num) as
      select *,
              date_accreditation_achieved as ACRDTN_ORG_ACHVMT_DT,
              date_accreditation_end as ACRDTN_ORG_END_DT,
             /* grouping code */
             row_number() over (
               partition by &srtlist
               order by record_number, ACRDTN_ORG asc
             ) as _ndx
      from #MC07_Accreditation_val
	  where ACRDTN_ORG is not null
      order by &srtlist;
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#MC07_Accreditation_val);

  execute( 
    %recode_lookup (intbl=#MC07_Accreditation2,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ACRNCQA', 
                    srcvar=ACRDTN_ORG,
                    newvar=ACRDTN_NCQA, 
                    outtbl=#MC07_Accreditation_NCQA,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  execute( 
    %recode_lookup (intbl=#MC07_Accreditation_NCQA,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ACRURAC', 
                    srcvar=ACRDTN_ORG,
                    newvar=ACRDTN_URAC, 
                    outtbl=#MC07_Accreditation_URAC,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#MC07_Accreditation_NCQA);

  execute( 
    %recode_lookup (intbl=#MC07_Accreditation_URAC,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ACRAAHC', 
                    srcvar=ACRDTN_ORG,
                    newvar=ACRDTN_AAHC, 
                    outtbl=#MC07_Accreditation_AAHC,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#MC07_Accreditation_URAC);

  execute( 
    %recode_lookup (intbl=#MC07_Accreditation_AAHC,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ACRNONE', 
                    srcvar=ACRDTN_ORG,
                    newvar=ACRDTN_NONE, 
                    outtbl=#MC07_Accreditation_NONE,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#MC07_Accreditation_AAHC);

  execute( 
    %recode_lookup (intbl=#MC07_Accreditation_NONE,
                    srtvars=srtlist,
                    fmttbl=mc_formats_sm,
                    fmtnm='ACROTHR', 
                    srcvar=ACRDTN_ORG,
                    newvar=ACRDTN_OTHR, 
                    outtbl=#MC07_Accreditation_OTHR,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#MC07_Accreditation_NONE);

  execute (
    create table #MC07_Accreditation_Mapped
                 diststyle key distkey(state_plan_id_num) as
      select &srtlist,
             max(ACRDTN_NCQA) as ACRDTN_NCQA,
             max(ACRDTN_URAC) as ACRDTN_URAC,
             max(ACRDTN_AAHC) as ACRDTN_AAHC,
             max(ACRDTN_NONE) as ACRDTN_NONE,
             max(ACRDTN_OTHR) as ACRDTN_OTHR
             %map_arrayvars (varnm=ACRDTN_ORG, N=&acdmax, fldtyp=C)
             %map_arrayvars (varnm=ACRDTN_ORG_ACHVMT_DT, N=&acdmax, fldtyp=D)
			 %map_arrayvars (varnm=ACRDTN_ORG_END_DT, N=&acdmax, fldtyp=D)
      from #MC07_Accreditation_OTHR
      group by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, recodes_MC07);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, constructed_MC07);
 
  execute( 
    drop table #MC07_Accreditation;
    drop table #MC07_Accreditation2;
    drop table #MC07_Accreditation_OTHR;
  ) by tmsis_passthrough;
 
  ** complete base/main table by adding operating authroity, enrollment, and accreditation information ;
  execute(
    create table #MC02_Base
                diststyle key distkey(mc_plan_id) as
         select M.DA_RUN_ID,
                M.mcp_link_key,
                M.MCP_FIL_DT,
                M.MCP_VRSN,
                M.tmsis_run_id,
                M.submtg_state_cd,
                M.state_plan_id_num as mc_plan_id,
                M.mc_cntrct_efctv_dt,
                M.mc_cntrct_end_dt,
                M.REG_FLAG,
                M.mc_name,
                M.mc_pgm_cd,
                M.mc_plan_type_cd,
                M.reimbrsmt_arngmt_cd,
                M.mc_prft_stus_cd,
                M.cbsa_cd,
                M.busns_pct,
                M.mc_sarea_cd,
                M.mc_plan_type_cat,
                M.reimbrsmt_arngmt_cat,
                M.sarea_statewide_ind,
                O.OPRTG_AUTHRTY_1115_demo_ind,
                O.OPRTG_AUTHRTY_1915b_ind,
                O.OPRTG_AUTHRTY_1932a_ind,
                O.OPRTG_AUTHRTY_1915a_ind,
                O.OPRTG_AUTHRTY_1915bc_conc_ind,
                O.OPRTG_AUTHRTY_1915ac_conc_ind,
                O.OPRTG_AUTHRTY_1932A_1915C_IND,
                O.OPRTG_AUTHRTY_pace_ind,
                O.OPRTG_AUTHRTY_1905t_ind,
                O.OPRTG_AUTHRTY_1937_ind,
                O.OPRTG_AUTHRTY_1902a70_ind,
                O.OPRTG_AUTHRTY_1915bi_conc_ind,
                O.OPRTG_AUTHRTY_1915ai_conc_ind,
                O.OPRTG_AUTHRTY_1932A_1915I_IND,
                O.OPRTG_AUTHRTY_1945_HH_ind,
				O.wvr_id_01,
				O.wvr_id_02,
				O.wvr_id_03,
				O.wvr_id_04,
				O.wvr_id_05,
				O.wvr_id_06,
				O.wvr_id_07,
				O.wvr_id_08,
				O.wvr_id_09,
				O.wvr_id_10,
				O.wvr_id_11,
				O.wvr_id_12,
				O.wvr_id_13,
				O.wvr_id_14,
				O.wvr_id_15,
				O.OPRTG_AUTHRTY_01,
				O.OPRTG_AUTHRTY_02,
				O.OPRTG_AUTHRTY_03,
				O.OPRTG_AUTHRTY_04,
				O.OPRTG_AUTHRTY_05,
				O.OPRTG_AUTHRTY_06,
				O.OPRTG_AUTHRTY_07,
				O.OPRTG_AUTHRTY_08,
				O.OPRTG_AUTHRTY_09,
				O.OPRTG_AUTHRTY_10,
				O.OPRTG_AUTHRTY_11,
				O.OPRTG_AUTHRTY_12,
				O.OPRTG_AUTHRTY_13,
				O.OPRTG_AUTHRTY_14,
				O.OPRTG_AUTHRTY_15,
                E.pop_mdcd_mand_COV_ADLT_IND,
                E.pop_mdcd_mand_cov_abd_ind,
                E.pop_mdcd_optn_COV_ADLT_IND,
                E.pop_mdcd_optn_cov_abd_ind,
                E.POP_MDCD_MDCLY_NDY_adlt_ind,
                E.POP_MDCD_MDCLY_NDY_abd_ind,
                E.pop_chip_cov_CHLDRN_ind,
                E.pop_chip_optn_CHLDRN_ind,
                E.pop_chip_optn_prgnt_WMN_ind,
                E.pop_1115_EXPNSN_ind,
                E.pop_UNK_ind,
                A.acrdtn_ncqa,
                A.acrdtn_urac,
                A.acrdtn_aahc,
                A.acrdtn_none,
                A.acrdtn_othr,
				A.acrdtn_org_01,
				A.acrdtn_org_02,
				A.acrdtn_org_03,
                A.acrdtn_org_achvmt_dt_01,
                A.acrdtn_org_achvmt_dt_02,
                A.acrdtn_org_achvmt_dt_03,
                A.acrdtn_org_end_dt_01,
                A.acrdtn_org_end_dt_02,
                A.acrdtn_org_end_dt_03
         from #MC02_Main_CNST M 
           left join #MC05_Operating_Authority_Mapped O
             on %write_equalkeys(keyvars=srtlist, t1=M, t2=O)
           left join #MC06_Population_Mapped E
             on %write_equalkeys(keyvars=srtlist, t1=M, t2=E)		   
           left join #MC07_Accreditation_Mapped A
             on %write_equalkeys(keyvars=srtlist, t1=M, t2=A)
         order by M.TMSIS_RUN_ID, 
                 M.SUBMTG_STATE_CD, 
                 mc_plan_id;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_mc_build.sas, base_MCP);


  * insert contents of each remaining temp table into final TAF files;

  execute(
    insert into &DA_SCHEMA..TAF_MCP
    select *
	from #MC02_Base 
  ) by tmsis_passthrough;
  
  execute(
    insert into &DA_SCHEMA..TAF_MCL
    select *
	from #MC03_Location_CNST 
  ) by tmsis_passthrough;

  execute(
    insert into &DA_SCHEMA..TAF_MCS
    select *
	from #MC04_Service_Area_CNST 
  ) by tmsis_passthrough;

    execute(
    insert into &DA_SCHEMA..TAF_MCE
    select *
	from #MC06_Population 
  ) by tmsis_passthrough;


** complete the TAF build by updating external references used for all TAF ;

 	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);
	sysecho 'in get cnt';
	%let TABLE_NAME = TAF_MCP;
	%let FIL_4TH_NODE = MCP;
    title3 "Count: TAF MCP Base"; 
	%GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_mc_build.sas, base_MCP, #MC02_Base, submtg_state_cd);   
    sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);
	
    %let TABLE_NAME = TAF_MCL;
	%let FIL_4TH_NODE = MCL;
    sysecho 'in get cnt';
    title3 "Count: TAF MCL"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_mc_build.sas, constructed_MC03, #MC03_Location_CNST, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);
	
    %let TABLE_NAME = TAF_MCS;
	%let FIL_4TH_NODE = MCS;
    title3 "Count: TAF MCS"; 
    sysecho 'in get cnt';
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_mc_build.sas, constructed_MC04, #MC04_Service_Area_CNST, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);
	
    %let TABLE_NAME = TAF_MCE;
	%let FIL_4TH_NODE = MCE;
    title3 "Count: TAF MCE"; 
    sysecho 'in get cnt';
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_mc_build.sas, segment_MC06, #MC06_Population, submtg_state_cd);   
    sysecho 'In Control Info';
	%FINAL_CONTROL_INFO(&DA_RUN_ID., &DA_SCHEMA.);	
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);
	
  %tmsis_disconnect;
quit;

/* PRINT INFO FROM JOB CONTROL TABLE */
PROC PRINT DATA=FINAL_CONTROL_INFO;
TITLE "FINAL CONTROL TABLE INFO";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_MCP;
TITLE "FINAL CONTENT LISTING OF THE TAF MCP HEADER TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_MCL;
TITLE "FINAL CONTENT LISTING OF THE TAF MCL LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_MCS;
TITLE "FINAL CONTENT LISTING OF THE TAF MCS SERVICE AREA TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_MCE;
TITLE "FINAL CONTENT LISTING OF THE TAF MCE ENROLLED POPULATION TABLE";
RUN;

proc printto; run;

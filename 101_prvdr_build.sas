** ========================================================================== 
** program documentation 
** program     : 101_prvdr_build.sas
** project     : MACBIS - Provider TAF
** programmer  : Dan Whalen
** description : drives the provider build process
** input data  : [RedShift Tables]
**                File_Header_Record_Provider (segment 01)
**                Prov_Attributes_Main  (segment 02)
**                Prov_Location_And_Contact_Info (segment 03)
**                Prov_Licensing_Info  (segment 04)
**                Prov_Identifiers  (segment 05)
**                Prov_Taxonomy_Classification (segment 06)
**                Prov_Medicaid_Enrollment (segment 07)
**                Prov_Affiliated_Groups (segment 08)
**                Prov_Affiliated_Programs (segment 09)
**                Prov_Bed_Type_Info (segnebt 10)
** output data : PRV_YYYYMM_yyyymmdd.sas7bdat
**                 YYYYMM=report period
**                 yyyymmdd=creation date

** -------------------------------------------------------------------------- 
** history 
** date        | action 
** ------------+------------------------------------------------------------- 
** 03/15/2017  | program written (D. Whalen)
** 07/17/2017  | program updated (H. Cohen)
** 11/03/2017  | program updated (H. Cohen) production pre-dev
** 07/05/2018  | program updated (H. Cohen) CCB changes
** 10/08/2018  | program updated (H. Cohen) CCB changes
** 03/25/2019  | program updated (H. Cohen) CCB changes
** 02/24/2020  | program updated (H. Cohen) CCB changes
** --------------------------------------------------------------------------;
%let taf = prv;

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

proc printto log="&path./logs/PRV_build_&reporting_period._&DARUNID..log" new;
run;

%put DA_RUN_ID=&DA_RUN_ID, RPTNG_PRD=&REPORTING_PERIOD, VRSN=&VERSION;
%put begmon=&begmon st_dt = &st_dt TAF_FILE_DATE=&TAF_FILE_DATE RPT_PRD=&RPT_PRD;

options noquotelenmax spool formdlim="~" ls=max nocenter compress=yes;
* options noquotelenmax mprint symbolgen spool formdlim="~" ls=max nocenter compress=yes;
* options nomprint nosymbolgen nomlogic ;

title1 'MACBIS - PRV TAF';
title2 'PRV Build';
footnote1 "(&sysdate): MACBIS PRV TAF";

%INCLUDE "&path/programs/*.sas";

page;
** ==========================================================================;
** redshift TAF build;
proc sql;
  %tmsis_connect;

  ** get formats for validation and recode ----------------------------------;
  execute( 
   create temp table PRV_formats_sm as
    select frmt_name_txt as FMTNAME,
            frmt_name_strt_val as START,
            frmt_end_val as END,
            label as LABEL,
            dtype as TYPE 
            from &DA_SCHEMA..prv_frmt_name_rng order by frmt_name_txt;
  ) by tmsis_passthrough;

  execute(
    create table #SPCLlst as
      select start, 
	  case when label like '%CHIP' then 'CHIP'
	       when label like '%TPA' then 'TPA'
	  end as SPCL
	  from PRV_formats_sm where fmtname='STFIPS' and (label like '%CHIP' or label like '%TPA')
      order by start;
  ) by tmsis_passthrough;

  %process_01_header(outtbl=#Prov01_Header);
  %process_02_main(runtbl=#Prov01_Header, 
                   outtbl=#Prov02_Main);

** add code to validate and recode source variables (when needed), use SAS variable names, add linking variables and sort records;

  %let srtlist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id;
 
  execute( 
    %recode_notnull (intbl=#Prov02_Main,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov02_Main_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  execute( 
    %recode_lookup (intbl=#Prov02_Main_STV,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPN', 
                    srcvar=SUBMTG_STATE_CD,
                    newvar=State, 
                    outtbl=#Prov02_Main_ST,
                    fldtyp=C,
					fldlen=7);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_STV);

  execute( 
    %recode_lookup (intbl=#Prov02_Main_ST,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='REGION', 
                    srcvar=State,
                    newvar=REG_FLAG, 
                    outtbl=#Prov02_Main_RG,
                    fldtyp=N);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_ST);

  execute( 
    %recode_lookup (intbl=#Prov02_Main_RG,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='PROVCLSS', 
                    srcvar=facility_group_individual_code,
                    newvar=FAC_GRP_INDVDL_CD, 
                    outtbl=#Prov02_Main_PC,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_RG);
 
  execute( 
    %recode_lookup (intbl=#Prov02_Main_PC,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='TEACHV', 
                    srcvar=teaching_ind,
                    newvar=TCHNG_IND, 
                    outtbl=#Prov02_Main_TF,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_PC);

  execute( 
    %recode_lookup (intbl=#Prov02_Main_TF,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='OWNERV', 
                    srcvar=ownership_code,
                    newvar=OWNRSHP_CD, 
                    outtbl=#Prov02_Main_OV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_TF);

  execute( 
    %recode_lookup (intbl=#Prov02_Main_OV,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='OWNER', 
                    srcvar=ownership_code,
                    newvar=OWNRSHP_CAT, 
                    outtbl=#Prov02_Main_O,
                    fldtyp=N);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_OV);

  execute( 
    %recode_lookup (intbl=#Prov02_Main_O,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='PROFV', 
                    srcvar=prov_profit_status,
                    newvar=PRVDR_PRFT_STUS_CD, 
                    outtbl=#Prov02_Main_PS,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_O);

  execute( 
    %recode_lookup (intbl=#Prov02_Main_PS,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='NEWPATV', 
                    srcvar=accepting_new_patients_ind,
                    newvar=ACPT_NEW_PTNTS_IND, 
                    outtbl=#Prov02_Main_NP,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_PS);

  ** separate processing for records that are inividual type;
  execute(
  create table #Prov02_Main_INDV
         diststyle key distkey(submitting_state_prov_id) 
         compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
    select *, 
	case 
	  when date_of_birth is null or date_cmp(date_of_birth,&RPT_PRD)=1 then null
	  when date_of_death is not null and date_cmp(date_of_death,&RPT_PRD) in (-1,0) then floor((date_of_death-date_of_birth)/365.25)
	  else floor((&RPT_PRD-date_of_birth)/365.25)
	end :: smallint as AGE_NUM,
	case when date_cmp(date_of_death,&RPT_PRD)=1 then null else date_of_death end as DEATH_DT
    from #Prov02_Main_NP
    where FAC_GRP_INDVDL_CD='03' or FAC_GRP_INDVDL_CD is null
    order by &srtlist;
  ) by tmsis_passthrough; 

  execute( 
    %recode_lookup (intbl=#Prov02_Main_INDV,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='GENDERV', 
                    srcvar=sex,
                    newvar=GNDR_CD, 
                    outtbl=#Prov02_Main_GC,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;

  %let var02nind = tms_run_id,
                tms_reporting_period,
                submitting_state,
                record_number,			
                submitting_state_prov_id,
                prov_attributes_eff_date,
                prov_attributes_end_date,
                prov_doing_business_as_name,
                prov_legal_name,
                prov_organization_name,
                prov_tax_name,
				SUBMTG_STATE_CD,
				State,
				REG_FLAG,
                FAC_GRP_INDVDL_CD,
                TCHNG_IND,
                OWNRSHP_CD,
				OWNRSHP_CAT,
                PRVDR_PRFT_STUS_CD,
                ACPT_NEW_PTNTS_IND;

  %let var02ind = prov_first_name,
                prov_middle_initial,
                prov_last_name,
                GNDR_CD,
				AGE_NUM,
                date_of_birth,
                DEATH_DT;

  execute( 
  create table #Prov02_Main_All 
                diststyle key distkey(submitting_state_prov_id)
                compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
    select R.SPCL, %write_keyprefix(keyvars=var02nind, prefix=R), %write_keyprefix(keyvars=var02ind, prefix=T)
    from #Prov02_Main_NP R
         left join #Prov02_Main_GC T
           on %write_equalkeys(keyvars=srtlist, t1=R, t2=T)
    order by %write_keyprefix(keyvars=srtlist, prefix=R);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov02_Main_GC);

  execute(
    create table #Prov02_Main_CNST
	  diststyle key distkey(submitting_state_prov_id) as
      select &srtlist,
	         tms_run_id as TMSIS_RUN_ID,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
             &DA_RUN_ID :: integer as DA_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
			 REG_FLAG,
			 %upper_case(prov_doing_business_as_name) as PRVDR_DBA_NAME, 
			 %upper_case(prov_legal_name) as PRVDR_LGL_NAME, 
			 %upper_case(prov_organization_name) as PRVDR_ORG_NAME, 
			 %upper_case(prov_tax_name) as PRVDR_TAX_NAME, 
			 FAC_GRP_INDVDL_CD,
			 TCHNG_IND,
			 %upper_case(prov_first_name) as PRVDR_1ST_NAME, 
			 %upper_case(prov_middle_initial) as PRVDR_MDL_INITL_NAME, 
			 %upper_case(prov_last_name) as PRVDR_LAST_NAME,
			 GNDR_CD,
			 OWNRSHP_CD,
			 OWNRSHP_CAT,
			 PRVDR_PRFT_STUS_CD,
			 date_of_birth as BIRTH_DT,
			 DEATH_DT,
			 ACPT_NEW_PTNTS_IND,
			 case 
			   when AGE_NUM<15 then null
			   when AGE_NUM>125 then 125
			   else AGE_NUM
			 end as AGE_NUM,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || SPCL) as varchar(50))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50))
			 end as PRV_LINK_KEY
      from #Prov02_Main_All
      order by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov02);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, individual_Prov02);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, all_types_Prov02);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov02);

  execute(
    drop table #Prov02_Main_NP;
	drop table #Prov02_Main_INDV;
	drop table #Prov02_Main_All;
	) by tmsis_passthrough;
  
  
 ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00003 ;
  %process_03_locations(maintbl=#Prov02_Main,
                        outtbl=#Prov03_Locations);  

  %let srtlistl = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
				 prov_location_id;
  
  execute( 
    %recode_notnull (intbl=#Prov03_Locations,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov03_Locations_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  execute(
    create table #Prov03_Locations_link
                diststyle key distkey(submitting_state_prov_id)
                compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
      select *,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**') || '-' || SPCL) as varchar(74))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74))
			 end as PRV_LOC_LINK_KEY
      from #Prov03_Locations_STV
      order by &srtlistl;
  ) by tmsis_passthrough;

  %let keyl = PRV_LOC_LINK_KEY;
  
  %DROP_temp_tables(#Prov03_Locations_STV);

  execute( 
    %recode_notnull (intbl=#Prov03_Locations_link,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='STCDN', 
                    srcvar=addr_state,
                    newvar=addr_state2, 
                    outtbl=#Prov03_Locations_AST1,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  execute( 
    %recode_lookup (intbl=#Prov03_Locations_AST1,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPV', 
                    srcvar=addr_state2,
                    newvar=ADR_STATE_CD, 
                    outtbl=#Prov03_Locations_AST,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov03_Locations_AST1);

  execute( 
    %recode_lookup (intbl=#Prov03_Locations_AST,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='BRDRSTV',
                    srcvar=addr_border_state_ind,
                    newvar=ADR_BRDR_STATE_IND, 
                    outtbl=#Prov03_Locations_BS,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov03_Locations_AST);

  ** create separate variables for provider address type values;
  execute(
    create table #Prov03_Location_TYPE
                diststyle key distkey(submitting_state_prov_id)
                compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
      select *, 
             case
               when prov_addr_type='1' then 1
			   when prov_addr_type='3' or prov_addr_type='4' then 0
               else null 
             end :: smallint as PRVDR_ADR_BLG_IND,
             case
               when prov_addr_type='3' then 1
			   when prov_addr_type='1' or prov_addr_type='4' then 0
               else null 
             end :: smallint as PRVDR_ADR_PRCTC_IND,
             case
               when prov_addr_type='4' then 1
			   when prov_addr_type='3' or prov_addr_type='1' then 0
               else null 
             end :: smallint as PRVDR_ADR_SRVC_IND
			 from #Prov03_Locations_BS 
      order by &keyl;
  ) by tmsis_passthrough;
  
  ** group by location ID;
  execute (
    create table #Prov03_Location_mapt
                diststyle key distkey(&keyl)
                compound sortkey (&keyl) as
      select &srtlistl, &keyl,
             max(PRVDR_ADR_BLG_IND) as PRVDR_ADR_BLG_IND,
             max(PRVDR_ADR_PRCTC_IND) as PRVDR_ADR_PRCTC_IND,
             max(PRVDR_ADR_SRVC_IND) as PRVDR_ADR_SRVC_IND
      from #Prov03_Location_TYPE
      group by &srtlistl, &keyl;
  ) by tmsis_passthrough;

  execute(
    create table #Prov03_Location_BSM
                diststyle key distkey(&keyl)
                compound sortkey (&keyl) as
      select &srtlistl, SPCL,
	            tms_reporting_period,
                record_number,
                prov_addr_type,
                prov_location_and_contact_info_eff_date,
                prov_location_and_contact_info_end_date,
                addr_ln1,
                addr_ln2,
                addr_ln3,
                addr_city,
                addr_state,
                addr_zip_code,
                addr_county,
                addr_border_state_ind,
				SUBMTG_STATE_CD,
				ADR_STATE_CD,
                ADR_BRDR_STATE_IND,
				PRV_LOC_LINK_KEY
			 from #Prov03_Locations_BS 
      order by &keyl;
  ) by tmsis_passthrough;


  execute(
    create table #Prov03_Location_CNST
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when T.SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || T.SUBMTG_STATE_CD || '-' || coalesce(T.submitting_state_prov_id, '*') || '-' || T.SPCL) as varchar(50))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || T.SUBMTG_STATE_CD || '-' || coalesce(T.submitting_state_prov_id, '*')) as varchar(50))
			 end as PRV_LINK_KEY,
             T.PRV_LOC_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
             T.tms_run_id as TMSIS_RUN_ID,
	         T.SUBMTG_STATE_CD,
			 T.submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
             T.prov_location_id as PRVDR_LCTN_ID,
             L.PRVDR_ADR_BLG_IND,
             L.PRVDR_ADR_PRCTC_IND,
             L.PRVDR_ADR_SRVC_IND,
             %upper_case(T.addr_ln1) as ADR_LINE_1_TXT, 
             %upper_case(T.addr_ln2) as ADR_LINE_2_TXT,
             %upper_case(T.addr_ln3) as ADR_LINE_3_TXT,
			 %upper_case(T.addr_city) as ADR_CITY_NAME,
             T.ADR_STATE_CD,
             T.addr_zip_code as ADR_ZIP_CD,
			 T.addr_county as ADR_CNTY_CD,
			 T.ADR_BRDR_STATE_IND,
             case
               when L.PRVDR_ADR_SRVC_IND=1 and T.SUBMTG_STATE_CD=T.ADR_STATE_CD and T.SUBMTG_STATE_CD is not null and T.ADR_STATE_CD is not null then 0
               when L.PRVDR_ADR_SRVC_IND=1 and T.SUBMTG_STATE_CD<>T.ADR_STATE_CD and T.SUBMTG_STATE_CD is not null and T.ADR_STATE_CD is not null then 1 
               else null 
             end :: smallint as PRVDR_SRVC_ST_DFRNT_SUBMTG_ST
      from #Prov03_Location_BSM T 
           left join #Prov03_Location_mapt L
             on T.&keyl=L.&keyl
	  where T.prov_addr_type='4' or (T.prov_addr_type='3' and (L.PRVDR_ADR_SRVC_IND is null or L.PRVDR_ADR_SRVC_IND=0)) or (T.prov_addr_type='1' and (L.PRVDR_ADR_SRVC_IND is null or L.PRVDR_ADR_SRVC_IND=0) and (L.PRVDR_ADR_PRCTC_IND is null or L.PRVDR_ADR_PRCTC_IND=0))
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, temp_link_Prov03);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov03);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, type_Prov03);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_1_Prov03);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_2_Prov03);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_3_Prov03);

  execute( 
	drop table #Prov03_Locations_link;
    drop table #Prov03_Locations_BS;
    drop table #Prov03_Location_TYPE;
    drop table #Prov03_Location_mapt;
	drop table #Prov03_Location_BSM;
    drop table #Prov03_Location_mapped;
  ) by tmsis_passthrough;

* insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV_LOC
    select *
	from #Prov03_Location_CNST 
  ) by tmsis_passthrough;

  execute( 
	drop table #Prov03_Location_CNST;
  ) by tmsis_passthrough;


  ** extract licensing data;
  ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00004 ;

  %process_04_licensing (loctbl=#Prov03_Locations, 
                         outtbl=#Prov04_Licensing);

  execute( 
    %recode_notnull (intbl=#Prov04_Licensing,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov04_Licensing_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov04_Licensing);

  execute( 
    %recode_lookup (intbl=#Prov04_Licensing_STV,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='LICCDV', 
                    srcvar=license_type,
                    newvar=LCNS_TYPE_CD, 
                    outtbl=#Prov04_Licensing_TYP,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov04_Licensing_STV);

  execute(
    create table #Prov04_Licensing_CNST
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**') || '-' || SPCL) as varchar(74))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74))
			 end as PRV_LOC_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
             prov_location_id as PRVDR_LCTN_ID,
             LCNS_TYPE_CD,
             license_or_accreditation_number as LCNS_OR_ACRDTN_NUM,
             license_issuing_entity_id as LCNS_ISSG_ENT_ID_TXT
			 from #Prov04_Licensing_TYP
			 where LCNS_TYPE_CD is not null
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov04);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov04);

  %DROP_temp_tables(#Prov04_Licensing_TYP);

* insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV_LIC
    select *
	from #Prov04_Licensing_CNST 
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov04_Licensing_CNST);


  ** extract identifiers data;
  ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00005 ;
 
  %process_05_identifiers (loctbl=#Prov03_Locations, 
                           outtbl=#Prov05_Identifiers);

  execute( 
    %recode_notnull (intbl=#Prov05_Identifiers,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov05_Identifiers_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov05_Identifiers);

  execute( 
    %recode_lookup (intbl=#Prov05_Identifiers_STV,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='IDCDV', 
                    srcvar=prov_identifier_type,
                    newvar=PRVDR_ID_TYPE_CD, 
                    outtbl=#Prov05_Identifiers_TYP,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
  
  %DROP_temp_tables(#Prov05_Identifiers_STV);

  execute(
    create table #Prov05_Identifiers_CNST
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**') || '-' || SPCL) as varchar(74))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74))
			 end as PRV_LOC_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
             prov_location_id as PRVDR_LCTN_ID,
             PRVDR_ID_TYPE_CD,
             prov_identifier as PRVDR_ID,
             prov_identifier_issuing_entity_id as PRVDR_ID_ISSG_ENT_ID_TXT
			 from #Prov05_Identifiers_TYP
			 where PRVDR_ID_TYPE_CD is not null
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID;
  ) by tmsis_passthrough;
   
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov05);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov05);
  
  %DROP_temp_tables(#Prov05_Identifiers_TYP);

* insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV_IDT
    select *
	from #Prov05_Identifiers_CNST 
  ) by tmsis_passthrough;
   
  %DROP_temp_tables(#Prov05_Identifiers_CNST);
 

  ** extract bed type data;
  ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00010 ;

  %process_10_beds (loctbl=#Prov03_Locations, 
                    outtbl=#Prov10_BedType);

  execute( 
    %recode_notnull (intbl=#Prov10_BedType,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov10_BedType_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov10_BedType);

  execute( 
    %recode_lookup (intbl=#Prov10_BedType_STV,
                    srtvars=srtlistl,
                    fmttbl=prv_formats_sm,
                    fmtnm='BEDCDV', 
                    srcvar=bed_type_code,
                    newvar=BED_TYPE_CD, 
                    outtbl=#Prov10_BedType_TYP,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov10_BedType_STV);

  execute(
    create table #Prov10_BedType_CNST
		diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
		select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**') || '-' || SPCL) as varchar(74))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74))
			 end as PRV_LOC_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
             prov_location_id as PRVDR_LCTN_ID,
             BED_TYPE_CD,
             bed_count as BED_CNT
		from #Prov10_BedType_TYP
		where BED_TYPE_CD is not null or bed_count is not null
		order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov10);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov10);

  %DROP_temp_tables(#Prov10_BedType_TYP);

* insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV_BED
    select *
	from #Prov10_BedType_CNST 
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov10_BedType_CNST);
 ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00006 ;
						
  %process_06_taxonomy(maintbl=#Prov02_Main,
                       outtbl=#Prov06_Taxonomies);

				
** create the separate linked child table;
** add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records;

  execute( 
    %recode_notnull (intbl=#Prov06_Taxonomies,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov06_Taxonomies_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies);
 
  execute( 
    %recode_lookup (intbl=#Prov06_Taxonomies_STV,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='CLSSCDV', 
                    srcvar=prov_classification_type,
                    newvar=PRVDR_CLSFCTN_TYPE_CD, 
                    outtbl=#Prov06_Taxonomies_TYP,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies_STV);

** validate a portion of the provider classification codes ;

  execute(
    create table #Prov06_Taxonomies_CCD
	  diststyle key distkey(submitting_state_prov_id) as
      select *,
             case when PRVDR_CLSFCTN_TYPE_CD='1' then prov_classification_code end :: varchar(30) as PRVDR_CLSFCTN_CD_TAX,
             case when PRVDR_CLSFCTN_TYPE_CD='2' then trim(prov_classification_code) end :: varchar(30) as PRVDR_CLSFCTN_CD_SPC,
             case when PRVDR_CLSFCTN_TYPE_CD='3' then trim(prov_classification_code) end :: varchar(30) as PRVDR_CLSFCTN_CD_PTYP,
             case when PRVDR_CLSFCTN_TYPE_CD='4' then trim(prov_classification_code) end :: varchar(30) as PRVDR_CLSFCTN_CD_CSRV
	  from #Prov06_Taxonomies_TYP
	  where PRVDR_CLSFCTN_TYPE_CD is not null
      order by &srtlist;
  ) by tmsis_passthrough;

  execute(
    create table #Prov06_Taxonomies_ALL
	  diststyle key distkey(submitting_state_prov_id) as
      select B.*,
             case when B.PRVDR_CLSFCTN_TYPE_CD='1' then B.PRVDR_CLSFCTN_CD_TAX
                  when B.PRVDR_CLSFCTN_TYPE_CD='2' then SPC.label
                  when B.PRVDR_CLSFCTN_TYPE_CD='3' then TYP.label
                  when B.PRVDR_CLSFCTN_TYPE_CD='4' then SRV.label end :: varchar(30) as PRVDR_CLSFCTN_CD
	  from #Prov06_Taxonomies_CCD B 
			 left join (select * from prv_formats_sm where fmtname='CLSPRSPC') SPC on B.PRVDR_CLSFCTN_CD_SPC=SPC.start
			 left join (select * from prv_formats_sm where fmtname='CLSPRTYP') TYP on B.PRVDR_CLSFCTN_CD_PTYP=TYP.start
			 left join (select * from prv_formats_sm where fmtname='CLSSRVCD') SRV on B.PRVDR_CLSFCTN_CD_CSRV=SRV.start
      order by &srtlist;
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies_CCD);

  execute(
    create table #Prov06_Taxonomies_seg
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || SPCL) as varchar(50))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50))
			 end as PRV_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
			 PRVDR_CLSFCTN_TYPE_CD,
             PRVDR_CLSFCTN_CD
			 from #Prov06_Taxonomies_All
	  		 where PRVDR_CLSFCTN_TYPE_CD is not null and PRVDR_CLSFCTN_CD is not null
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov06);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov06);

  %DROP_temp_tables(#Prov06_Taxonomies_TYP);

** insert contents of temp table into final TAF file for segment 6 ;

  execute(
    insert into &DA_SCHEMA..TAF_PRV_TAX
    select *
	from #Prov06_Taxonomies_seg 
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies_seg);

** create the fields to merge with the root/main file ;

  execute( 
    %recode_lookup (intbl=#Prov06_Taxonomies_All,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='TAXTYP', 
                    srcvar=PRVDR_CLSFCTN_CD,
                    newvar=PRVDR_CLSFCTN_IND, 
                    outtbl=#Prov06_Taxonomies_IND,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies_ALL);

  execute( 
    %recode_lookup (intbl=#Prov06_Taxonomies_IND,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='SMECLASS', 
                    srcvar=PRVDR_CLSFCTN_CD,
                    newvar=PRVDR_CLSFCTN_SME, 
                    outtbl=#Prov06_Taxonomies_SME,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies_IND);

  execute( 
    %recode_lookup (intbl=#Prov06_Taxonomies_SME,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='MHPRVTY', 
                    srcvar=PRVDR_CLSFCTN_CD,
                    newvar=PRVDR_CLSFCTN_MHT, 
                    outtbl=#Prov06_Taxonomies_MHT,
                    fldtyp=N);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov06_Taxonomies_SME);

  execute(
    create table #Prov06_Taxonomies_CNST
                diststyle key distkey(submitting_state_prov_id)
                compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
      select &srtlist,
             PRVDR_CLSFCTN_IND,
	         PRVDR_CLSFCTN_TYPE_CD,
			 case
               when PRVDR_CLSFCTN_IND=1 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0
             end :: smallint as MLT_SNGL_SPCLTY_GRP_IND,
             case
               when PRVDR_CLSFCTN_IND=2 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as ALPTHC_OSTPTHC_PHYSN_IND,
             case
               when PRVDR_CLSFCTN_IND=3 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=4 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as CHRPRCTIC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=5 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as DNTL_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=6 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as DTRY_NTRTNL_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=7 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as EMER_MDCL_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=8 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as EYE_VSN_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=9 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as NRSNG_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=10 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as NRSNG_SRVC_RLTD_IND,
             case
               when PRVDR_CLSFCTN_IND=11 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as OTHR_INDVDL_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=12 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as PHRMCY_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=13 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=14 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as POD_MDCN_SRGRY_SRVCS_IND,
             case
               when PRVDR_CLSFCTN_IND=15 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as RESP_DEV_REH_RESTOR_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=16 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as SPCH_LANG_HEARG_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=17 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as STDNT_HLTH_CARE_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=18 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=19 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as AGNCY_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=20 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as AMB_HLTH_CARE_FAC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=21 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as HOSP_UNIT_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=22 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as HOSP_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=23 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as LAB_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=24 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as MCO_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=25 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as NRSNG_CSTDL_CARE_FAC_IND,
             case
               when PRVDR_CLSFCTN_IND=26 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as OTHR_NONINDVDL_SRVC_PRVDRS_IND,
             case
               when PRVDR_CLSFCTN_IND=27 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as RSDNTL_TRTMT_FAC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=28 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as RESP_CARE_FAC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=29 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as SUPLR_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_IND=30 then 1 
			   when PRVDR_CLSFCTN_IND is null then null
               else 0 
             end :: smallint as TRNSPRTN_SRVCS_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_TYPE_CD='1' and PRVDR_CLSFCTN_SME=1 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='2' and PRVDR_CLSFCTN_SME=4 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='4' and PRVDR_CLSFCTN_SME=7 then 1 
               when PRVDR_CLSFCTN_CD='.' or PRVDR_CLSFCTN_CD is null or PRVDR_CLSFCTN_TYPE_CD='.' or PRVDR_CLSFCTN_TYPE_CD is null then null
               when PRVDR_CLSFCTN_TYPE_CD<>'1' and PRVDR_CLSFCTN_TYPE_CD<>'2' and PRVDR_CLSFCTN_TYPE_CD<>'4' then null
              else 0 
             end :: smallint as SUD_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_TYPE_CD='1' and PRVDR_CLSFCTN_SME=2 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='2' and PRVDR_CLSFCTN_SME=5 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='3' and PRVDR_CLSFCTN_MHT=1 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='4' and PRVDR_CLSFCTN_SME=8 then 1 
               when PRVDR_CLSFCTN_CD='.' or PRVDR_CLSFCTN_CD is null or PRVDR_CLSFCTN_TYPE_CD='.' or PRVDR_CLSFCTN_TYPE_CD is null then null
               when PRVDR_CLSFCTN_TYPE_CD<>'1' and PRVDR_CLSFCTN_TYPE_CD<>'2' and  PRVDR_CLSFCTN_TYPE_CD<>'3' and PRVDR_CLSFCTN_TYPE_CD<>'4' then null
               else 0 
             end :: smallint as MH_SRVC_PRVDR_IND,
             case
               when PRVDR_CLSFCTN_TYPE_CD='1' and PRVDR_CLSFCTN_SME=3 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='2' and PRVDR_CLSFCTN_SME=6 then 1 
               when PRVDR_CLSFCTN_TYPE_CD='4' and PRVDR_CLSFCTN_SME=9 then 1 
               when PRVDR_CLSFCTN_CD='.' or PRVDR_CLSFCTN_CD is null or PRVDR_CLSFCTN_TYPE_CD='.' or PRVDR_CLSFCTN_TYPE_CD is null then null
               when PRVDR_CLSFCTN_TYPE_CD<>'1' and PRVDR_CLSFCTN_TYPE_CD<>'2' and PRVDR_CLSFCTN_TYPE_CD<>'4' then null
               else 0 
             end :: smallint as EMER_SRVCS_PRVDR_IND,
             /* grouping code */
             row_number() over (
               partition by &srtlist
               order by record_number asc
             ) as _ndx
			 from #Prov06_Taxonomies_MHT
			 where PRVDR_CLSFCTN_TYPE_CD is not null and PRVDR_CLSFCTN_CD is not null
      order by &srtlist;
  ) by tmsis_passthrough;

  execute (
    create table #Prov06_Taxonomies_Mapped
                diststyle key distkey(submitting_state_prov_id)
                compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
      select &srtlist,
             max(MLT_SNGL_SPCLTY_GRP_IND) as MLT_SNGL_SPCLTY_GRP_IND,
             max(ALPTHC_OSTPTHC_PHYSN_IND) as ALPTHC_OSTPTHC_PHYSN_IND,
             max(BHVRL_HLTH_SCL_SRVC_PRVDR_IND) as BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
             max(CHRPRCTIC_PRVDR_IND) as CHRPRCTIC_PRVDR_IND,
             max(DNTL_PRVDR_IND) as DNTL_PRVDR_IND,
             max(DTRY_NTRTNL_SRVC_PRVDR_IND) as DTRY_NTRTNL_SRVC_PRVDR_IND,
             max(EMER_MDCL_SRVC_PRVDR_IND) as EMER_MDCL_SRVC_PRVDR_IND,
             max(EYE_VSN_SRVC_PRVDR_IND) as EYE_VSN_SRVC_PRVDR_IND,
             max(NRSNG_SRVC_PRVDR_IND) as NRSNG_SRVC_PRVDR_IND,
             max(NRSNG_SRVC_RLTD_IND) as NRSNG_SRVC_RLTD_IND,
             max(OTHR_INDVDL_SRVC_PRVDR_IND) as OTHR_INDVDL_SRVC_PRVDR_IND,
             max(PHRMCY_SRVC_PRVDR_IND) as PHRMCY_SRVC_PRVDR_IND,
             max(PA_ADVCD_PRCTC_NRSNG_PRVDR_IND) as PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
             max(POD_MDCN_SRGRY_SRVCS_IND) as POD_MDCN_SRGRY_SRVCS_IND,
             max(RESP_DEV_REH_RESTOR_PRVDR_IND) as RESP_DEV_REH_RESTOR_PRVDR_IND,
             max(SPCH_LANG_HEARG_SRVC_PRVDR_IND) as SPCH_LANG_HEARG_SRVC_PRVDR_IND,
             max(STDNT_HLTH_CARE_PRVDR_IND) as STDNT_HLTH_CARE_PRVDR_IND,
             max(TT_OTHR_TCHNCL_SRVC_PRVDR_IND) as TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
             max(AGNCY_PRVDR_IND) as AGNCY_PRVDR_IND,
             max(AMB_HLTH_CARE_FAC_PRVDR_IND) as AMB_HLTH_CARE_FAC_PRVDR_IND,
             max(HOSP_UNIT_PRVDR_IND) as HOSP_UNIT_PRVDR_IND,
             max(HOSP_PRVDR_IND) as HOSP_PRVDR_IND,
             max(LAB_PRVDR_IND) as LAB_PRVDR_IND,
             max(MCO_PRVDR_IND) as MCO_PRVDR_IND,
             max(NRSNG_CSTDL_CARE_FAC_IND) as NRSNG_CSTDL_CARE_FAC_IND,
             max(OTHR_NONINDVDL_SRVC_PRVDRS_IND) as OTHR_NONINDVDL_SRVC_PRVDRS_IND,
             max(RSDNTL_TRTMT_FAC_PRVDR_IND) as RSDNTL_TRTMT_FAC_PRVDR_IND,
             max(RESP_CARE_FAC_PRVDR_IND) as RESP_CARE_FAC_PRVDR_IND,
             max(SUPLR_PRVDR_IND) as SUPLR_PRVDR_IND,
             max(TRNSPRTN_SRVCS_PRVDR_IND) as TRNSPRTN_SRVCS_PRVDR_IND,
             max(SUD_SRVC_PRVDR_IND) as SUD_SRVC_PRVDR_IND,
             max(MH_SRVC_PRVDR_IND) as MH_SRVC_PRVDR_IND,
             max(EMER_SRVCS_PRVDR_IND) as EMER_SRVCS_PRVDR_IND
			 from #Prov06_Taxonomies_CNST
      group by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_2_Prov06);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_2_Prov06);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_3_Prov06);
 
  %DROP_temp_tables(#Prov06_Taxonomies_MHT);
  %DROP_temp_tables(#Prov06_Taxonomies_CNST);

  
 ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00007 ;
  
  %process_07_medicaid(maintbl=#Prov02_Main,
                       outtbl=#Prov07_Medicaid);

** add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records;

  execute( 
    %recode_notnull (intbl=#Prov07_Medicaid,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov07_Medicaid_ST,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov07_Medicaid);
  
  execute( 
    %recode_lookup (intbl=#Prov07_Medicaid_ST,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='ENRSTCDV', 
                    srcvar=prov_medicaid_enrollment_status_code,
                    newvar=PRVDR_MDCD_ENRLMT_STUS_CD, 
                    outtbl=#Prov07_Medicaid_STS,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov07_Medicaid_ST);


  execute( 
    %recode_lookup (intbl=#Prov07_Medicaid_STS,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='ENRSTCAT', 
                    srcvar=PRVDR_MDCD_ENRLMT_STUS_CD,
                    newvar=PRVDR_MDCD_ENRLMT_STUS_CTGRY, 
                    outtbl=#Prov07_Medicaid_STC,
                    fldtyp=N);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov07_Medicaid_STS);

  execute( 
    %recode_lookup (intbl=#Prov07_Medicaid_STC,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='ENRCDV', 
                    srcvar=state_plan_enrollment,
                    newvar=STATE_PLAN_ENRLMT_CD, 
                    outtbl=#Prov07_Medicaid_ENR,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov07_Medicaid_STC);

  execute( 
    %recode_lookup (intbl=#Prov07_Medicaid_ENR,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='ENRMDCDV', 
                    srcvar=prov_enrollment_method,
                    newvar=PRVDR_MDCD_ENRLMT_MTHD_CD, 
                    outtbl=#Prov07_Medicaid_MTD,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov07_Medicaid_ENR);
   
  execute(
    create table #Prov07_Medicaid_CNST
                 diststyle key distkey(submitting_state_prov_id) as
      select &srtlist, SUBMTG_STATE_CD, SPCL,
             PRVDR_MDCD_ENRLMT_STUS_CD,
			 STATE_PLAN_ENRLMT_CD,
			 PRVDR_MDCD_ENRLMT_MTHD_CD,
			 appl_date as APLCTN_DT,
             PRVDR_MDCD_ENRLMT_STUS_CTGRY,
			 prov_medicaid_eff_date as PRVDR_MDCD_EFCTV_DT,
			 prov_medicaid_end_date as PRVDR_MDCD_END_DT,
			 case
               when STATE_PLAN_ENRLMT_CD=1 then 1 
			   when STATE_PLAN_ENRLMT_CD is null then null
               else 0
             end :: smallint as MDCD_ENRLMT_IND,
			 case
               when STATE_PLAN_ENRLMT_CD=2 then 1 
			   when STATE_PLAN_ENRLMT_CD is null then null
               else 0
             end :: smallint as CHIP_ENRLMT_IND,
			 case
               when STATE_PLAN_ENRLMT_CD=3 then 1 
			   when STATE_PLAN_ENRLMT_CD is null then null
               else 0
             end :: smallint as MDCD_CHIP_ENRLMT_IND,
			 case
               when STATE_PLAN_ENRLMT_CD=4 then 1 
			   when STATE_PLAN_ENRLMT_CD is null then null
               else 0
             end :: smallint as NOT_SP_AFLTD_IND,
			 case
               when PRVDR_MDCD_ENRLMT_STUS_CTGRY=1 then 1 
			   when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
               else 0
             end :: smallint as PRVDR_ENRLMT_STUS_ACTV_IND,
			 case
               when PRVDR_MDCD_ENRLMT_STUS_CTGRY=2 then 1 
			   when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
               else 0
             end :: smallint as PRVDR_ENRLMT_STUS_DND_IND,
			 case
               when PRVDR_MDCD_ENRLMT_STUS_CTGRY=3 then 1 
			   when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
               else 0
             end :: smallint as PRVDR_ENRLMT_STUS_PENDG_IND,
			 case
               when PRVDR_MDCD_ENRLMT_STUS_CTGRY=4 then 1 
			   when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
               else 0
             end :: smallint as PRVDR_ENRLMT_STUS_TRMNTD_IND,
             /* grouping code */
             row_number() over (
               partition by &srtlist
               order by record_number asc
             ) as _ndx
      from #Prov07_Medicaid_MTD
	  where PRVDR_MDCD_ENRLMT_STUS_CD is not null
      order by &srtlist;
  ) by tmsis_passthrough;

  ** create the separate linked child table;

  execute(
    create table #Prov07_Medicaid_ENRPOP
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || SPCL) as varchar(50))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50))
			 end as PRV_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
			 PRVDR_MDCD_EFCTV_DT,
			 PRVDR_MDCD_END_DT,
             PRVDR_MDCD_ENRLMT_STUS_CD,
			 STATE_PLAN_ENRLMT_CD,
			 PRVDR_MDCD_ENRLMT_MTHD_CD,
			 APLCTN_DT,
             PRVDR_MDCD_ENRLMT_STUS_CTGRY
			 from #Prov07_Medicaid_CNST 
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, segment_Prov07);

** insert contents of temp table into final TAF file for segment 7 ;

  execute(
    insert into &DA_SCHEMA..TAF_PRV_ENR
    select *
	from #Prov07_Medicaid_ENRPOP 
  ) by tmsis_passthrough;
 
  %DROP_temp_tables(#Prov07_Medicaid_ENRPOP);

  ** final step in creating constructed variables;
  execute (
    create table #Prov07_Medicaid_Mapped
                 diststyle key distkey(submitting_state_prov_id) as
      select &srtlist,
	  		 1 as PRVDR_MDCD_ENRLMT_IND,
			 max(MDCD_ENRLMT_IND) as MDCD_ENRLMT_IND,
			 max(CHIP_ENRLMT_IND) as CHIP_ENRLMT_IND,
			 max(MDCD_CHIP_ENRLMT_IND) as MDCD_CHIP_ENRLMT_IND,
			 max(NOT_SP_AFLTD_IND) as NOT_SP_AFLTD_IND,
			 max(PRVDR_ENRLMT_STUS_ACTV_IND) as PRVDR_ENRLMT_STUS_ACTV_IND,
			 max(PRVDR_ENRLMT_STUS_DND_IND) as PRVDR_ENRLMT_STUS_DND_IND,
			 max(PRVDR_ENRLMT_STUS_TRMNTD_IND) as PRVDR_ENRLMT_STUS_TRMNTD_IND,
			 max(PRVDR_ENRLMT_STUS_PENDG_IND) as PRVDR_ENRLMT_STUS_PENDG_IND
      from #Prov07_Medicaid_CNST
      group by &srtlist;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov07);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_1_Prov07);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_2_Prov07);

  %DROP_temp_tables(#Prov07_Medicaid_CNST);
  %DROP_temp_tables(#Prov07_Medicaid_MTD);

  execute(
    create table #Prov02_Base
         diststyle key distkey(submtg_state_prvdr_id) as
         select M.DA_RUN_ID,
		        M.prv_link_key,
                M.PRV_FIL_DT,
                M.PRV_VRSN ,
                M.tmsis_run_id,
                M.submtg_state_cd,
                M.submtg_state_prvdr_id,
                M.REG_FLAG,
                M.prvdr_dba_name,
                M.prvdr_lgl_name,
                M.prvdr_org_name,
                M.prvdr_tax_name,
                M.fac_grp_indvdl_cd,
                M.tchng_ind,
                M.prvdr_1st_name,
                M.prvdr_mdl_initl_name,
                M.prvdr_last_name,
                M.gndr_cd,
                M.ownrshp_cd,
                M.ownrshp_cat,
                M.prvdr_prft_stus_cd,
                M.birth_dt,
                M.death_dt,
                M.acpt_new_ptnts_ind,
                M.age_num,
				coalesce(E.PRVDR_MDCD_ENRLMT_IND, 0) :: smallint as PRVDR_MDCD_ENRLMT_IND,
				case
				  when E.MDCD_CHIP_ENRLMT_IND=1 then 0 
				  else E.MDCD_ENRLMT_IND
				end :: smallint as MDCD_ENRLMT_IND,
				case
				  when E.MDCD_CHIP_ENRLMT_IND=1 then 0 
				  else E.CHIP_ENRLMT_IND
				end :: smallint as CHIP_ENRLMT_IND,
                E.MDCD_CHIP_ENRLMT_IND,
                E.NOT_SP_AFLTD_IND,
                E.PRVDR_ENRLMT_STUS_ACTV_IND,
                E.PRVDR_ENRLMT_STUS_DND_IND,
                E.PRVDR_ENRLMT_STUS_TRMNTD_IND,
                E.PRVDR_ENRLMT_STUS_PENDG_IND,				
             	T.MLT_SNGL_SPCLTY_GRP_IND,
             	T.ALPTHC_OSTPTHC_PHYSN_IND,
             	T.BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
             	T.CHRPRCTIC_PRVDR_IND,
             	T.DNTL_PRVDR_IND,
             	T.DTRY_NTRTNL_SRVC_PRVDR_IND,
             	T.emer_MDCL_srvc_prvdr_ind,
             	T.eye_VSN_srvc_prvdr_ind,
             	T.nrsng_srvc_prvdr_ind,
             	T.nrsng_srvc_rltd_ind,
             	T.othr_INDVDL_srvc_prvdr_ind,
             	T.PHRMCY_SRVC_PRVDR_IND,
             	T.PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
             	T.POD_MDCN_SRGRY_SRVCS_IND,
             	T.resp_dev_reh_restor_prvdr_ind,
             	T.SPCH_LANG_HEARG_SRVC_PRVDR_IND,
             	T.STDNT_HLTH_CARE_PRVDR_IND,
             	T.TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
             	T.agncy_prvdr_ind,
             	T.amb_hlth_CARE_fac_prvdr_ind,
             	T.hosp_unit_prvdr_ind,
             	T.hosp_prvdr_ind,
             	T.lab_prvdr_ind,
             	T.mco_prvdr_ind,
             	T.NRSNG_CSTDL_CARE_FAC_IND,
             	T.OTHR_NONINDVDL_SRVC_PRVDRS_IND,
             	T.RSDNTL_TRTMT_FAC_PRVDR_IND,
             	T.RESP_CARE_FAC_PRVDR_IND,
             	T.SUPLR_PRVDR_IND,
             	T.TRNSPRTN_SRVCS_PRVDR_IND,
             	T.sud_srvc_prvdr_ind,
             	T.mh_srvc_prvdr_ind,
             	T.emer_srvcs_prvdr_ind
         from #Prov02_Main_CNST M 
           left join #Prov07_Medicaid_Mapped E
             on %write_equalkeys(keyvars=srtlist, t1=M, t2=E)
           left join #Prov06_Taxonomies_Mapped T
             on %write_equalkeys(keyvars=srtlist, t1=M, t2=T)
         order by M.tmsis_run_id, M.submtg_state_cd, M.submtg_state_prvdr_id;
  ) by tmsis_passthrough;

  
  %DROP_temp_tables(#Prov02_Main_CNST);
  %DROP_temp_tables(#Prov07_Medicaid_Mapped);
  %DROP_temp_tables(#Prov06_Taxonomies_Mapped);

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, base_Prov);

  * insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV
    select *
	from #Prov02_Base 
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov02_Base);
 
 
 ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00008 ;
 
  %process_08_groups(maintbl=#Prov02_Main,
                     outtbl=#Prov08_Groups);

** add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records;

  execute( 
    %recode_notnull (intbl=#Prov08_Groups,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov08_Groups_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov08_Groups);

  execute(
    create table #Prov08_Groups_CNST
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || SPCL) as varchar(50))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50))
			 end as PRV_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
             submitting_state_prov_id_of_affiliated_entity as SUBMTG_STATE_AFLTD_PRVDR_ID
			 from #Prov08_Groups_STV 
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov08);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov08);

  %DROP_temp_tables(#Prov08_Groups_STV);

* insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV_GRP
    select *
	from #Prov08_Groups_CNST
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov08_Groups_CNST);

  
 ** Applies subsetting criteria and validate data or create constructed variables for segment PRV00009 ;
  
  %process_09_affpgms(maintbl=#Prov02_Main,
                      outtbl=#Prov09_AffPgms);

** add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records;

  execute( 
    %recode_notnull (intbl=#Prov09_AffPgms,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='STFIPC', 
                    srcvar=submitting_state,
                    newvar=SUBMTG_STATE_CD, 
                    outtbl=#Prov09_AffPgms_STV,
                    fldtyp=C,
					fldlen=2);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov09_AffPgms);

  execute( 
    %recode_lookup (intbl=#Prov09_AffPgms_STV,
                    srtvars=srtlist,
                    fmttbl=prv_formats_sm,
                    fmtnm='PRGCDV', 
                    srcvar=affiliated_program_type,
                    newvar=AFLTD_PGM_TYPE_CD, 
                    outtbl=#Prov09_AffPgms_TYP1,
                    fldtyp=C,
					fldlen=1);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov09_AffPgms_STV);

  ** remove duplicate records;
  %let grplistp = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 AFLTD_PGM_TYPE_CD,
                 affiliated_program_id;

  execute(
    alter table #Prov09_AffPgms_TYP1 drop column _wanted;

    %remove_duprecs (intbl=#Prov09_AffPgms_TYP1, 
                     grpvars=grplistp, 
                     dtvar_beg=prov_affiliated_program_eff_date,
                     dtvar_end=prov_affiliated_program_end_date,
                     ordvar=affiliated_program_id,
                     outtbl=#Prov09_AffPgms_TYP);
  ) by tmsis_passthrough;

  %DROP_temp_tables(#Prov09_AffPgms_TYP1);

  execute(
    create table #Prov09_AffPgms_CNST
	  diststyle key distkey(SUBMTG_STATE_PRVDR_ID) as
      select &DA_RUN_ID :: integer as DA_RUN_ID,
			 case
			   when SPCL is not null then
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || SPCL) as varchar(50))
			   else
			   cast ((%nrbquote('&VERSION.') || '-' || &monyrout. || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50))
			 end as PRV_LINK_KEY,
             &TAF_FILE_DATE :: varchar(10) as PRV_FIL_DT,
	         %nrbquote('&VERSION.') :: varchar(2) as PRV_VRSN,
	         tms_run_id as TMSIS_RUN_ID,
	         SUBMTG_STATE_CD,
			 submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
			 AFLTD_PGM_TYPE_CD,
             affiliated_program_id as AFLTD_PGM_ID
			 from #Prov09_AffPgms_TYP
      order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID;
  ) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, recodes_Prov09);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 101_prvdr_build.sas, constructed_Prov09);

  %DROP_temp_tables(#Prov09_AffPgms_TYP);

* insert contents of temp table into final TAF file;
  execute(
    insert into &DA_SCHEMA..TAF_PRV_PGM
    select *
	from #Prov09_AffPgms_CNST	
  ) by tmsis_passthrough;
  %DROP_temp_tables(#Prov09_AffPgms_CNST);


 	sysecho 'in jobcntl updt2';
	%JOB_CONTROL_UPDT2(&DA_RUN_ID., &DA_SCHEMA.);
	%let TABLE_NAME = TAF_PRV;
	%let FIL_4TH_NODE = PBS;
	sysecho 'in get cnt';
    title3 "Count: TAF PRV Base"; 
	%GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, base_Prov, #Prov02_Base, submtg_state_cd);   
    sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);

    %let TABLE_NAME = TAF_PRV_LOC;
	%let FIL_4TH_NODE = PLO;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_LOC"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_3_Prov03, #Prov03_Location_CNST, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.); 

    %let TABLE_NAME = TAF_PRV_LIC;
	%let FIL_4TH_NODE = PLI;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_LIC"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_Prov04, #Prov04_Licensing_CNST, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);
	
    %let TABLE_NAME = TAF_PRV_IDT;
	%let FIL_4TH_NODE = PID;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_IDT"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_Prov05, #Prov05_Identifiers_CNST, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);

    %let TABLE_NAME = TAF_PRV_TAX;
	%let FIL_4TH_NODE = PTX;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_TAX"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_Prov06, #Prov06_Taxonomies_seg, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);

    %let TABLE_NAME = TAF_PRV_ENR;
	%let FIL_4TH_NODE = PEN;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_ENR"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, segment_Prov07, #Prov07_Medicaid_ENRPOP, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);
	
    %let TABLE_NAME = TAF_PRV_GRP;
	%let FIL_4TH_NODE = PAG;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_GRP"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_Prov08, #Prov08_Groups_CNST, submtg_state_cd);   
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);

    %let TABLE_NAME = TAF_PRV_PGM;
	%let FIL_4TH_NODE = PAP;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_PGM";
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo';
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_Prov09, #Prov09_AffPgms_CNST, submtg_state_cd);
	sysecho 'In file contents Header';
    %FILE_CONTENTS(&DA_SCHEMA., &TABLE_NAME.);

    %let TABLE_NAME = TAF_PRV_BED;
	%let FIL_4TH_NODE = PBT;
    sysecho 'in get cnt';
    title3 "Count: TAF PRV_PGM"; 
    %GET_CNT(&TABLE_NAME., &DA_SCHEMA.);
	sysecho 'create metainfo'; 
 	%CREATE_META_INFO(&DA_SCHEMA., &TABLE_NAME.,&DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);  
	%CREATE_EFTSMETA_INFO(&DA_SCHEMA., &DA_RUN_ID., &TABLE_NAME., 101_prvdr_build.sas, constructed_Prov10, #Prov10_BedType_CNST, submtg_state_cd);   
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

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV HEADER TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_LOC;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_LOC LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_LIC;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_LIC LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_IDT;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_IDT LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_TAX;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_TAX LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_ENR;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_ENR LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_GRP;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_GRP LOCATION TABLE";
RUN;

PROC CONTENTS DATA=FILE_CONTENTS_TAF_PRV_PGM;
TITLE "FINAL CONTENT LISTING OF THE TAF PRV_PGM LOCATION TABLE";
RUN;

proc printto; run;

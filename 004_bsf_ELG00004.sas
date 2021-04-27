/**********************************************************************************************/
/*Program: 004_bsf_ELG00004.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_ELGBL_CNTCT and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_ELG00004(tab_no, _2x_segment, eff_date, end_date);
%let loc=home; 
%let created_vars =  trim(ELGBL_STATE_CD)||trim(ELGBL_CNTY_CD) as ENROLLEES_COUNTY_CD_HOME,
 		   elgbl_line_1_adr as elgbl_line_1_adr_&loc,
		   elgbl_line_2_adr as elgbl_line_2_adr_&loc,
		   elgbl_line_3_adr as elgbl_line_3_adr_&loc,
		   elgbl_city_name as elgbl_city_name_&loc,
		   elgbl_zip_cd as elgbl_zip_cd_&loc,	
		   elgbl_cnty_cd as elgbl_cnty_cd_&loc,
		   lpad(elgbl_state_cd,2,'0') as elgbl_state_cd_&loc,
		   elgbl_phne_num as elgbl_phne_num_&loc;

execute(

     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._recCt 
     distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num,recCt) as
	 select 
        submtg_state_cd, 
        msis_ident_num,
		count(msis_ident_num) as recCt
		from &tab_no
        where ELGBL_ADR_TYPE_CD in ('01','1')
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select recCt,count(msis_ident_num) as beneficiaries from &tab_no._recCt group by recCt ) order by recCt;


execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no._uniq 
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num) as
	 select t1.*,
	       &created_vars,
			1 as KEEP_FLAG

		from &tab_no t1
        inner join &tab_no._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1

		where ELGBL_ADR_TYPE_CD in ('01','1')

		) by tmsis_passthrough;

title "Number of beneficiary with unique records in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._uniq );

%MultiIds(sort_key=%str(coalesce(trim(elgbl_line_1_adr),'xx') || coalesce(trim(elgbl_city_name),'xx') || coalesce(trim(elgbl_cnty_cd),'xx') || 
                        coalesce(trim(elgbl_phne_num),'xx') || coalesce(trim(elgbl_state_cd),'xx') || coalesce(trim(elgbl_zip_cd),'xx')),
          where=%str(ELGBL_ADR_TYPE_CD in ('01','1')));


title "Number of beneficiares who were processed for duplicates in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._multi );

%let loc = mail; 
%let created_vars =  trim(ELGBL_STATE_CD)||trim(ELGBL_CNTY_CD) as ENROLLEES_COUNTY_CD_HOME,
 		   elgbl_line_1_adr as elgbl_line_1_adr_&loc,
		   elgbl_line_2_adr as elgbl_line_2_adr_&loc,
		   elgbl_line_3_adr as elgbl_line_3_adr_&loc,
		   elgbl_city_name as elgbl_city_name_&loc,
		   elgbl_zip_cd as elgbl_zip_cd_&loc,	
		   elgbl_cnty_cd as elgbl_cnty_cd_&loc,
		   elgbl_state_cd as elgbl_state_cd_&loc,
		   elgbl_phne_num as elgbl_phne_num_&loc;

execute(

     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no.A_recCt 
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        submtg_state_cd, 
        msis_ident_num,
		count(msis_ident_num) as recCt
		from &tab_no
        where ELGBL_ADR_TYPE_CD in ('06','6')
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records per beneficiary in &tab_no.A";
select * from connection to tmsis_passthrough
 ( select recCt,count(msis_ident_num) as beneficiaries from &tab_no.A_recCt group by recCt ) order by recCt;

execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no.A_uniq 
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select t1.*,
	        &created_vars,
			1 as KEEP_FLAG
		from &tab_no t1
        inner join &tab_no.A_recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1

		where ELGBL_ADR_TYPE_CD in ('06','6')

		) by tmsis_passthrough;
title "Number of beneficiary with unique records in &tab_no.A";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no.A_uniq );

%MultiIds(sort_key=%str(coalesce(trim(elgbl_line_1_adr),'xx') || coalesce(trim(elgbl_city_name),'xx') || coalesce(trim(elgbl_cnty_cd),'xx') || 
                        coalesce(trim(elgbl_phne_num),'xx') || coalesce(trim(elgbl_state_cd),'xx') || coalesce(trim(elgbl_zip_cd),'xx')),
          where=%str(ELGBL_ADR_TYPE_CD in ('06','6')),suffix=A);

	 /* Union together tables for a permanent table */
 execute(
     create temp table &tab_no._uniq_step1
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select * from &tab_no._uniq
	 union all
	 select * from &tab_no._multi
	)  by tmsis_passthrough;

execute(
     create temp table &tab_no._uniq_step2
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
     select * from &tab_no.A_uniq
	 union all
	 select * from &tab_no.A_multi)  by tmsis_passthrough;

execute(
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
     select coalesce(t1.msis_ident_num,t2.msis_ident_num) as msis_ident_num,
	        coalesce(t1.submtg_state_cd,t2.submtg_state_cd) as submtg_state_cd,
			t1.ENROLLEES_COUNTY_CD_HOME,
			coalesce(t1.elgbl_adr_efctv_dt,t2.elgbl_adr_efctv_dt) as ELGBL_ADR_EFCTV_DT,
		    coalesce(t1.elgbl_adr_end_dt,t2.elgbl_adr_end_dt) as ELGBL_ADR_END_DT,
			coalesce(t1.elgbl_adr_type_cd,t2.elgbl_adr_type_cd) as ELGBL_ADR_TYPE_CD,


		   t1.elgbl_line_1_adr_home,
		   t1.elgbl_line_2_adr_home,
		   t1.elgbl_line_3_adr_home,
		   t1.elgbl_city_name_home,
		   t1.elgbl_zip_cd_home,	
		   t1.elgbl_cnty_cd_home,
		   t1.elgbl_state_cd_home,
		   t1.elgbl_phne_num_home,

		   t2.elgbl_line_1_adr_mail,
		   t2.elgbl_line_2_adr_mail,
		   t2.elgbl_line_3_adr_mail,
		   t2.elgbl_city_name_mail,
		   t2.elgbl_zip_cd_mail,	
		   t2.elgbl_cnty_cd_mail,
		   t2.elgbl_state_cd_mail,
		   t2.elgbl_phne_num_mail 

          from &tab_no._uniq_step1 t1
          full join &tab_no._uniq_step2 t2
          on t1.msis_ident_num=t2.msis_ident_num
          and t1.submtg_state_cd=t2.submtg_state_cd)  by tmsis_passthrough;

title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 004_bsf_ELG00004, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 004_bsf_ELG00004, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._recCt &tab_no._uniq &tab_no.A_recCt &tab_no._multi &tab_no.A_multi &tab_no._multi_all &tab_no._multi_step2);

%mend create_ELG00004;



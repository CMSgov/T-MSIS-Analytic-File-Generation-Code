/**********************************************************************************************/
/*Program: 022_bsf_TPL00002.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_TPL_MDCD_PRSN_MN and create unique output for BSF.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/

 
%macro create_TPL00002(tab_no, _2x_segment, eff_date, end_date);
%let created_vars=;
execute (
     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._recCt 
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num,recCt) as
	 select 
        submtg_state_cd, 
        msis_ident_num,
		count(TMSIS_RUN_ID) as recCt
		from &tab_no
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

		 1 as KEEP_FLAG

		from &tab_no t1
        inner join &tab_no._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1
		) by tmsis_passthrough;

title "Number of beneficiary with unique records in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._uniq );

 %MultiIds(sort_key=%str(coalesce(trim(tpl_insrnc_cvrg_ind),'xx') || coalesce(trim(tpl_othr_cvrg_ind),'xx')))

 execute(

     /* Union together tables for a permanent table */
     create temp table &tab_no._&BSF_FILE_DATE._uniq 
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select *

	 from (
	 select * from &tab_no._uniq
	 union all
	 select * from &tab_no._multi) 

    )  by tmsis_passthrough;

/* extract to SAS or load to permanent TMSIS table */
title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 022_bsf_TPL00022, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 022_bsf_TPL00022, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._recCt &tab_no._uniq &tab_no._multi &tab_no._multi_step2 &tab_no._multi_all);

%mend create_TPL00002;



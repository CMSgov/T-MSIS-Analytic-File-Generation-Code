/**********************************************************************************************/
/*Program: 008_bsf_ELG00008.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_HH_CHRNC_COND and create unique output for BSF.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
 
%macro create_ELG00008(tab_no, _2x_segment, eff_date, end_date);

execute (
     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        submtg_state_cd, 
        msis_ident_num,
        max(case when trim(HH_CHRNC_CD) in('A','B','C','D','E','F','G','H') then 1 else 0 end) as ANY_VALID_HH_CC,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='A' then 1 else 0 end) as MH_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='B' then 1 else 0 end) as SA_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='C' then 1 else 0 end) as ASTHMA_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='D' then 1 else 0 end) as DIABETES_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='E' then 1 else 0 end) as HEART_DIS_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='F' then 1 else 0 end) as OVERWEIGHT_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='G' then 1 else 0 end) as HIV_AIDS_HH_CHRONIC_COND_FLG,
		max(case when nullif(trim(HH_CHRNC_CD),'') is null then null when trim(HH_CHRNC_CD)='H' then 1 else 0 end) as OTHER_HH_CHRONIC_COND_FLG
		

		from &tab_no
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;


/* extract to SAS or load to permanent TMSIS table */
title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 008_bsf_ELG00008, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 008_bsf_ELG00008, 0.2. MultiIds);

%drop_table_multi(&tab_no.);

%mend create_ELG00008;

/**********************************************************************************************/
/*Program: 020_bsf_ELG00020.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_HCBS_CHRNC_COND_NON_HH and create unique output for BSF.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
 
%macro create_ELG00020(tab_no, _2x_segment, eff_date, end_date);

execute (
     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        submtg_state_cd, 
        msis_ident_num, 

        max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='001' then 1 else 0 end) as HCBS_AGED_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='002' then 1 else 0 end) as HCBS_PHYS_DISAB_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='003' then 1 else 0 end) as HCBS_INTEL_DISAB_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='004' then 1 else 0 end) as HCBS_AUTISM_SP_DIS_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='005' then 1 else 0 end) as HCBS_DD_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='006' then 1 else 0 end) as HCBS_MI_SED_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='007' then 1 else 0 end) as HCBS_BRAIN_INJ_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='008' then 1 else 0 end) as HCBS_HIV_AIDS_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='009' then 1 else 0 end) as HCBS_TECH_DEP_MF_NON_HHCC_FLG,
		max(case when nullif(NDC_UOM_CHRNC_NON_HH_CD,'') is null then null when NDC_UOM_CHRNC_NON_HH_CD ='010' then 1 else 0 end) as HCBS_DISAB_OTHER_NON_HHCC_FLG

		from &tab_no
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 020_bsf_ELG00020, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 020_bsf_ELG00020, 0.2. MultiIds);


%drop_table_multi(&tab_no.);

%mend create_ELG00020;


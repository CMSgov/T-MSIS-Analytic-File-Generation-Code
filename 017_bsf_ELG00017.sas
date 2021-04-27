/**********************************************************************************************/
/*Program: 017_bsf_ELG00017.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_DSBLTY_INFO and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_ELG00017(tab_no, _2x_segment, eff_date, end_date);

execute (
     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        submtg_state_cd, 
        msis_ident_num, 

	    max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='01' then 1 else 0 end) as DEAF_DISAB_FLG,
		max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='02' then 1 else 0 end) as BLIND_DISAB_FLG,
		max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='03' then 1 else 0 end) as DIFF_CONC_DISAB_FLG,
		max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='04' then 1 else 0 end) as DIFF_WALKING_DISAB_FLG,
		max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='05' then 1 else 0 end) as DIFF_DRESSING_BATHING_DISAB_FLG,
		max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='06' then 1 else 0 end) as DIFF_ERRANDS_ALONE_DISAB_FLG,
		max(case when nullif(trim(DSBLTY_TYPE_CD),'') is null then null when trim(DSBLTY_TYPE_CD) ='07' then 1 else 0 end) as OTHER_DISAB_FLG

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

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 017_bsf_ELG00017, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 017_bsf_ELG00017, 0.2. MultiIds);

%drop_table_multi(&tab_no.);

%mend create_ELG00017;

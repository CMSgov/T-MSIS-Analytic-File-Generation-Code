/**********************************************************************************************/
/*Program: 016_bsf_ELG00016.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_RACE_INFO and create unique output for BSF.
/*Mod:   12/06/2021: New valid value Other(018) for RACE 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/

%macro create_ELG00016(tab_no, _2x_segment, eff_date, end_date);
execute (
     create temp table &tab_no._step1
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num,keeper) as
	 select 
        submtg_state_cd, 
        msis_ident_num, 
		CRTFD_AMRCN_INDN_ALSKN_NTV_IND,

		row_number() over (partition by submtg_state_cd,
		                        msis_ident_num
                       order by submtg_state_cd,
		                        msis_ident_num,
                                TMSIS_RPTG_PRD desc,
								&eff_date desc,
								&end_date desc,
								REC_NUM desc,
                                CRTFD_AMRCN_INDN_ALSKN_NTV_IND) as keeper
		from &tab_no
        where CRTFD_AMRCN_INDN_ALSKN_NTV_IND is not null
		) by tmsis_passthrough;

execute (
	create temp table &tab_no._step2
		 distkey(msis_ident_num) 
	     sortkey(submtg_state_cd,msis_ident_num) as
		 select *
			from &tab_no._step1
	        where keeper=1		
) by tmsis_passthrough;

execute (
     create temp table &tab_no._step3
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        submtg_state_cd, 
        msis_ident_num, 

	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='012' then 1 else 0 end) as NATIVE_HI_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='013' then 1 else 0 end) as GUAM_CHAMORRO_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='014' then 1 else 0 end) as SAMOAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='015' then 1 else 0 end) as OTHER_PAC_ISLANDER_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='016' then 1 else 0 end) as UNK_PAC_ISLANDER_FLG ,
 	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='004' then 1 else 0 end) as ASIAN_INDIAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='005' then 1 else 0 end) as CHINESE_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='006' then 1 else 0 end) as FILIPINO_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='007' then 1 else 0 end) as JAPANESE_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='008' then 1 else 0 end) as KOREAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='009' then 1 else 0 end) as VIETNAMESE_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='010' then 1 else 0 end) as OTHER_ASIAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='011' then 1 else 0 end) as UNKNOWN_ASIAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='001' then 1 else 0 end) as WHITE_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='002' then 1 else 0 end) as BLACK_AFRICAN_AMERICAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='003' then 1 else 0 end) as AIAN_FLG ,
	   max(case when nullif(trim(RACE_CD),'') is null then null when trim(RACE_CD) ='018' then 1 else 0 end) as OTHER_OTHER_FLG
	
		from &tab_no
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

execute (
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        t1.*,
		t2.CRTFD_AMRCN_INDN_ALSKN_NTV_IND,

		case when 
        (coalesce(ASIAN_INDIAN_FLG,0)+ coalesce(CHINESE_FLG,0)+
        coalesce(FILIPINO_FLG,0) + coalesce(JAPANESE_FLG,0)+
        coalesce(KOREAN_FLG,0)+ coalesce(VIETNAMESE_FLG,0)+
        coalesce(OTHER_ASIAN_FLG,0)+ coalesce(UNKNOWN_ASIAN_FLG,0))>=1 then 1 else 0 end as GLOBAL_ASIAN,

		case when 
        (coalesce(ASIAN_INDIAN_FLG,0)+ coalesce(CHINESE_FLG,0)+
        coalesce(FILIPINO_FLG,0) + coalesce(JAPANESE_FLG,0)+
        coalesce(KOREAN_FLG,0)+ coalesce(VIETNAMESE_FLG,0)+
        coalesce(OTHER_ASIAN_FLG,0)+ coalesce(UNKNOWN_ASIAN_FLG,0))>1 then 1 else 0 end as MULTI_ASIAN,

        case when
        (coalesce(NATIVE_HI_FLG,0)+ coalesce(GUAM_CHAMORRO_FLG,0)+ 
        coalesce(SAMOAN_FLG,0)+ coalesce(OTHER_PAC_ISLANDER_FLG,0)+
        coalesce(UNK_PAC_ISLANDER_FLG,0))>=1 then 1 else 0 end as GLOBAL_ISLANDER,

        case when
        (coalesce(NATIVE_HI_FLG,0)+ coalesce(GUAM_CHAMORRO_FLG,0)+ 
        coalesce(SAMOAN_FLG,0)+ coalesce(OTHER_PAC_ISLANDER_FLG,0)+
        coalesce(UNK_PAC_ISLANDER_FLG,0))>1 then 1 else 0 end as MULTI_ISLANDER

		from &tab_no._step3 t1
		left join &tab_no._step2 t2
		on t1.submtg_state_cd=t2.submtg_state_cd
		and t1.msis_ident_num=t2.msis_ident_num

		) by tmsis_passthrough;

/* extract to SAS or load to permanent TMSIS table */
title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 016_bsf_ELG00016, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 016_bsf_ELG00016, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._step1 &tab_no._step2 &tab_no._step3);

%mend create_ELG00016;


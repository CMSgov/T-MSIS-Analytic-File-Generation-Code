/**********************************************************************************************/
/*Program: 023_bsf_final.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Combine tables from 002 through 023 to produce the final BSF file.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
 
%macro hh_chk(col);
case when t6.HH_PROGRAM_PARTICIPANT_FLG <> 1 then null else &col end
%mend hh_chk;

%MACRO BSF_Final;
	%macro tbl_joiner(tbl,num);
	  left join &tbl t&num on t1.SUBMTG_STATE_CD=t&num..SUBMTG_STATE_CD
	                      and t1.MSIS_IDENT_NUM=t&num..MSIS_IDENT_NUM
	%mend tbl_joiner;

 execute(

     /* Union together tables for a permanent table */
     create temp table BSF_STEP1
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num) as
	 select t1.*,
	        %ELG00002(2), t2.AGE, t2.DECEASED_FLG as DECEASED_FLAG,
                          t2.AGE_GROUP_FLG as AGE_GROUP_FLAG, t2.GNDR_CODE,

			%ELG00003(3), t3.PRMRY_LANG_CODE, 
                          t3.PRMRY_LANG_FLG as PRMRY_LANG_FLAG, 
                          t3.PREGNANCY_FLG as PREGNANCY_FLAG,

			%ELG00004(4), 

			%ELG00005(5), t5.CARE_LVL_STUS_CODE, t5.DUAL_ELIGIBLE_FLG as DUAL_ELIGIBLE_FLAG,
                          t5.ELIGIBILITY_GROUP_CATEGORY_FLG as ELIGIBILITY_GROUP_CATEGORY_FLAG,
						  t5.MASBOE,

			%ELG00006(6), 
  
			case 
                /* If HH flag = 2 it means they had data but both names were always null/space filled */
			    /* If ANY_VALID_HH_CC = 0 it means they had data but never a valid HH code */
                when t6.HH_PROGRAM_PARTICIPANT_FLG = 2 and 
			         t8.ANY_VALID_HH_CC = 0 then null

				when coalesce(t6.HH_PROGRAM_PARTICIPANT_FLG,0)=1 or
				     coalesce(t8.ANY_VALID_HH_CC,0)=1 then 1
				else 0 end as HH_PROGRAM_PARTICIPANT_FLAG,

			%ELG00007(7),

			/*%ELG00008(8),*/ 
			t8.MH_HH_CHRONIC_COND_FLG as MH_HH_CHRONIC_COND_FLAG,
		    t8.SA_HH_CHRONIC_COND_FLG as SA_HH_CHRONIC_COND_FLAG,
		    t8.ASTHMA_HH_CHRONIC_COND_FLG as ASTHMA_HH_CHRONIC_COND_FLAG,
		    t8.DIABETES_HH_CHRONIC_COND_FLG as DIABETES_HH_CHRONIC_COND_FLAG,
		    t8.HEART_DIS_HH_CHRONIC_COND_FLG as HEART_DIS_HH_CHRONIC_COND_FLAG,
		    t8.OVERWEIGHT_HH_CHRONIC_COND_FLG as OVERWEIGHT_HH_CHRONIC_COND_FLAG,
		    t8.HIV_AIDS_HH_CHRONIC_COND_FLG as HIV_AIDS_HH_CHRONIC_COND_FLAG,
		    t8.OTHER_HH_CHRONIC_COND_FLG as OTHER_HH_CHRONIC_COND_FLAG,

			/*%ELG00009(9),*/ t9.LCKIN_PRVDR_NUM1, t9.LCKIN_PRVDR_NUM2, t9.LCKIN_PRVDR_NUM3,
			              t9.LCKIN_PRVDR_TYPE_CD1, t9.LCKIN_PRVDR_TYPE_CD2, t9.LCKIN_PRVDR_TYPE_CD3,
						  /* If lockin value is null it means no segment, set to 0 */
						  /* If lockin value is 2 it means all provider numbers were null on segment -- set to null */
						 nullif(coalesce(t9.LOCK_IN_FLG,0),2) as LOCK_IN_FLAG,

			%ELG00010(10), t10.mfp_prtcptn_endd_rsn_code, t10.mfp_qlfyd_instn_code,
			               t10.mfp_qlfyd_rsdnc_code, t10.mfp_rinstlzd_rsn_code,
						   coalesce(t10.MFP_PARTICIPANT_FLG,0) as MFP_PARTICIPANT_FLAG,

			/*%ELG00011(11),*/
                           t11.COMMUNITY_FIRST_CHOICE_SPO_FLG as COMMUNITY_FIRST_CHOICE_SPO_FLAG,
                           t11._1915I_SPO_FLG as _1915I_SPO_FLAG,
                           t11._1915J_SPO_FLG as _1915J_SPO_FLAG,
				 		   t11._1915A_SPO_FLG as _1915A_SPO_FLAG,
                           t11._1932A_SPO_FLG as _1932A_SPO_FLAG,
                           t11._1937_ABP_SPO_FLG as _1937_ABP_SPO_FLAG,


			/*%ELG00012(12),*/	t12.WVR_ID1, t12.WVR_ID2, t12.WVR_ID3, t12.WVR_ID4, t12.WVR_ID5, t12.WVR_ID6,
			                    t12.WVR_ID7, t12.WVR_ID8, t12.WVR_ID9, t12.WVR_ID10,
                                t12.WVR_TYPE_CD1, t12.WVR_TYPE_CD2, t12.WVR_TYPE_CD3, t12.WVR_TYPE_CD4, t12.WVR_TYPE_CD5,
								t12.WVR_TYPE_CD6, t12.WVR_TYPE_CD7, t12.WVR_TYPE_CD8, t12.WVR_TYPE_CD9, t12.WVR_TYPE_CD10,


			/*%ELG00013(13),*/ t13.LTSS_PRVDR_NUM1, t13.LTSS_PRVDR_NUM2, t13.LTSS_PRVDR_NUM3,
			               t13.LTSS_LVL_CARE_CD1, t13.LTSS_LVL_CARE_CD2, t13.LTSS_LVL_CARE_CD3,

			/*%ELG00014(14),*/ t14.MC_PLAN_ID1, t14.MC_PLAN_ID2, t14.MC_PLAN_ID3, t14.MC_PLAN_ID4, t14.MC_PLAN_ID5,
			                  t14.MC_PLAN_ID6, t14.MC_PLAN_ID7, t14.MC_PLAN_ID8, t14.MC_PLAN_ID9, t14.MC_PLAN_ID10,
                              t14.MC_PLAN_ID11, t14.MC_PLAN_ID12, t14.MC_PLAN_ID13, t14.MC_PLAN_ID14,
							  t14.MC_PLAN_ID15, t14.MC_PLAN_ID16,
			               t14.MC_PLAN_TYPE_CD1, t14.MC_PLAN_TYPE_CD2, t14.MC_PLAN_TYPE_CD3, t14.MC_PLAN_TYPE_CD4,
						   t14.MC_PLAN_TYPE_CD5, t14.MC_PLAN_TYPE_CD6, t14.MC_PLAN_TYPE_CD7, t14.MC_PLAN_TYPE_CD8,
						   t14.MC_PLAN_TYPE_CD9, t14.MC_PLAN_TYPE_CD10, t14.MC_PLAN_TYPE_CD11, t14.MC_PLAN_TYPE_CD12,
						   t14.MC_PLAN_TYPE_CD13, t14.MC_PLAN_TYPE_CD14, t14.MC_PLAN_TYPE_CD15, t14.MC_PLAN_TYPE_CD16,
			
			%ELG00015(15), t15.HISPANIC_ETHNICITY_FLG as HISPANIC_ETHNICITY_FLAG,

            %ELG00016(16),            
            t16.NATIVE_HI_FLG as NATIVE_HI_FLAG, t16.GUAM_CHAMORRO_FLG as GUAM_CHAMORRO_FLAG,
			t16.SAMOAN_FLG as SAMOAN_FLAG, t16.OTHER_PAC_ISLANDER_FLG as OTHER_PAC_ISLANDER_FLAG,
			t16.UNK_PAC_ISLANDER_FLG as UNK_PAC_ISLANDER_FLAG, t16.ASIAN_INDIAN_FLG as ASIAN_INDIAN_FLAG,
			t16.CHINESE_FLG as CHINESE_FLAG, t16.FILIPINO_FLG as FILIPINO_FLAG,
			t16.JAPANESE_FLG as JAPANESE_FLAG, t16.KOREAN_FLG as KOREAN_FLAG,
			t16.VIETNAMESE_FLG as VIETNAMESE_FLAG, t16.OTHER_ASIAN_FLG as OTHER_ASIAN_FLAG,
			t16.UNKNOWN_ASIAN_FLG as UNKNOWN_ASIAN_FLAG, t16.WHITE_FLG as WHITE_FLAG,
			t16.BLACK_AFRICAN_AMERICAN_FLG as BLACK_AFRICAN_AMERICAN_FLAG, t16.AIAN_FLG as AIAN_FLAG,


			case when 
			     (COALESCE(t16.WHITE_FLG,0) + COALESCE(t16.BLACK_AFRICAN_AMERICAN_FLG,0) 
		         + COALESCE(t16.AIAN_FLG,0) + COALESCE(GLOBAL_ASIAN,0) + COALESCE(GLOBAL_ISLANDER,0))>1
				 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 6

                 when t15.HISPANIC_ETHNICITY_FLG = 1 then 7
  	
                 when t16.WHITE_FLG = 1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 1

                 when t16.BLACK_AFRICAN_AMERICAN_FLG = 1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 2

				 when t16.GLOBAL_ASIAN=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 3

				 when t16.AIAN_FLG = 1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 4

				 when t16.GLOBAL_ISLANDER=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 5 

				 when t15.HISPANIC_ETHNICITY_FLG=0 then null

                else null end as RACE_ETHNICITY_FLAG,

		   case when t15.HISPANIC_ETHNICITY_FLG = 1 then 20
                when 
                (COALESCE(t16.WHITE_FLG,0) + COALESCE(t16.BLACK_AFRICAN_AMERICAN_FLG,0) 
		         + COALESCE(t16.AIAN_FLG,0) + COALESCE(GLOBAL_ASIAN,0) + COALESCE(GLOBAL_ISLANDER,0))>1
				 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 19 /* Multi Racial */
				when coalesce(t16.MULTI_ASIAN,0) = 1
                     and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 12 /* Multi Asian */
                when coalesce(t16.MULTI_ISLANDER,0) = 1
					 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 18 /* Multi Islander */
                when t16.WHITE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 1
		        when t16.BLACK_AFRICAN_AMERICAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 2
				when t16.AIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 3
				when t16.ASIAN_INDIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 4
				when t16.CHINESE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 5
				when t16.FILIPINO_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 6
				when t16.JAPANESE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 7
				when t16.KOREAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 8
				when t16.VIETNAMESE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 9
				when t16.OTHER_ASIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 10
				when t16.UNKNOWN_ASIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 11
				when t16.NATIVE_HI_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 13
				when t16.GUAM_CHAMORRO_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 14
				when t16.SAMOAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 15
			    when t16.OTHER_PAC_ISLANDER_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 16
				when t16.UNK_PAC_ISLANDER_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 17			
				when t15.HISPANIC_ETHNICITY_FLG = 0 then null 
           else null end as RACE_ETHNCTY_EXP_FLAG,

/*   			%ELG00017(17),*/
			t17.DEAF_DISAB_FLG as DEAF_DISAB_FLAG,
			t17.BLIND_DISAB_FLG as BLIND_DISAB_FLAG,
			t17.DIFF_CONC_DISAB_FLG as DIFF_CONC_DISAB_FLAG,
			t17.DIFF_WALKING_DISAB_FLG as DIFF_WALKING_DISAB_FLAG,
			t17.DIFF_DRESSING_BATHING_DISAB_FLG as DIFF_DRESSING_BATHING_DISAB_FLAG,
			t17.DIFF_ERRANDS_ALONE_DISAB_FLG as DIFF_ERRANDS_ALONE_DISAB_FLAG,
			t17.OTHER_DISAB_FLG as OTHER_DISAB_FLAG,

			%ELG00018(18), t18._1115A_PARTICIPANT_FLG as _1115A_PARTICIPANT_FLAG,

/*			%ELG00020(19),*/
			t19.HCBS_AGED_NON_HHCC_FLG as HCBS_AGED_NON_HHCC_FLAG,
			t19.HCBS_PHYS_DISAB_NON_HHCC_FLG as HCBS_PHYS_DISAB_NON_HHCC_FLAG,
			t19.HCBS_INTEL_DISAB_NON_HHCC_FLG as HCBS_INTEL_DISAB_NON_HHCC_FLAG,
			t19.HCBS_AUTISM_SP_DIS_NON_HHCC_FLG as HCBS_AUTISM_SP_DIS_NON_HHCC_FLAG,
			t19.HCBS_DD_NON_HHCC_FLG as HCBS_DD_NON_HHCC_FLAG,
			t19.HCBS_MI_SED_NON_HHCC_FLG as HCBS_MI_SED_NON_HHCC_FLAG,
			t19.HCBS_BRAIN_INJ_NON_HHCC_FLG as HCBS_BRAIN_INJ_NON_HHCC_FLAG,
			t19.HCBS_HIV_AIDS_NON_HHCC_FLG as HCBS_HIV_AIDS_NON_HHCC_FLAG,
			t19.HCBS_TECH_DEP_MF_NON_HHCC_FLG as HCBS_TECH_DEP_MF_NON_HHCC_FLAG,
			t19.HCBS_DISAB_OTHER_NON_HHCC_FLG as HCBS_DISAB_OTHER_NON_HHCC_FLAG,

			%TPL00002(20),

			%nrbquote('&TAF_FILE_DATE') as BSF_FIL_DT,

			%nrbquote('&VERSION') as BSF_VRSN,

			&DA_RUN_ID as DA_RUN_ID

	 from ELG00021_&BSF_FILE_DATE._uniq t1
        %tbl_joiner(ELG00002_&BSF_FILE_DATE._uniq,2) 
		%tbl_joiner(ELG00003_&BSF_FILE_DATE._uniq,3)
		%tbl_joiner(ELG00004_&BSF_FILE_DATE._uniq,4)
		%tbl_joiner(ELG00005_&BSF_FILE_DATE._uniq,5)
		%tbl_joiner(ELG00006_&BSF_FILE_DATE._uniq,6)
		%tbl_joiner(ELG00007_&BSF_FILE_DATE._uniq,7)
		%tbl_joiner(ELG00008_&BSF_FILE_DATE._uniq,8)
		%tbl_joiner(ELG00009_&BSF_FILE_DATE._uniq,9)
		%tbl_joiner(ELG00010_&BSF_FILE_DATE._uniq,10)
		%tbl_joiner(ELG00011_&BSF_FILE_DATE._uniq,11)
		%tbl_joiner(ELG00012_&BSF_FILE_DATE._uniq,12)
		%tbl_joiner(ELG00013_&BSF_FILE_DATE._uniq,13)
		%tbl_joiner(ELG00014_&BSF_FILE_DATE._uniq,14)
		%tbl_joiner(ELG00015_&BSF_FILE_DATE._uniq,15)
		%tbl_joiner(ELG00016_&BSF_FILE_DATE._uniq,16)
		%tbl_joiner(ELG00017_&BSF_FILE_DATE._uniq,17)
		%tbl_joiner(ELG00018_&BSF_FILE_DATE._uniq,18)
		%tbl_joiner(ELG00020_&BSF_FILE_DATE._uniq,19)
		%tbl_joiner(TPL00002_&BSF_FILE_DATE._uniq,20)
 
    )  by tmsis_passthrough;

%drop_table_multi(ELG00002_&BSF_FILE_DATE._uniq ELG00003_&BSF_FILE_DATE._uniq
                  ELG00004_&BSF_FILE_DATE._uniq ELG00005_&BSF_FILE_DATE._uniq ELG00006_&BSF_FILE_DATE._uniq 
                  ELG00007_&BSF_FILE_DATE._uniq ELG00008_&BSF_FILE_DATE._uniq ELG00009_&BSF_FILE_DATE._uniq
                  ELG00010_&BSF_FILE_DATE._uniq ELG00011_&BSF_FILE_DATE._uniq ELG00012_&BSF_FILE_DATE._uniq
                  ELG00013_&BSF_FILE_DATE._uniq ELG00014_&BSF_FILE_DATE._uniq ELG00015_&BSF_FILE_DATE._uniq
				  ELG00016_&BSF_FILE_DATE._uniq ELG00017_&BSF_FILE_DATE._uniq ELG00018_&BSF_FILE_DATE._uniq
				  ELG00020_&BSF_FILE_DATE._uniq TPL00002_&BSF_FILE_DATE._uniq ELG00021_&BSF_FILE_DATE._uniq)

execute (
     create temp table BSF_&RPT_OUT._&BSF_FILE_DATE.
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select distinct
        %FINAL_ORDER
	 from BSF_STEP1

		) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 023_bsf_ELG00023, 0.1. create_initial_table);

%drop_table_multi(BSF_STEP1)

%mend BSF_Final;

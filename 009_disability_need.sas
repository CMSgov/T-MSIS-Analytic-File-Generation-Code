/**********************************************************************************************/
/*Program: 009_disability_need.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual DE segment 009: Disability & Need
/*Mod: 
/*Notes: This program reads in all columns related to: HCBS, LTSS, LCKIN, and other needs.
/*       It creates four _SPLMTL flags based on these cols: HCBS_COND_SPLMTL, LTSS_SPLMTL, LCKIN_SPLMTL,
/*       and OTHER_NEEDS_SPLMTL. These columns are kept in a temp table to join to the base
/*       table, and then the rest of the columns are inserted into the permanent table, subset
/*       to any of the four flags = 1.
/**********************************************************************************************/


%macro create_DSBLTY;

	%create_temp_table(disability_need,
	              subcols=%nrbquote(  %last_best(HCBS_AGED_NON_HHCC_FLAG)
	                                  %last_best(HCBS_PHYS_DSBL_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_INTEL_DSBL_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_AUTSM_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_DD_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_MI_SED_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_BRN_INJ_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_HIV_AIDS_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_TECH_DEP_MF_NON_HHCC_FLAG,outcol=HCBS_TCH_DP_MF_NON_HHCC_FLAG) 
	                                  %last_best(HCBS_DSBL_OTHR_NON_HHCC_FLAG)
	                                  
	                                  %monthly_array(CARE_LVL_STUS_CD)
	                                  %monthly_array(DFCLTY_CONC_DSBL_FLAG,outcol=DFCLTY_CNCNTRTNG_DSBL_FLAG)
	                                  %monthly_array(DFCLTY_WLKG_DSBL_FLAG)
	                                  %monthly_array(DFCLTY_DRSNG_BATHG_DSBL_FLAG,outcol=DFCLTY_DRSNG_BTH_DSBL_FLAG)
	                                  %monthly_array(DFCLTY_ERRANDS_ALN_DSBL_FLAG,outcol=DFCLTY_ERNDS_ALN_DSBL_FLAG)
	                                  
	                                  %ever_year(LCKIN_FLAG,outcol=LCKIN_FLAG)
	                                  
									  /* Must use month of latest non-missing LCKIN_PRVDR_NUM1 to pull all three NUMs
									     and TYPES (so must array to then pull in outer query) */

									  %last_best(LCKIN_PRVDR_NUM1)
	                                  %nonmiss_month(LCKIN_PRVDR_NUM1)

									  %monthly_array(LCKIN_PRVDR_TYPE_CD1)
									  %monthly_array(LCKIN_PRVDR_NUM2) 
	                                  %monthly_array(LCKIN_PRVDR_TYPE_CD2) 
	                                  %monthly_array(LCKIN_PRVDR_NUM3)
	                                  %monthly_array(LCKIN_PRVDR_TYPE_CD3)

									  /* Must use month of latest non-missing LTSS_PRVDR_NUM1 to pull other
									     two provider nums (so must array to then pull in outer query) */
	                                  
	                                  %last_best(LTSS_PRVDR_NUM1)
									  %nonmiss_month(LTSS_PRVDR_NUM1)

	                                  %monthly_array(LTSS_LVL_CARE_CD1)
	                                  %last_best(LTSS_LVL_CARE_CD1,outcol=LTSS_LVL_CARE_CD1_LTST)
	                                  
									  %monthly_array(LTSS_PRVDR_NUM2)
	                                  %monthly_array(LTSS_LVL_CARE_CD2)
	                                  %last_best(LTSS_LVL_CARE_CD2,outcol=LTSS_LVL_CARE_CD2_LTST)
	                                  
									  %monthly_array(LTSS_PRVDR_NUM3)
	                                  %monthly_array(LTSS_LVL_CARE_CD3)
	                                  %last_best(LTSS_LVL_CARE_CD3,outcol=LTSS_LVL_CARE_CD3_LTST)
	                                  
	                                  %monthly_array(SSDI_IND)
	                                  %monthly_array(SSI_IND)
	                                  %monthly_array(SSI_STATE_SPLMT_STUS_CD)
	                                  %monthly_array(SSI_STUS_CD)
	                                  %monthly_array(BIRTH_CNCPTN_IND)
	                                  %monthly_array(TANF_CASH_CD)
	                                  
	                                  %monthly_array(TPL_INSRNC_CVRG_IND)
	                                  %monthly_array(TPL_OTHR_CVRG_IND)

									  /* Create indicators to be used for LTSS_SPLMTL */
										      
									  %ever_year(CARE_LVL_STUS_CD, condition=%nrstr(is not null))
									  %ever_year(LTSS_LVL_CARE_CD1, condition=%nrstr(is not null))
									  %ever_year(LTSS_LVL_CARE_CD2, condition=%nrstr(is not null))
									  %ever_year(LTSS_LVL_CARE_CD3, condition=%nrstr(is not null))

									  /* Create indicators to be used for OTHER_NEEDS_SPLMTL */

									  %ever_year(DFCLTY_CONC_DSBL_FLAG,outcol=DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR)
									  %ever_year(DFCLTY_WLKG_DSBL_FLAG)
									  %ever_year(DFCLTY_DRSNG_BATHG_DSBL_FLAG, outcol=DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR)
									  %ever_year(DFCLTY_ERRANDS_ALN_DSBL_FLAG, outcol=DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR)
									  %ever_year(SSDI_IND)
									  %ever_year(SSI_IND)
									  %ever_year(BIRTH_CNCPTN_IND)
									  %ever_year(TPL_INSRNC_CVRG_IND)
									  %ever_year(TPL_OTHR_CVRG_IND)
									  %ever_year(SSI_STATE_SPLMT_STUS_CD, usenulls=1, nullcond=%nrstr('000'),condition=is not null)
									  %ever_year(SSI_STUS_CD, usenulls=1, nullcond=%nrstr('000'),condition=is not null)
									  %ever_year(TANF_CASH_CD, condition=%nrstr(='2'))
 
	                  ),
	              
	              outercols=%nrbquote(  %assign_nonmiss_month(LCKIN_PRVDR_TYPE_CD1,LCKIN_PRVDR_NUM1_MN,LCKIN_PRVDR_TYPE_CD1)
	                                    %assign_nonmiss_month(LCKIN_PRVDR_NUM2,LCKIN_PRVDR_NUM1_MN,LCKIN_PRVDR_NUM2)
	                                    %assign_nonmiss_month(LCKIN_PRVDR_TYPE_CD2,LCKIN_PRVDR_NUM1_MN,LCKIN_PRVDR_TYPE_CD2)
	                                    %assign_nonmiss_month(LCKIN_PRVDR_NUM3,LCKIN_PRVDR_NUM1_MN,LCKIN_PRVDR_NUM3)
	                                    %assign_nonmiss_month(LCKIN_PRVDR_TYPE_CD3,LCKIN_PRVDR_NUM1_MN,LCKIN_PRVDR_TYPE_CD3)
	                                  
										%assign_nonmiss_month(LTSS_PRVDR_NUM2,LTSS_PRVDR_NUM1_MN,LTSS_PRVDR_NUM2)
										%assign_nonmiss_month(LTSS_PRVDR_NUM3,LTSS_PRVDR_NUM1_MN,LTSS_PRVDR_NUM3)

										/* Create HCBS_COND_SPLMTL (which will go onto the base segment and be used to determine 
										   whether a record will go onto the Disability and Need segment) */
										   
										 %any_col(HCBS_AGED_NON_HHCC_FLAG HCBS_PHYS_DSBL_NON_HHCC_FLAG 
                                                  HCBS_INTEL_DSBL_NON_HHCC_FLAG
										          HCBS_AUTSM_NON_HHCC_FLAG HCBS_DD_NON_HHCC_FLAG 
                                                  HCBS_MI_SED_NON_HHCC_FLAG HCBS_BRN_INJ_NON_HHCC_FLAG
										          HCBS_HIV_AIDS_NON_HHCC_FLAG HCBS_TCH_DP_MF_NON_HHCC_FLAG 
										          HCBS_DSBL_OTHR_NON_HHCC_FLAG,
										            
										          HCBS_COND_SPLMTL)

	                                     /* Create LCKIN_SPLMTL as LCKIN_FLAG (which will go onto the base segment and be used to determine 
   											whether a record will go onto the Disability and Need segment) */
   
   										,LCKIN_FLAG as LCKIN_SPLMTL
						

										/* Create OTHER_NEEDS_SPLMTL (which will go onto the base segment and be used to determine 
									       whether a record will go onto the Disability and Need segment) */
									   
									  %any_col(DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR DFCLTY_WLKG_DSBL_FLAG_EVR 
                                             DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR
									         DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR SSDI_IND_EVR SSI_IND_EVR
									         BIRTH_CNCPTN_IND_EVR TPL_INSRNC_CVRG_IND_EVR TPL_OTHR_CVRG_IND_EVR
									         SSI_STATE_SPLMT_STUS_CD_EVR SSI_STUS_CD_EVR TANF_CASH_CD_EVR,
									         
									         outcol=OTHER_NEEDS_SPLMTL)

	                          ) ); 


	 /* Create LTSS_SPLMTL (which will go onto the base segment and be used to determine 
   		whether a record will go onto the Disability and Need segment) */

	 execute (
	 	create temp table disability_need_&year.2
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select *
               ,case when CARE_LVL_STUS_CD_EVR=1 or 
				LTSS_LVL_CARE_CD1_EVR=1 or 
				LTSS_LVL_CARE_CD2_EVR=1 or
				LTSS_LVL_CARE_CD3_EVR=1 or
				LTSS_PRVDR_NUM1 is not null or
				LTSS_PRVDR_NUM2 is not null or
				LTSS_PRVDR_NUM3 is not null
				then 1 else 0
				end as LTSS_SPLMTL

		from disability_need_&year.

	) by tmsis_passthrough;

	/* Create temp table with JUST _SPLMTL flags to join to base table */

	execute(
		create temp table DIS_NEED_SPLMTLS_&year. 
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
	   		   ,msis_ident_num
			   ,HCBS_COND_SPLMTL
			   ,LCKIN_SPLMTL
			   ,LTSS_SPLMTL
			   ,OTHER_NEEDS_SPLMTL

		from disability_need_&year.2

	) by tmsis_passthrough;

	/* Insert into permanent table, subset to any of the four _SPLMTL flags = 1 */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select

			%table_id_cols
			,HCBS_AGED_NON_HHCC_FLAG 
			,HCBS_PHYS_DSBL_NON_HHCC_FLAG 
			,HCBS_INTEL_DSBL_NON_HHCC_FLAG 
			,HCBS_AUTSM_NON_HHCC_FLAG 
			,HCBS_DD_NON_HHCC_FLAG 
			,HCBS_MI_SED_NON_HHCC_FLAG 
			,HCBS_BRN_INJ_NON_HHCC_FLAG 
			,HCBS_HIV_AIDS_NON_HHCC_FLAG 
			,HCBS_TCH_DP_MF_NON_HHCC_FLAG 
			,HCBS_DSBL_OTHR_NON_HHCC_FLAG 
			,CARE_LVL_STUS_CD_01
			,CARE_LVL_STUS_CD_02
			,CARE_LVL_STUS_CD_03
			,CARE_LVL_STUS_CD_04
			,CARE_LVL_STUS_CD_05
			,CARE_LVL_STUS_CD_06
			,CARE_LVL_STUS_CD_07
			,CARE_LVL_STUS_CD_08
			,CARE_LVL_STUS_CD_09
			,CARE_LVL_STUS_CD_10
			,CARE_LVL_STUS_CD_11
			,CARE_LVL_STUS_CD_12
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_01
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_02
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_03
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_04
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_05
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_06
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_07
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_08
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_09
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_10
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_11
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_12
			,DFCLTY_WLKG_DSBL_FLAG_01
			,DFCLTY_WLKG_DSBL_FLAG_02
			,DFCLTY_WLKG_DSBL_FLAG_03
			,DFCLTY_WLKG_DSBL_FLAG_04
			,DFCLTY_WLKG_DSBL_FLAG_05
			,DFCLTY_WLKG_DSBL_FLAG_06
			,DFCLTY_WLKG_DSBL_FLAG_07
			,DFCLTY_WLKG_DSBL_FLAG_08
			,DFCLTY_WLKG_DSBL_FLAG_09
			,DFCLTY_WLKG_DSBL_FLAG_10
			,DFCLTY_WLKG_DSBL_FLAG_11
			,DFCLTY_WLKG_DSBL_FLAG_12
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_01
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_02
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_03
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_04
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_05
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_06
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_07
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_08
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_09
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_10
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_11
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_12
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_01
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_02
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_03
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_04
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_05
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_06
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_07
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_08
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_09
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_10
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_11
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_12
			,LCKIN_FLAG 
			,LCKIN_PRVDR_NUM1 
			,LCKIN_PRVDR_TYPE_CD1 
			,LCKIN_PRVDR_NUM2 
			,LCKIN_PRVDR_TYPE_CD2 
			,LCKIN_PRVDR_NUM3 
			,LCKIN_PRVDR_TYPE_CD3 
			,LTSS_PRVDR_NUM1 
			,LTSS_LVL_CARE_CD1_01
			,LTSS_LVL_CARE_CD1_02
			,LTSS_LVL_CARE_CD1_03
			,LTSS_LVL_CARE_CD1_04
			,LTSS_LVL_CARE_CD1_05
			,LTSS_LVL_CARE_CD1_06
			,LTSS_LVL_CARE_CD1_07
			,LTSS_LVL_CARE_CD1_08
			,LTSS_LVL_CARE_CD1_09
			,LTSS_LVL_CARE_CD1_10
			,LTSS_LVL_CARE_CD1_11
			,LTSS_LVL_CARE_CD1_12
			,LTSS_LVL_CARE_CD1_LTST
			,LTSS_PRVDR_NUM2 
			,LTSS_LVL_CARE_CD2_01
			,LTSS_LVL_CARE_CD2_02
			,LTSS_LVL_CARE_CD2_03
			,LTSS_LVL_CARE_CD2_04
			,LTSS_LVL_CARE_CD2_05
			,LTSS_LVL_CARE_CD2_06
			,LTSS_LVL_CARE_CD2_07
			,LTSS_LVL_CARE_CD2_08
			,LTSS_LVL_CARE_CD2_09
			,LTSS_LVL_CARE_CD2_10
			,LTSS_LVL_CARE_CD2_11
			,LTSS_LVL_CARE_CD2_12
			,LTSS_LVL_CARE_CD2_LTST
			,LTSS_PRVDR_NUM3 
			,LTSS_LVL_CARE_CD3_01
			,LTSS_LVL_CARE_CD3_02
			,LTSS_LVL_CARE_CD3_03
			,LTSS_LVL_CARE_CD3_04
			,LTSS_LVL_CARE_CD3_05
			,LTSS_LVL_CARE_CD3_06
			,LTSS_LVL_CARE_CD3_07
			,LTSS_LVL_CARE_CD3_08
			,LTSS_LVL_CARE_CD3_09
			,LTSS_LVL_CARE_CD3_10
			,LTSS_LVL_CARE_CD3_11
			,LTSS_LVL_CARE_CD3_12
			,LTSS_LVL_CARE_CD3_LTST
			,SSDI_IND_01
			,SSDI_IND_02
			,SSDI_IND_03
			,SSDI_IND_04
			,SSDI_IND_05
			,SSDI_IND_06
			,SSDI_IND_07
			,SSDI_IND_08
			,SSDI_IND_09
			,SSDI_IND_10
			,SSDI_IND_11
			,SSDI_IND_12
			,SSI_IND_01
			,SSI_IND_02
			,SSI_IND_03
			,SSI_IND_04
			,SSI_IND_05
			,SSI_IND_06
			,SSI_IND_07
			,SSI_IND_08
			,SSI_IND_09
			,SSI_IND_10
			,SSI_IND_11
			,SSI_IND_12
			,SSI_STATE_SPLMT_STUS_CD_01
			,SSI_STATE_SPLMT_STUS_CD_02
			,SSI_STATE_SPLMT_STUS_CD_03
			,SSI_STATE_SPLMT_STUS_CD_04
			,SSI_STATE_SPLMT_STUS_CD_05
			,SSI_STATE_SPLMT_STUS_CD_06
			,SSI_STATE_SPLMT_STUS_CD_07
			,SSI_STATE_SPLMT_STUS_CD_08
			,SSI_STATE_SPLMT_STUS_CD_09
			,SSI_STATE_SPLMT_STUS_CD_10
			,SSI_STATE_SPLMT_STUS_CD_11
			,SSI_STATE_SPLMT_STUS_CD_12
			,SSI_STUS_CD_01
			,SSI_STUS_CD_02
			,SSI_STUS_CD_03
			,SSI_STUS_CD_04
			,SSI_STUS_CD_05
			,SSI_STUS_CD_06
			,SSI_STUS_CD_07
			,SSI_STUS_CD_08
			,SSI_STUS_CD_09
			,SSI_STUS_CD_10
			,SSI_STUS_CD_11
			,SSI_STUS_CD_12
			,BIRTH_CNCPTN_IND_01
			,BIRTH_CNCPTN_IND_02
			,BIRTH_CNCPTN_IND_03
			,BIRTH_CNCPTN_IND_04
			,BIRTH_CNCPTN_IND_05
			,BIRTH_CNCPTN_IND_06
			,BIRTH_CNCPTN_IND_07
			,BIRTH_CNCPTN_IND_08
			,BIRTH_CNCPTN_IND_09
			,BIRTH_CNCPTN_IND_10
			,BIRTH_CNCPTN_IND_11
			,BIRTH_CNCPTN_IND_12
			,TANF_CASH_CD_01 
			,TANF_CASH_CD_02
			,TANF_CASH_CD_03
			,TANF_CASH_CD_04
			,TANF_CASH_CD_05
			,TANF_CASH_CD_06
			,TANF_CASH_CD_07
			,TANF_CASH_CD_08
			,TANF_CASH_CD_09
			,TANF_CASH_CD_10
			,TANF_CASH_CD_11
			,TANF_CASH_CD_12
			,TPL_INSRNC_CVRG_IND_01 
			,TPL_INSRNC_CVRG_IND_02
			,TPL_INSRNC_CVRG_IND_03
			,TPL_INSRNC_CVRG_IND_04
			,TPL_INSRNC_CVRG_IND_05
			,TPL_INSRNC_CVRG_IND_06
			,TPL_INSRNC_CVRG_IND_07
			,TPL_INSRNC_CVRG_IND_08
			,TPL_INSRNC_CVRG_IND_09
			,TPL_INSRNC_CVRG_IND_10
			,TPL_INSRNC_CVRG_IND_11
			,TPL_INSRNC_CVRG_IND_12
			,TPL_OTHR_CVRG_IND_01
			,TPL_OTHR_CVRG_IND_02
			,TPL_OTHR_CVRG_IND_03
			,TPL_OTHR_CVRG_IND_04
			,TPL_OTHR_CVRG_IND_05
			,TPL_OTHR_CVRG_IND_06
			,TPL_OTHR_CVRG_IND_07
			,TPL_OTHR_CVRG_IND_08
			,TPL_OTHR_CVRG_IND_09
			,TPL_OTHR_CVRG_IND_10
			,TPL_OTHR_CVRG_IND_11
			,TPL_OTHR_CVRG_IND_12

		from disability_need_&year.2
		where HCBS_COND_SPLMTL=1 or
			  LCKIN_SPLMTL=1 or
			  LTSS_SPLMTL=1 or
			  OTHER_NEEDS_SPLMTL=1

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(disability_need_&year. disability_need_&year.2)


%mend create_DSBLTY;

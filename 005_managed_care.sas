/**********************************************************************************************/
/*Program: 005_managed_care.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual BSF segment 005: Managed Care
/*Mod: 
/*Notes: This program arrays out all MC slots for every month. Also, it creates counts of
/*       enrolled months for each type of MC (based on a given type in ANY of the monthly slots
/*       for each month). It then inserts this temp table into the permanent table and deletes
/*       the temp table.
/*       It also creates the flag MNGD_CARE_SPLMTL which = 1 if ANY ID or type in the year is
/*       non-null, which will be kept in a temp table to be joined to the base segment.
/**********************************************************************************************/

%macro create_MC;

	%create_temp_table(managed_care,
	              subcols=%nrbquote( 
				  					/* Create monthly indicators for each type of MC plan to then sum 
				                       in the outer query. Note this will run for months 1-3 (which is the
				                       text limit of the macro var), and then the other months will run in 
				                       below additional subcol macro vars. */ 

									  %run_mc_slots(1,3)
	                                  
	                                  %monthly_array(MC_PLAN_ID,nslots=&nmcslots.)
	                                  %monthly_array(MC_PLAN_TYPE_CD,nslots=&nmcslots.)
	                                  
	              ),
				  subcols2=%nrbquote( 
                                     %run_mc_slots(4,6)
 				  ),
				  subcols3=%nrbquote( 
                                     %run_mc_slots(7,9)
			 	  ),
				  subcols4=%nrbquote( 
                                     %run_mc_slots(10,12)

			      ), 

				   /* Create _SPLMTL flag = 1 if ANY ID or type are non-null/00 (must put in separate subcol
				      var because of text limit). Must run in separate calls because of the macro var text
				      limit, and will then have to combine by identifying whether ANY =1. */

				  subcols5=%nrbquote( 
                                     %mc_nonnull_zero(MNGD_CARE_SPLMTL,1,3)

			      ),

				  subcols6=%nrbquote( 
                                     %mc_nonnull_zero(MNGD_CARE_SPLMTL,4,6)

			      ),

				  subcols7=%nrbquote( 
                                     %mc_nonnull_zero(MNGD_CARE_SPLMTL,7,9)

			      ),

				  subcols8=%nrbquote( 
                                     %mc_nonnull_zero(MNGD_CARE_SPLMTL,10,12)

			      ),
	              
	              outercols=%nrbquote(  %sum_months(CMPRHNSV_MC_PLAN)
										%sum_months(TRDTNL_PCCM_MC_PLAN)
										%sum_months(ENHNCD_PCCM_MC_PLAN)
										%sum_months(HIO_MC_PLAN)
										%sum_months(PIHP_MC_PLAN)
										%sum_months(PAHP_MC_PLAN)
										%sum_months(LTC_PIHP_MC_PLAN)
										%sum_months(MH_PIHP_MC_PLAN)
										%sum_months(MH_PAHP_MC_PLAN)
										%sum_months(SUD_PIHP_MC_PLAN)
										%sum_months(SUD_PAHP_MC_PLAN)
										%sum_months(MH_SUD_PIHP_MC_PLAN)
										%sum_months(MH_SUD_PAHP_MC_PLAN)
										%sum_months(DNTL_PAHP_MC_PLAN)
										%sum_months(TRANSPRTN_PAHP_MC_PLAN)
										%sum_months(DEASE_MGMT_MC_PLAN)
										%sum_months(PACE_MC_PLAN)
										%sum_months(PHRMCY_PAHP_MC_PLAN)
										%sum_months(ACNTBL_MC_PLAN)
										%sum_months(HM_HOME_MC_PLAN)
										%sum_months(IC_DUALS_MC_PLAN)

	              ) );


	/* Create temp table with just MNGD_CARE_SPLMTL (must create as ANY=1 from the four above) to join to base */

	execute(
		create temp table MNGD_CARE_SPLMTL_&year. 
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
	   		   ,msis_ident_num
			   ,case when MNGD_CARE_SPLMTL_1_3=1 or MNGD_CARE_SPLMTL_4_6=1 or
			              MNGD_CARE_SPLMTL_7_9=1 or MNGD_CARE_SPLMTL_10_12=1 
                     then 1 else 0 end
                     as MNGD_CARE_SPLMTL

		from managed_care_&year.

	) by tmsis_passthrough;

	/* Insert into the permanent table, subset to ANY MNGD_CARE_SPLMTL=1 */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select

			%table_id_cols
			,CMPRHNSV_MC_PLAN_MOS
			,TRDTNL_PCCM_MC_PLAN_MOS
			,ENHNCD_PCCM_MC_PLAN_MOS
			,HIO_MC_PLAN_MOS
			,PIHP_MC_PLAN_MOS
			,PAHP_MC_PLAN_MOS
			,LTC_PIHP_MC_PLAN_MOS
			,MH_PIHP_MC_PLAN_MOS
			,MH_PAHP_MC_PLAN_MOS
			,SUD_PIHP_MC_PLAN_MOS
			,SUD_PAHP_MC_PLAN_MOS
			,MH_SUD_PIHP_MC_PLAN_MOS
			,MH_SUD_PAHP_MC_PLAN_MOS
			,DNTL_PAHP_MC_PLAN_MOS
			,TRANSPRTN_PAHP_MC_PLAN_MOS
			,DEASE_MGMT_MC_PLAN_MOS
			,PACE_MC_PLAN_MOS
			,PHRMCY_PAHP_MC_PLAN_MOS
			,ACNTBL_MC_PLAN_MOS
			,HM_HOME_MC_PLAN_MOS
			,IC_DUALS_MC_PLAN_MOS
			,MC_PLAN_ID1_01
			,MC_PLAN_ID1_02
			,MC_PLAN_ID1_03
			,MC_PLAN_ID1_04
			,MC_PLAN_ID1_05
			,MC_PLAN_ID1_06
			,MC_PLAN_ID1_07
			,MC_PLAN_ID1_08
			,MC_PLAN_ID1_09
			,MC_PLAN_ID1_10
			,MC_PLAN_ID1_11
			,MC_PLAN_ID1_12
			,MC_PLAN_TYPE_CD1_01
			,MC_PLAN_TYPE_CD1_02
			,MC_PLAN_TYPE_CD1_03
			,MC_PLAN_TYPE_CD1_04
			,MC_PLAN_TYPE_CD1_05
			,MC_PLAN_TYPE_CD1_06
			,MC_PLAN_TYPE_CD1_07
			,MC_PLAN_TYPE_CD1_08
			,MC_PLAN_TYPE_CD1_09
			,MC_PLAN_TYPE_CD1_10
			,MC_PLAN_TYPE_CD1_11
			,MC_PLAN_TYPE_CD1_12
			,MC_PLAN_ID2_01
			,MC_PLAN_ID2_02
			,MC_PLAN_ID2_03
			,MC_PLAN_ID2_04
			,MC_PLAN_ID2_05
			,MC_PLAN_ID2_06
			,MC_PLAN_ID2_07
			,MC_PLAN_ID2_08
			,MC_PLAN_ID2_09
			,MC_PLAN_ID2_10
			,MC_PLAN_ID2_11
			,MC_PLAN_ID2_12
			,MC_PLAN_TYPE_CD2_01
			,MC_PLAN_TYPE_CD2_02
			,MC_PLAN_TYPE_CD2_03
			,MC_PLAN_TYPE_CD2_04
			,MC_PLAN_TYPE_CD2_05
			,MC_PLAN_TYPE_CD2_06
			,MC_PLAN_TYPE_CD2_07
			,MC_PLAN_TYPE_CD2_08
			,MC_PLAN_TYPE_CD2_09
			,MC_PLAN_TYPE_CD2_10
			,MC_PLAN_TYPE_CD2_11
			,MC_PLAN_TYPE_CD2_12
			,MC_PLAN_ID3_01
			,MC_PLAN_ID3_02
			,MC_PLAN_ID3_03
			,MC_PLAN_ID3_04
			,MC_PLAN_ID3_05
			,MC_PLAN_ID3_06
			,MC_PLAN_ID3_07
			,MC_PLAN_ID3_08
			,MC_PLAN_ID3_09
			,MC_PLAN_ID3_10
			,MC_PLAN_ID3_11
			,MC_PLAN_ID3_12
			,MC_PLAN_TYPE_CD3_01
			,MC_PLAN_TYPE_CD3_02
			,MC_PLAN_TYPE_CD3_03
			,MC_PLAN_TYPE_CD3_04
			,MC_PLAN_TYPE_CD3_05
			,MC_PLAN_TYPE_CD3_06
			,MC_PLAN_TYPE_CD3_07
			,MC_PLAN_TYPE_CD3_08
			,MC_PLAN_TYPE_CD3_09
			,MC_PLAN_TYPE_CD3_10
			,MC_PLAN_TYPE_CD3_11
			,MC_PLAN_TYPE_CD3_12
			,MC_PLAN_ID4_01
			,MC_PLAN_ID4_02
			,MC_PLAN_ID4_03
			,MC_PLAN_ID4_04
			,MC_PLAN_ID4_05
			,MC_PLAN_ID4_06
			,MC_PLAN_ID4_07
			,MC_PLAN_ID4_08
			,MC_PLAN_ID4_09
			,MC_PLAN_ID4_10
			,MC_PLAN_ID4_11
			,MC_PLAN_ID4_12
			,MC_PLAN_TYPE_CD4_01
			,MC_PLAN_TYPE_CD4_02
			,MC_PLAN_TYPE_CD4_03
			,MC_PLAN_TYPE_CD4_04
			,MC_PLAN_TYPE_CD4_05
			,MC_PLAN_TYPE_CD4_06
			,MC_PLAN_TYPE_CD4_07
			,MC_PLAN_TYPE_CD4_08
			,MC_PLAN_TYPE_CD4_09
			,MC_PLAN_TYPE_CD4_10
			,MC_PLAN_TYPE_CD4_11
			,MC_PLAN_TYPE_CD4_12
			,MC_PLAN_ID5_01
			,MC_PLAN_ID5_02
			,MC_PLAN_ID5_03
			,MC_PLAN_ID5_04
			,MC_PLAN_ID5_05
			,MC_PLAN_ID5_06
			,MC_PLAN_ID5_07
			,MC_PLAN_ID5_08
			,MC_PLAN_ID5_09
			,MC_PLAN_ID5_10
			,MC_PLAN_ID5_11
			,MC_PLAN_ID5_12
			,MC_PLAN_TYPE_CD5_01
			,MC_PLAN_TYPE_CD5_02
			,MC_PLAN_TYPE_CD5_03
			,MC_PLAN_TYPE_CD5_04
			,MC_PLAN_TYPE_CD5_05
			,MC_PLAN_TYPE_CD5_06
			,MC_PLAN_TYPE_CD5_07
			,MC_PLAN_TYPE_CD5_08
			,MC_PLAN_TYPE_CD5_09
			,MC_PLAN_TYPE_CD5_10
			,MC_PLAN_TYPE_CD5_11
			,MC_PLAN_TYPE_CD5_12
			,MC_PLAN_ID6_01
			,MC_PLAN_ID6_02
			,MC_PLAN_ID6_03
			,MC_PLAN_ID6_04
			,MC_PLAN_ID6_05
			,MC_PLAN_ID6_06
			,MC_PLAN_ID6_07
			,MC_PLAN_ID6_08
			,MC_PLAN_ID6_09
			,MC_PLAN_ID6_10
			,MC_PLAN_ID6_11
			,MC_PLAN_ID6_12
			,MC_PLAN_TYPE_CD6_01
			,MC_PLAN_TYPE_CD6_02
			,MC_PLAN_TYPE_CD6_03
			,MC_PLAN_TYPE_CD6_04
			,MC_PLAN_TYPE_CD6_05
			,MC_PLAN_TYPE_CD6_06
			,MC_PLAN_TYPE_CD6_07
			,MC_PLAN_TYPE_CD6_08
			,MC_PLAN_TYPE_CD6_09
			,MC_PLAN_TYPE_CD6_10
			,MC_PLAN_TYPE_CD6_11
			,MC_PLAN_TYPE_CD6_12
			,MC_PLAN_ID7_01
			,MC_PLAN_ID7_02
			,MC_PLAN_ID7_03
			,MC_PLAN_ID7_04
			,MC_PLAN_ID7_05
			,MC_PLAN_ID7_06
			,MC_PLAN_ID7_07
			,MC_PLAN_ID7_08
			,MC_PLAN_ID7_09
			,MC_PLAN_ID7_10
			,MC_PLAN_ID7_11
			,MC_PLAN_ID7_12
			,MC_PLAN_TYPE_CD7_01
			,MC_PLAN_TYPE_CD7_02
			,MC_PLAN_TYPE_CD7_03
			,MC_PLAN_TYPE_CD7_04
			,MC_PLAN_TYPE_CD7_05
			,MC_PLAN_TYPE_CD7_06
			,MC_PLAN_TYPE_CD7_07
			,MC_PLAN_TYPE_CD7_08
			,MC_PLAN_TYPE_CD7_09
			,MC_PLAN_TYPE_CD7_10
			,MC_PLAN_TYPE_CD7_11
			,MC_PLAN_TYPE_CD7_12
			,MC_PLAN_ID8_01
			,MC_PLAN_ID8_02
			,MC_PLAN_ID8_03
			,MC_PLAN_ID8_04
			,MC_PLAN_ID8_05
			,MC_PLAN_ID8_06
			,MC_PLAN_ID8_07
			,MC_PLAN_ID8_08
			,MC_PLAN_ID8_09
			,MC_PLAN_ID8_10
			,MC_PLAN_ID8_11
			,MC_PLAN_ID8_12
			,MC_PLAN_TYPE_CD8_01
			,MC_PLAN_TYPE_CD8_02
			,MC_PLAN_TYPE_CD8_03
			,MC_PLAN_TYPE_CD8_04
			,MC_PLAN_TYPE_CD8_05
			,MC_PLAN_TYPE_CD8_06
			,MC_PLAN_TYPE_CD8_07
			,MC_PLAN_TYPE_CD8_08
			,MC_PLAN_TYPE_CD8_09
			,MC_PLAN_TYPE_CD8_10
			,MC_PLAN_TYPE_CD8_11
			,MC_PLAN_TYPE_CD8_12
			,MC_PLAN_ID9_01
			,MC_PLAN_ID9_02
			,MC_PLAN_ID9_03
			,MC_PLAN_ID9_04
			,MC_PLAN_ID9_05
			,MC_PLAN_ID9_06
			,MC_PLAN_ID9_07
			,MC_PLAN_ID9_08
			,MC_PLAN_ID9_09
			,MC_PLAN_ID9_10
			,MC_PLAN_ID9_11
			,MC_PLAN_ID9_12
			,MC_PLAN_TYPE_CD9_01
			,MC_PLAN_TYPE_CD9_02
			,MC_PLAN_TYPE_CD9_03
			,MC_PLAN_TYPE_CD9_04
			,MC_PLAN_TYPE_CD9_05
			,MC_PLAN_TYPE_CD9_06
			,MC_PLAN_TYPE_CD9_07
			,MC_PLAN_TYPE_CD9_08
			,MC_PLAN_TYPE_CD9_09
			,MC_PLAN_TYPE_CD9_10
			,MC_PLAN_TYPE_CD9_11
			,MC_PLAN_TYPE_CD9_12
			,MC_PLAN_ID10_01
			,MC_PLAN_ID10_02
			,MC_PLAN_ID10_03
			,MC_PLAN_ID10_04
			,MC_PLAN_ID10_05
			,MC_PLAN_ID10_06
			,MC_PLAN_ID10_07
			,MC_PLAN_ID10_08
			,MC_PLAN_ID10_09
			,MC_PLAN_ID10_10
			,MC_PLAN_ID10_11
			,MC_PLAN_ID10_12
			,MC_PLAN_TYPE_CD10_01
			,MC_PLAN_TYPE_CD10_02
			,MC_PLAN_TYPE_CD10_03
			,MC_PLAN_TYPE_CD10_04
			,MC_PLAN_TYPE_CD10_05
			,MC_PLAN_TYPE_CD10_06
			,MC_PLAN_TYPE_CD10_07
			,MC_PLAN_TYPE_CD10_08
			,MC_PLAN_TYPE_CD10_09
			,MC_PLAN_TYPE_CD10_10
			,MC_PLAN_TYPE_CD10_11
			,MC_PLAN_TYPE_CD10_12
			,MC_PLAN_ID11_01
			,MC_PLAN_ID11_02
			,MC_PLAN_ID11_03
			,MC_PLAN_ID11_04
			,MC_PLAN_ID11_05
			,MC_PLAN_ID11_06
			,MC_PLAN_ID11_07
			,MC_PLAN_ID11_08
			,MC_PLAN_ID11_09
			,MC_PLAN_ID11_10
			,MC_PLAN_ID11_11
			,MC_PLAN_ID11_12
			,MC_PLAN_TYPE_CD11_01
			,MC_PLAN_TYPE_CD11_02
			,MC_PLAN_TYPE_CD11_03
			,MC_PLAN_TYPE_CD11_04
			,MC_PLAN_TYPE_CD11_05
			,MC_PLAN_TYPE_CD11_06
			,MC_PLAN_TYPE_CD11_07
			,MC_PLAN_TYPE_CD11_08
			,MC_PLAN_TYPE_CD11_09
			,MC_PLAN_TYPE_CD11_10
			,MC_PLAN_TYPE_CD11_11
			,MC_PLAN_TYPE_CD11_12
			,MC_PLAN_ID12_01
			,MC_PLAN_ID12_02
			,MC_PLAN_ID12_03
			,MC_PLAN_ID12_04
			,MC_PLAN_ID12_05
			,MC_PLAN_ID12_06
			,MC_PLAN_ID12_07
			,MC_PLAN_ID12_08
			,MC_PLAN_ID12_09
			,MC_PLAN_ID12_10
			,MC_PLAN_ID12_11
			,MC_PLAN_ID12_12
			,MC_PLAN_TYPE_CD12_01
			,MC_PLAN_TYPE_CD12_02
			,MC_PLAN_TYPE_CD12_03
			,MC_PLAN_TYPE_CD12_04
			,MC_PLAN_TYPE_CD12_05
			,MC_PLAN_TYPE_CD12_06
			,MC_PLAN_TYPE_CD12_07
			,MC_PLAN_TYPE_CD12_08
			,MC_PLAN_TYPE_CD12_09
			,MC_PLAN_TYPE_CD12_10
			,MC_PLAN_TYPE_CD12_11
			,MC_PLAN_TYPE_CD12_12
			,MC_PLAN_ID13_01
			,MC_PLAN_ID13_02
			,MC_PLAN_ID13_03
			,MC_PLAN_ID13_04
			,MC_PLAN_ID13_05
			,MC_PLAN_ID13_06
			,MC_PLAN_ID13_07
			,MC_PLAN_ID13_08
			,MC_PLAN_ID13_09
			,MC_PLAN_ID13_10
			,MC_PLAN_ID13_11
			,MC_PLAN_ID13_12
			,MC_PLAN_TYPE_CD13_01
			,MC_PLAN_TYPE_CD13_02
			,MC_PLAN_TYPE_CD13_03
			,MC_PLAN_TYPE_CD13_04
			,MC_PLAN_TYPE_CD13_05
			,MC_PLAN_TYPE_CD13_06
			,MC_PLAN_TYPE_CD13_07
			,MC_PLAN_TYPE_CD13_08
			,MC_PLAN_TYPE_CD13_09
			,MC_PLAN_TYPE_CD13_10
			,MC_PLAN_TYPE_CD13_11
			,MC_PLAN_TYPE_CD13_12
			,MC_PLAN_ID14_01
			,MC_PLAN_ID14_02
			,MC_PLAN_ID14_03
			,MC_PLAN_ID14_04
			,MC_PLAN_ID14_05
			,MC_PLAN_ID14_06
			,MC_PLAN_ID14_07
			,MC_PLAN_ID14_08
			,MC_PLAN_ID14_09
			,MC_PLAN_ID14_10
			,MC_PLAN_ID14_11
			,MC_PLAN_ID14_12
			,MC_PLAN_TYPE_CD14_01
			,MC_PLAN_TYPE_CD14_02
			,MC_PLAN_TYPE_CD14_03
			,MC_PLAN_TYPE_CD14_04
			,MC_PLAN_TYPE_CD14_05
			,MC_PLAN_TYPE_CD14_06
			,MC_PLAN_TYPE_CD14_07
			,MC_PLAN_TYPE_CD14_08
			,MC_PLAN_TYPE_CD14_09
			,MC_PLAN_TYPE_CD14_10
			,MC_PLAN_TYPE_CD14_11
			,MC_PLAN_TYPE_CD14_12
			,MC_PLAN_ID15_01
			,MC_PLAN_ID15_02
			,MC_PLAN_ID15_03
			,MC_PLAN_ID15_04
			,MC_PLAN_ID15_05
			,MC_PLAN_ID15_06
			,MC_PLAN_ID15_07
			,MC_PLAN_ID15_08
			,MC_PLAN_ID15_09
			,MC_PLAN_ID15_10
			,MC_PLAN_ID15_11
			,MC_PLAN_ID15_12
			,MC_PLAN_TYPE_CD15_01
			,MC_PLAN_TYPE_CD15_02
			,MC_PLAN_TYPE_CD15_03
			,MC_PLAN_TYPE_CD15_04
			,MC_PLAN_TYPE_CD15_05
			,MC_PLAN_TYPE_CD15_06
			,MC_PLAN_TYPE_CD15_07
			,MC_PLAN_TYPE_CD15_08
			,MC_PLAN_TYPE_CD15_09
			,MC_PLAN_TYPE_CD15_10
			,MC_PLAN_TYPE_CD15_11
			,MC_PLAN_TYPE_CD15_12
			,MC_PLAN_ID16_01
			,MC_PLAN_ID16_02
			,MC_PLAN_ID16_03
			,MC_PLAN_ID16_04
			,MC_PLAN_ID16_05
			,MC_PLAN_ID16_06
			,MC_PLAN_ID16_07
			,MC_PLAN_ID16_08
			,MC_PLAN_ID16_09
			,MC_PLAN_ID16_10
			,MC_PLAN_ID16_11
			,MC_PLAN_ID16_12
			,MC_PLAN_TYPE_CD16_01
			,MC_PLAN_TYPE_CD16_02
			,MC_PLAN_TYPE_CD16_03
			,MC_PLAN_TYPE_CD16_04
			,MC_PLAN_TYPE_CD16_05
			,MC_PLAN_TYPE_CD16_06
			,MC_PLAN_TYPE_CD16_07
			,MC_PLAN_TYPE_CD16_08
			,MC_PLAN_TYPE_CD16_09
			,MC_PLAN_TYPE_CD16_10
			,MC_PLAN_TYPE_CD16_11
			,MC_PLAN_TYPE_CD16_12

		from managed_care_&year.
		where MNGD_CARE_SPLMTL_1_3=1 or MNGD_CARE_SPLMTL_4_6=1 or
			  MNGD_CARE_SPLMTL_7_9=1 or MNGD_CARE_SPLMTL_10_12=1 

	) by tmsis_passthrough;

	/* Drop temp tables */

	%drop_tables(managed_care_&year.)

%mend create_MC;

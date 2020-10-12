/**********************************************************************************************/
/*Program: 001_base.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual DE segment 001: Base
/*Mod: 
/*Notes: This program first creates all the non-demographic columns for the base file, using
/*       the current year data. It then pulls in the demographic columns (for which we also look
/*       to the prior year if available) for current and prior year, if getprior=1. If getprior=1,
/*       join the current and prior year demographics and take the last best with current year
/*       first, then prior. It then joins the non-demographic data to the demographic data.
/*       Finally, it pulls in the days eligible counts and _SPLMTL flags.
/*       It then inserts into the permanent table.
/**********************************************************************************************/

%macro create_BASE;

	/* Create the base segment, pulling in only non-demographic columns for which we DO NOT look to the prior year.
       Set all pregnancy flags to null */

	%create_temp_table(base_nondemo,
		          subcols=%nrbquote( %last_best(CTZNSHP_VRFCTN_IND)
								     %last_best(IMGRTN_STUS_CD)
								     %last_best(IMGRTN_VRFCTN_IND) 

									 ,null :: smallint as PRGNCY_FLAG_01
									 ,null :: smallint as PRGNCY_FLAG_02
									 ,null :: smallint as PRGNCY_FLAG_03
									 ,null :: smallint as PRGNCY_FLAG_04
									 ,null :: smallint as PRGNCY_FLAG_05
									 ,null :: smallint as PRGNCY_FLAG_06
									 ,null :: smallint as PRGNCY_FLAG_07
									 ,null :: smallint as PRGNCY_FLAG_08
									 ,null :: smallint as PRGNCY_FLAG_09
									 ,null :: smallint as PRGNCY_FLAG_10
									 ,null :: smallint as PRGNCY_FLAG_11
									 ,null :: smallint as PRGNCY_FLAG_12

									 ,null :: smallint as PRGNCY_FLAG_EVR

									 %monthly_array(ELGBLTY_GRP_CD)
									 %last_best(ELGBLTY_GRP_CD,outcol=ELGBLTY_GRP_CD_LTST)
									 %monthly_array(MASBOE_CD)
									 %last_best(MASBOE_CD,outcol=MASBOE_CD_LTST)
									 %last_best(CARE_LVL_STUS_CD)
									 %ever_year(DEAF_DSBL_FLAG)
									 %ever_year(BLND_DSBL_FLAG)
									 %ever_year(DFCLTY_CONC_DSBL_FLAG,outcol=DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR)
									 %ever_year(DFCLTY_WLKG_DSBL_FLAG)
									 %ever_year(DFCLTY_DRSNG_BATHG_DSBL_FLAG,outcol=DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR)
									 %ever_year(DFCLTY_ERRANDS_ALN_DSBL_FLAG,outcol=DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR)
									 %ever_year(OTHR_DSBL_FLAG)

									 %monthly_array(CHIP_CD)
									 %last_best(CHIP_CD,outcol=CHIP_CD_LTST)
     
									 %monthly_array(STATE_SPEC_ELGBLTY_FCTR_TXT,outcol=STATE_SPEC_ELGBLTY_GRP)
									 %last_best(STATE_SPEC_ELGBLTY_FCTR_TXT,outcol=STATE_SPEC_ELGBLTY_GRP_LTST)
									 %monthly_array(DUAL_ELGBL_CD)
									 %last_best(DUAL_ELGBL_CD,outcol=DUAL_ELGBL_CD_LTST)
									                     
									 %mc_type_rank(smonth=1,emonth=2)
									                  
									 %monthly_array(RSTRCTD_BNFTS_CD)
									 %last_best(RSTRCTD_BNFTS_CD,outcol=RSTRCTD_BNFTS_CD_LTST)
									 %last_best(SSDI_IND)
									 %last_best(SSI_IND)
									 %last_best(SSI_STATE_SPLMT_STUS_CD)
									 %last_best(SSI_STUS_CD)
									 %last_best(BIRTH_CNCPTN_IND)
									 %last_best(TANF_CASH_CD)
									 %last_best(TPL_INSRNC_CVRG_IND)
									 %last_best(TPL_OTHR_CVRG_IND)	

									 %misg_enrlmt_type
									 

		              ),
                      subcols2=%nrbquote(
                                     %mc_type_rank(smonth=3,emonth=4)
                     ),
                      subcols3=%nrbquote(
                                     %mc_type_rank(smonth=5,emonth=6)
                     ),
                      subcols4=%nrbquote(
                                     %mc_type_rank(smonth=7,emonth=8)
                     ),
                      subcols5=%nrbquote(
                                     %mc_type_rank(smonth=9,emonth=10)
                     ),
                      subcols6=%nrbquote(
                                     %mc_type_rank(smonth=11,emonth=12)

                     ) );

	/* Now pull in the demographic columns for which we will look to the prior year if data are
	   available - if so, pull in the same columns for the prior year and then join to get last/best
       values from current and prior year */

	%macro demographics(runyear);

		%create_temp_table(base_demo,
		              inyear=&runyear.,
		              subcols=%nrbquote(  %last_best(SSN_NUM)
								          %last_best(BIRTH_DT)
								          %last_best(DEATH_DT)
								          %last_best(DCSD_FLAG)
								          %last_best(AGE_NUM)
								          %last_best(AGE_GRP_FLAG)
								          %last_best(GNDR_CD)
								          %last_best(MRTL_STUS_CD)
								          %last_best(INCM_CD)
								          %last_best(VET_IND)
								          %last_best(CTZNSHP_IND)
								          %last_best(IMGRTN_STUS_5_YR_BAR_END_DT)
										  %last_best(OTHR_LANG_HOME_CD)
								          %last_best(PRMRY_LANG_FLAG)
								          %last_best(PRMRY_LANG_ENGLSH_PRFCNCY_CD)
								          %last_best(HSEHLD_SIZE_CD)
									                    
								          %last_best(CRTFD_AMRCN_INDN_ALSKN_NTV_IND)
								          %last_best(ETHNCTY_CD)
								          %last_best(RACE_ETHNCTY_FLAG)
								          %last_best(RACE_ETHNCTY_EXP_FLAG)

										  /* Must array all address elements to take the value that aligns with
										     latest non-missing home address 1 (in outer loop) */

										  %monthly_array(ELGBL_LINE_1_ADR_HOME)
										  %monthly_array(ELGBL_LINE_1_ADR_MAIL)
								                      
							              %monthly_array(ELGBL_ZIP_CD_HOME)
		                                  %monthly_array(ELGBL_CNTY_CD_HOME)
		                                  %monthly_array(ELGBL_STATE_CD_HOME)
		                                  %monthly_array(ELGBL_ZIP_CD_MAIL)
		                                  %monthly_array(ELGBL_CNTY_CD_MAIL)
		                                  %monthly_array(ELGBL_STATE_CD_MAIL) 

								          %nonmiss_month(ELGBL_LINE_1_ADR_HOME)
		                                  %nonmiss_month(ELGBL_LINE_1_ADR_MAIL) 

								          %last_best(MSIS_CASE_NUM) 
									                    
								          %last_best(MDCR_BENE_ID)
								          %last_best(MDCR_HICN_NUM)
		              ) ,
		                      
		              outercols=%nrbquote(  %assign_nonmiss_month(ELGBL_LINE_1_ADR,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_LINE_1_ADR_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_LINE_1_ADR_MAIL)
		                                    %assign_nonmiss_month(ELGBL_ZIP_CD,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_ZIP_CD_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_ZIP_CD_MAIL)
		                                    %assign_nonmiss_month(ELGBL_CNTY_CD,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_CNTY_CD_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_CNTY_CD_MAIL)
		                                    %assign_nonmiss_month(ELGBL_STATE_CD,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_STATE_CD_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_STATE_CD_MAIL)
		                                  
									) );

	%mend demographics;

	%demographics(&year.)

	/* If getprior=1 (have prior year(s) of TAF to get prior information for), run the above for all prior years AND
	   then combine for those demographics only */ 

    %if &getprior.=1 %then %do;
		%do p=1 %to %sysfunc(countw(&pyears.));
	 		%let pyear=%scan(&pyears.,&p.);
			%demographics(&pyear.)
		%end; 
	
	   /* Now join the above tables together to use prior years if current year is missing, keeping demographics only.
		  For address information, identify year pulled for latest non-null value of ELGBL_LINE_1_ADR. 
		   Use that year to then take value for all cols */
   
	  execute(
	  	create temp table base_demo_&year._out
	  	distkey(msis_ident_num)
	  	sortkey(submtg_state_cd,msis_ident_num) as
	  	
	  	select  c.msis_ident_num 
	  	       ,c.submtg_state_cd
	  	       
	  	       %last_best(SSN_NUM,prior=1)
			   %last_best(BIRTH_DT,prior=1)
			   %last_best(DEATH_DT,prior=1)
			   %last_best(DCSD_FLAG,prior=1)
			   %last_best(AGE_NUM,prior=1)
			   %last_best(AGE_GRP_FLAG,prior=1)
			   %last_best(GNDR_CD,prior=1)
			   %last_best(MRTL_STUS_CD,prior=1)
			   %last_best(INCM_CD,prior=1)
			   %last_best(VET_IND,prior=1)
			   %last_best(CTZNSHP_IND,prior=1)
			   %last_best(IMGRTN_STUS_5_YR_BAR_END_DT,prior=1)
			   %last_best(OTHR_LANG_HOME_CD,prior=1)
			   %last_best(PRMRY_LANG_FLAG,prior=1)
			   %last_best(PRMRY_LANG_ENGLSH_PRFCNCY_CD,prior=1)
			   %last_best(HSEHLD_SIZE_CD,prior=1)
							                      
			   %last_best(CRTFD_AMRCN_INDN_ALSKN_NTV_IND,prior=1)
			   %last_best(ETHNCTY_CD,prior=1)
			   %last_best(RACE_ETHNCTY_FLAG,prior=1)
			   %last_best(RACE_ETHNCTY_EXP_FLAG,prior=1)

			   ,case when c.ELGBL_LINE_1_ADR is not null then &year.
				         %do p=1 %to %sysfunc(countw(&pyears.));
	 		      			  %let pyear=%scan(&pyears.,&p.);
							  when p&p..ELGBL_LINE_1_ADR is not null then &pyear.
						 %end;
						 else null
						 end as yearpull

				%address_same_year(ELGBL_ZIP_CD)
				%address_same_year(ELGBL_CNTY_CD)
				%address_same_year(ELGBL_STATE_CD)
							                      
			   %last_best(MSIS_CASE_NUM,prior=1)
			   %last_best(MDCR_BENE_ID,prior=1)
			   %last_best(MDCR_HICN_NUM,prior=1)
	  	       
	  	       
	  	 from base_demo_&year. c
		      %do p=1 %to %sysfunc(countw(&pyears.));
	 		      %let pyear=%scan(&pyears.,&p.);

		  	      left join
		  	      base_demo_&pyear. p&p.
		  	      
			  	 on c.submtg_state_cd = p&p..submtg_state_cd and
			  	    c.msis_ident_num = p&p..msis_ident_num  
			  %end;	
	  	
	  	) by tmsis_passthrough;

	%end; /* end getprior=1 loop */

	/* Now if we do NOT have prior year data, simply rename base_demo_YR to base_demo_out */

	%if &getprior.=0 %then %do;

		execute (
			alter table base_demo_&year. rename to base_demo_&year._out
		) by tmsis_passthrough;

	%end; 

	/* Join base_nondemo and base_demo.
	   For SSN_NUM only, add _TEMP suffix (for when join to claims IDs and set to 0-fill for all benes
	   in claims and not DE)*/

    execute (
		create temp table base_&year.
	    distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select a.*,
	  	       b.SSN_NUM as SSN_NUM_TEMP,
			   b.BIRTH_DT,
			   b.DEATH_DT,
			   b.DCSD_FLAG,
			   b.AGE_NUM,
			   b.AGE_GRP_FLAG,
			   b.GNDR_CD,
			   b.MRTL_STUS_CD,
			   b.INCM_CD,
			   b.VET_IND,
			   b.CTZNSHP_IND,
			   b.IMGRTN_STUS_5_YR_BAR_END_DT,
			   b.OTHR_LANG_HOME_CD,
			   b.PRMRY_LANG_FLAG,
			   b.PRMRY_LANG_ENGLSH_PRFCNCY_CD,
			   b.HSEHLD_SIZE_CD,
							                      
			   b.CRTFD_AMRCN_INDN_ALSKN_NTV_IND,
			   b.ETHNCTY_CD,
			   b.RACE_ETHNCTY_FLAG,
			   b.RACE_ETHNCTY_EXP_FLAG,
							                      
			   b.ELGBL_ZIP_CD,
			   b.ELGBL_CNTY_CD,
			   b.ELGBL_STATE_CD,
						 
			   b.MSIS_CASE_NUM,
			   b.MDCR_BENE_ID,
			   b.MDCR_HICN_NUM

		from base_nondemo_&year. a
		     inner join
		     base_demo_&year._out b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.msis_ident_num = b.msis_ident_num

	) by tmsis_passthrough;

	/* Drop temp tables no longer needed */

	%if &getprior.=1 %then %do;

		%drop_tables(base_demo_&year. %do p=1 %to %sysfunc(countw(&pyears.));
	 		      							%let pyear=%scan(&pyears.,&p.); 
                                            base_demo_&pyear.
									  %end; )

	%end;

		
	/* Join to the eligibility days segment and other _SPLMTL flags. Note not everyone is in
	   enrolled days (if only unknown enrollment) so must set to 0 if null */

	execute (
		create temp table base_&year._final0 
	    distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select a.*
			   %do m=1 %to 12;
					%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
					,coalesce(b.MDCD_ENRLMT_DAYS_&m.,0) as MDCD_ENRLMT_DAYS_&m.
					,coalesce(b.CHIP_ENRLMT_DAYS_&m.,0) as CHIP_ENRLMT_DAYS_&m.
				%end;
				,coalesce(b.MDCD_ENRLMT_DAYS_YR,0) as MDCD_ENRLMT_DAYS_YR
				,coalesce(b.CHIP_ENRLMT_DAYS_YR,0) as CHIP_ENRLMT_DAYS_YR
				,coalesce(b.EL_DTS_SPLMTL,0) as EL_DTS_SPLMTL
			  
			   ,c.MNGD_CARE_SPLMTL

			   ,d.WAIVER_SPLMTL

			   ,e.MFP_SPLMTL

			   ,f.HH_SPO_SPLMTL

			   ,g.HCBS_COND_SPLMTL
			   ,g.LCKIN_SPLMTL
			   ,g.LTSS_SPLMTL
			   ,g.OTHER_NEEDS_SPLMTL

		from base_&year. a
		     left join
			 enrolled_days_&year. b

			on a.submtg_state_cd = b.submtg_state_cd and
			   a.msis_ident_num = b.msis_ident_num

			inner join 
			MNGD_CARE_SPLMTL_&year. c

			on a.submtg_state_cd = c.submtg_state_cd and
			   a.msis_ident_num = c.msis_ident_num

			inner join
			WAIVER_SPLMTL_&year. d

			on a.submtg_state_cd = d.submtg_state_cd and
			   a.msis_ident_num = d.msis_ident_num

			inner join
			MFP_SPLMTL_&year. e

			on a.submtg_state_cd = e.submtg_state_cd and
			   a.msis_ident_num = e.msis_ident_num

			inner join
			HH_SPO_SPLMTL_&year. f

			on a.submtg_state_cd = f.submtg_state_cd and
			   a.msis_ident_num = f.msis_ident_num

			inner join 
			DIS_NEED_SPLMTLS_&year. g

			on a.submtg_state_cd = g.submtg_state_cd and
			   a.msis_ident_num = g.msis_ident_num

	) by tmsis_passthrough;

	/* Drop temp tables no longer needed before creating a temp table of all IDs from claims */

	%drop_tables(base_&year. enrolled_days_&year. MNGD_CARE_SPLMTL_&year. WAIVER_SPLMTL_&year. MFP_SPLMTL_&year.
	             HH_SPO_SPLMTL_&year. DIS_NEED_SPLMTLS_&year.)

	/* Create a table of all unique state/MSIS IDs from claims, to join back to Base and create dummy records for
	   all benes with a claim and not in Base */

	execute (
		create temp table claims_ids as
		select distinct submtg_state_cd
		                ,msis_ident_num

		from (%unique_claims_ids(IP) 

		     union
			 %unique_claims_ids(LT)

			 union
			 %unique_claims_ids(OT)

			 union
			 %unique_claims_ids(RX))

	) by tmsis_passthrough;

	/* Now join claim IDs to Base table. For those IDs not in Base, assign dummy rec and populate SSN_NUM with 0s.
	   Also create combined version of state and ID which will be output */

	execute (
		create temp table base_&year._final as

		select a.*
		       ,coalesce(a.submtg_state_cd,b.submtg_state_cd) as submtg_state_cd_comb
			   ,coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num_comb
		       ,case when a.submtg_state_cd is null then '000000000'
			         else SSN_NUM_TEMP
					 end as SSN_NUM

			   ,case when a.submtg_state_cd is null 
			         then 1 else 0
					 end as MISG_ELGBLTY_DATA_IND


		from base_&year._final0 a
		     full join
			 claims_ids b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.msis_ident_num = b.msis_ident_num


	) by tmsis_passthrough;


	/* Insert into permanent table */

	/* For the Base file, because we need to add cols after REC_ADD_TS and REC_UPDT_TS, must put all col names into a macro to
	   be included twice below (in parens below table name, and then in actual select) */

	%macro basecols;
 
			,SSN_NUM
			,BIRTH_DT
			,DEATH_DT
			,DCSD_FLAG
			,AGE_NUM
			,AGE_GRP_FLAG
			,GNDR_CD
			,MRTL_STUS_CD
			,INCM_CD
			,VET_IND
			,CTZNSHP_IND
			,CTZNSHP_VRFCTN_IND
			,IMGRTN_STUS_CD
			,IMGRTN_VRFCTN_IND
			,IMGRTN_STUS_5_YR_BAR_END_DT
			,OTHR_LANG_HOME_CD
			,PRMRY_LANG_FLAG
			,PRMRY_LANG_ENGLSH_PRFCNCY_CD
			,HSEHLD_SIZE_CD
			,PRGNCY_FLAG_01
			,PRGNCY_FLAG_02
			,PRGNCY_FLAG_03
			,PRGNCY_FLAG_04
			,PRGNCY_FLAG_05
			,PRGNCY_FLAG_06
			,PRGNCY_FLAG_07
			,PRGNCY_FLAG_08
			,PRGNCY_FLAG_09
			,PRGNCY_FLAG_10
			,PRGNCY_FLAG_11
			,PRGNCY_FLAG_12
			,PRGNCY_FLAG_EVR
			,CRTFD_AMRCN_INDN_ALSKN_NTV_IND
			,ETHNCTY_CD
			,RACE_ETHNCTY_FLAG
			,RACE_ETHNCTY_EXP_FLAG
			,ELGBL_ZIP_CD
			,ELGBL_CNTY_CD
			,ELGBL_STATE_CD
			,ELGBLTY_GRP_CD_01
			,ELGBLTY_GRP_CD_02
			,ELGBLTY_GRP_CD_03
			,ELGBLTY_GRP_CD_04
			,ELGBLTY_GRP_CD_05
			,ELGBLTY_GRP_CD_06
			,ELGBLTY_GRP_CD_07
			,ELGBLTY_GRP_CD_08
			,ELGBLTY_GRP_CD_09
			,ELGBLTY_GRP_CD_10
			,ELGBLTY_GRP_CD_11
			,ELGBLTY_GRP_CD_12
			,ELGBLTY_GRP_CD_LTST
			,MASBOE_CD_01
			,MASBOE_CD_02
			,MASBOE_CD_03
			,MASBOE_CD_04
			,MASBOE_CD_05
			,MASBOE_CD_06
			,MASBOE_CD_07
			,MASBOE_CD_08
			,MASBOE_CD_09
			,MASBOE_CD_10
			,MASBOE_CD_11
			,MASBOE_CD_12
			,MASBOE_CD_LTST
			,CARE_LVL_STUS_CD
			,DEAF_DSBL_FLAG_EVR
			,BLND_DSBL_FLAG_EVR
			,DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR
			,DFCLTY_WLKG_DSBL_FLAG_EVR
			,DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR
			,DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR
			,OTHR_DSBL_FLAG_EVR
			,MSIS_CASE_NUM
			,MDCD_ENRLMT_DAYS_01
			,MDCD_ENRLMT_DAYS_02
			,MDCD_ENRLMT_DAYS_03
			,MDCD_ENRLMT_DAYS_04
			,MDCD_ENRLMT_DAYS_05
			,MDCD_ENRLMT_DAYS_06
			,MDCD_ENRLMT_DAYS_07
			,MDCD_ENRLMT_DAYS_08
			,MDCD_ENRLMT_DAYS_09
			,MDCD_ENRLMT_DAYS_10
			,MDCD_ENRLMT_DAYS_11
			,MDCD_ENRLMT_DAYS_12
			,MDCD_ENRLMT_DAYS_YR
			,CHIP_ENRLMT_DAYS_01
			,CHIP_ENRLMT_DAYS_02
			,CHIP_ENRLMT_DAYS_03
			,CHIP_ENRLMT_DAYS_04
			,CHIP_ENRLMT_DAYS_05
			,CHIP_ENRLMT_DAYS_06
			,CHIP_ENRLMT_DAYS_07
			,CHIP_ENRLMT_DAYS_08
			,CHIP_ENRLMT_DAYS_09
			,CHIP_ENRLMT_DAYS_10
			,CHIP_ENRLMT_DAYS_11
			,CHIP_ENRLMT_DAYS_12
			,CHIP_ENRLMT_DAYS_YR
			,CHIP_CD_01
			,CHIP_CD_02
			,CHIP_CD_03
			,CHIP_CD_04
			,CHIP_CD_05
			,CHIP_CD_06
			,CHIP_CD_07
			,CHIP_CD_08
			,CHIP_CD_09
			,CHIP_CD_10
			,CHIP_CD_11
			,CHIP_CD_12
			,CHIP_CD_LTST
			,MDCR_BENE_ID
			,MDCR_HICN_NUM
			,STATE_SPEC_ELGBLTY_GRP_01 
			,STATE_SPEC_ELGBLTY_GRP_02
			,STATE_SPEC_ELGBLTY_GRP_03
			,STATE_SPEC_ELGBLTY_GRP_04
			,STATE_SPEC_ELGBLTY_GRP_05
			,STATE_SPEC_ELGBLTY_GRP_06
			,STATE_SPEC_ELGBLTY_GRP_07
			,STATE_SPEC_ELGBLTY_GRP_08
			,STATE_SPEC_ELGBLTY_GRP_09
			,STATE_SPEC_ELGBLTY_GRP_10
			,STATE_SPEC_ELGBLTY_GRP_11
			,STATE_SPEC_ELGBLTY_GRP_12
			,STATE_SPEC_ELGBLTY_GRP_LTST
			,DUAL_ELGBL_CD_01
			,DUAL_ELGBL_CD_02
			,DUAL_ELGBL_CD_03
			,DUAL_ELGBL_CD_04
			,DUAL_ELGBL_CD_05
			,DUAL_ELGBL_CD_06
			,DUAL_ELGBL_CD_07
			,DUAL_ELGBL_CD_08
			,DUAL_ELGBL_CD_09
			,DUAL_ELGBL_CD_10
			,DUAL_ELGBL_CD_11
			,DUAL_ELGBL_CD_12
			,DUAL_ELGBL_CD_LTST
			,MC_PLAN_TYPE_CD_01
			,MC_PLAN_TYPE_CD_02
			,MC_PLAN_TYPE_CD_03
			,MC_PLAN_TYPE_CD_04
			,MC_PLAN_TYPE_CD_05
			,MC_PLAN_TYPE_CD_06
			,MC_PLAN_TYPE_CD_07
			,MC_PLAN_TYPE_CD_08
			,MC_PLAN_TYPE_CD_09
			,MC_PLAN_TYPE_CD_10
			,MC_PLAN_TYPE_CD_11
			,MC_PLAN_TYPE_CD_12
			,RSTRCTD_BNFTS_CD_01
			,RSTRCTD_BNFTS_CD_02
			,RSTRCTD_BNFTS_CD_03
			,RSTRCTD_BNFTS_CD_04
			,RSTRCTD_BNFTS_CD_05
			,RSTRCTD_BNFTS_CD_06
			,RSTRCTD_BNFTS_CD_07
			,RSTRCTD_BNFTS_CD_08
			,RSTRCTD_BNFTS_CD_09
			,RSTRCTD_BNFTS_CD_10
			,RSTRCTD_BNFTS_CD_11
			,RSTRCTD_BNFTS_CD_12
			,RSTRCTD_BNFTS_CD_LTST
			,SSDI_IND
			,SSI_IND
			,SSI_STATE_SPLMT_STUS_CD
			,SSI_STUS_CD
			,BIRTH_CNCPTN_IND
			,TANF_CASH_CD
			,TPL_INSRNC_CVRG_IND
			,TPL_OTHR_CVRG_IND
			,EL_DTS_SPLMTL
			,MNGD_CARE_SPLMTL
			,HCBS_COND_SPLMTL
			,LCKIN_SPLMTL
			,LTSS_SPLMTL
			,MFP_SPLMTL
			,HH_SPO_SPLMTL
            ,OTHER_NEEDS_SPLMTL
			,WAIVER_SPLMTL		
			,MISG_ENRLMT_TYPE_IND_01
			,MISG_ENRLMT_TYPE_IND_02
			,MISG_ENRLMT_TYPE_IND_03
			,MISG_ENRLMT_TYPE_IND_04
			,MISG_ENRLMT_TYPE_IND_05
			,MISG_ENRLMT_TYPE_IND_06
			,MISG_ENRLMT_TYPE_IND_07
			,MISG_ENRLMT_TYPE_IND_08
			,MISG_ENRLMT_TYPE_IND_09
			,MISG_ENRLMT_TYPE_IND_10
			,MISG_ENRLMT_TYPE_IND_11
			,MISG_ENRLMT_TYPE_IND_12
			,MISG_ELGBLTY_DATA_IND 

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		(DA_RUN_ID, DE_LINK_KEY, DE_FIL_DT, ANN_DE_VRSN, SUBMTG_STATE_CD, MSIS_IDENT_NUM %basecols)
		select 
		    %table_id_cols(suffix=_comb)
		    %basecols

		from base_&year._final

	) by tmsis_passthrough; 


%mend create_BASE;

** ========================================================================== 
** program documentation 
** program     : 001_base_pr.sas
** description : Generate the annual PR segment 001: Base
** date        : 10/2019 12/2020
** note        : This program creates all the columns for the base file. 
**               It takes the last best value or ever in the year value depending on element and
**               includes one or two NPI values if available.
**               Finally, it pulls in _SPLMTL flags.
**               It then inserts into the permanent table.
** ==========================================================================;

%macro create_BASE;

	/* Create the partial base segment, pulling in only columns are not acceditation */
	
	%create_temp_table(fileseg=PRV, tblname=base_pr,
		          subcols=%nrbquote( 
									%last_best(REG_FLAG)
									%last_best(PRVDR_DBA_NAME)
									%last_best(PRVDR_LGL_NAME)
									%last_best(PRVDR_ORG_NAME)
									%last_best(PRVDR_TAX_NAME)
									%last_best(FAC_GRP_INDVDL_CD)
									%monthly_array(PRVDR_1ST_NAME)
									%monthly_array(PRVDR_MDL_INITL_NAME)
									%monthly_array(PRVDR_LAST_NAME)
									%monthly_array(GNDR_CD)
									%monthly_array(BIRTH_DT)
									%monthly_array(DEATH_DT)
									%monthly_array(AGE_NUM)
									%nonmiss_month(FAC_GRP_INDVDL_CD)
									%ind_nonmiss_month
									%last_best(OWNRSHP_CD)
									%last_best(OWNRSHP_CAT)
									%last_best(PRVDR_PRFT_STUS_CD)
									%ever_year(PRVDR_MDCD_ENRLMT_IND)
									%ever_year(MDCD_ENRLMT_IND)
									%ever_year(CHIP_ENRLMT_IND)
									%ever_year(MDCD_CHIP_ENRLMT_IND)
									%ever_year(NOT_SP_AFLTD_IND)
									%ever_year(PRVDR_ENRLMT_STUS_ACTV_IND)
									%ever_year(PRVDR_ENRLMT_STUS_DND_IND)
									%ever_year(PRVDR_ENRLMT_STUS_TRMNTD_IND)
									%ever_year(PRVDR_ENRLMT_STUS_PENDG_IND)
									%ever_year(MLT_SNGL_SPCLTY_GRP_IND)
									%ever_year(ALPTHC_OSTPTHC_PHYSN_IND)
									%ever_year(BHVRL_HLTH_SCL_SRVC_PRVDR_IND)
									%ever_year(CHRPRCTIC_PRVDR_IND)
									%ever_year(DNTL_PRVDR_IND)
									%ever_year(DTRY_NTRTNL_SRVC_PRVDR_IND)
									%ever_year(EMER_MDCL_SRVC_PRVDR_IND)
									%ever_year(EYE_VSN_SRVC_PRVDR_IND)
									%ever_year(NRSNG_SRVC_PRVDR_IND)
									%ever_year(NRSNG_SRVC_RLTD_IND)
									%ever_year(OTHR_INDVDL_SRVC_PRVDR_IND)
									%ever_year(PHRMCY_SRVC_PRVDR_IND)
									%ever_year(PA_ADVCD_PRCTC_NRSNG_PRVDR_IND)
									%ever_year(POD_MDCN_SRGRY_SRVCS_IND)
									%ever_year(RESP_DEV_REH_RESTOR_PRVDR_IND)
									%ever_year(SPCH_LANG_HEARG_SRVC_PRVDR_IND)
									%ever_year(STDNT_HLTH_CARE_PRVDR_IND)
									%ever_year(TT_OTHR_TCHNCL_SRVC_PRVDR_IND)
									%ever_year(AGNCY_PRVDR_IND)
									%ever_year(AMB_HLTH_CARE_FAC_PRVDR_IND)
									%ever_year(HOSP_UNIT_PRVDR_IND)
									%ever_year(HOSP_PRVDR_IND)
									%ever_year(LAB_PRVDR_IND)
									%ever_year(MCO_PRVDR_IND)
									%ever_year(NRSNG_CSTDL_CARE_FAC_IND)
									%ever_year(OTHR_NONINDVDL_SRVC_PRVDRS_IND)
									%ever_year(RSDNTL_TRTMT_FAC_PRVDR_IND)
									%ever_year(RESP_CARE_FAC_PRVDR_IND)
									%ever_year(SUPLR_PRVDR_IND)
									%ever_year(TRNSPRTN_SRVCS_PRVDR_IND)
									%ever_year(SUD_SRVC_PRVDR_IND)
									%ever_year(MH_SRVC_PRVDR_IND)
									%ever_year(EMER_SRVCS_PRVDR_IND)
									%ever_year(TCHNG_IND)
									%ever_year(ACPT_NEW_PTNTS_IND)
									%any_month(SUBMTG_STATE_PRVDR_ID,PRVDR_FLAG,condition=%nrstr(is not null))
									 ),
	              outercols=%nrbquote(  
									 %assign_nonmiss_month(PRVDR_1ST_NAME,FAC_GRP_INDVDL_CD_MN,PRVDR_1ST_NAME,monthval2=ind_any_MN,incol2=PRVDR_1ST_NAME)
									 %assign_nonmiss_month(PRVDR_MDL_INITL_NAME,FAC_GRP_INDVDL_CD_MN,PRVDR_MDL_INITL_NAME,monthval2=ind_any_MN,incol2=PRVDR_MDL_INITL_NAME)
									 %assign_nonmiss_month(PRVDR_LAST_NAME,FAC_GRP_INDVDL_CD_MN,PRVDR_LAST_NAME,monthval2=ind_any_MN,incol2=PRVDR_LAST_NAME)
									 %assign_nonmiss_month(GNDR_CD,FAC_GRP_INDVDL_CD_MN,GNDR_CD,monthval2=ind_any_MN,incol2=GNDR_CD)
									 %assign_nonmiss_month(BIRTH_DT,FAC_GRP_INDVDL_CD_MN,BIRTH_DT,monthval2=ind_any_MN,incol2=BIRTH_DT)
									 %assign_nonmiss_month(DEATH_DT,FAC_GRP_INDVDL_CD_MN,DEATH_DT,monthval2=ind_any_MN,incol2=DEATH_DT)
									 %assign_nonmiss_month(AGE_NUM,FAC_GRP_INDVDL_CD_MN,AGE_NUM,monthval2=ind_any_MN,incol2=AGE_NUM)
									) );
									 
	/* Join to the monthly _SPLMTL flags. */

	execute (
		create temp table base_&year._final
	    distkey(SUBMTG_STATE_PRVDR_ID) 
		sortkey(SUBMTG_STATE_CD,SUBMTG_STATE_PRVDR_ID) as

		select a.*
			   ,d2.PRVDR_NPI_01
			   ,d2.PRVDR_NPI_02
			   ,d2.PRVDR_NPI_CNT
			   ,case when b.LCTN_SPLMTL_CT>0 then 1 else 0 end :: smallint as LCTN_SPLMTL
			   ,case when c.LCNS_SPLMTL_CT>0 then 1 else 0 end :: smallint as LCNS_SPLMTL
			   ,case when d.ID_SPLMTL_CT>0 then 1 else 0 end :: smallint as ID_SPLMTL
			   ,case when e.GRP_SPLMTL_CT>0 then 1 else 0 end :: smallint as GRP_SPLMTL
			   ,case when f.PGM_SPLMTL_CT>0 then 1 else 0 end :: smallint as PGM_SPLMTL
			   ,case when g.TXNMY_SPLMTL_CT>0 then 1 else 0 end :: smallint as TXNMY_SPLMTL
			   ,case when h.ENRLMT_SPLMTL_CT>0 then 1 else 0 end :: smallint as ENRLMT_SPLMTL
			   ,case when i.BED_SPLMTL_CT>0 then 1 else 0 end :: smallint as BED_SPLMTL

		from base_pr_&year. a
		    left join
			LCTN_SPLMTL_&year. b on a.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = b.SUBMTG_STATE_PRVDR_ID
			left join 
			LCNS_SPLMTL_&year. c on a.SUBMTG_STATE_CD = c.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = c.SUBMTG_STATE_PRVDR_ID
			left join
			ID_SPLMTL_&year. d on a.SUBMTG_STATE_CD = d.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = d.SUBMTG_STATE_PRVDR_ID
			left join
			npi_final d2 on a.SUBMTG_STATE_CD = d2.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = d2.SUBMTG_STATE_PRVDR_ID
			left join
			GRP_SPLMTL_&year. e on a.SUBMTG_STATE_CD = e.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = e.SUBMTG_STATE_PRVDR_ID
			left join 
			PGM_SPLMTL_&year. f on a.SUBMTG_STATE_CD = f.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = f.SUBMTG_STATE_PRVDR_ID
			left join 
			TXNMY_SPLMTL_&year. g on a.SUBMTG_STATE_CD = g.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = g.SUBMTG_STATE_PRVDR_ID
			left join 
			ENRLMT_SPLMTL_&year. h on a.SUBMTG_STATE_CD = h.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = h.SUBMTG_STATE_PRVDR_ID
			left join 
			BED_SPLMTL_&year. i on a.SUBMTG_STATE_CD = i.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = i.SUBMTG_STATE_PRVDR_ID

			order by SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID;

	) by tmsis_passthrough;

	/* Drop temp tables no longer needed */

	%drop_tables(base_pr_&year. LCTN_SPLMTL_&year. LCNS_SPLMTL_&year. ID_SPLMTL_&year. npi_final GRP_SPLMTL_&year. PGM_SPLMTL_&year. TXNMY_SPLMTL_&year. ENRLMT_SPLMTL_&year. BED_SPLMTL_&year.)

	/* Insert into permanent table */

	%macro basecols;
	
			,REG_FLAG
			,PRVDR_DBA_NAME
			,PRVDR_LGL_NAME
			,PRVDR_ORG_NAME
			,PRVDR_TAX_NAME
			,FAC_GRP_INDVDL_CD
			,PRVDR_1ST_NAME
			,PRVDR_MDL_INITL_NAME
			,PRVDR_LAST_NAME
			,GNDR_CD
			,BIRTH_DT
			,DEATH_DT
			,AGE_NUM
			,TCHNG_IND
			,OWNRSHP_CD
			,OWNRSHP_CAT
			,PRVDR_PRFT_STUS_CD
			,ACPT_NEW_PTNTS_IND
			,PRVDR_MDCD_ENRLMT_IND
			,MDCD_ENRLMT_IND
			,CHIP_ENRLMT_IND
			,MDCD_CHIP_ENRLMT_IND
			,NOT_SP_AFLTD_IND
			,PRVDR_ENRLMT_STUS_ACTV_IND
			,PRVDR_ENRLMT_STUS_DND_IND
			,PRVDR_ENRLMT_STUS_TRMNTD_IND
			,PRVDR_ENRLMT_STUS_PENDG_IND
			,MLT_SNGL_SPCLTY_GRP_IND
			,ALPTHC_OSTPTHC_PHYSN_IND
			,BHVRL_HLTH_SCL_SRVC_PRVDR_IND
			,CHRPRCTIC_PRVDR_IND
			,DNTL_PRVDR_IND
			,DTRY_NTRTNL_SRVC_PRVDR_IND
			,EMER_MDCL_SRVC_PRVDR_IND
			,EYE_VSN_SRVC_PRVDR_IND
			,NRSNG_SRVC_PRVDR_IND
			,NRSNG_SRVC_RLTD_IND
			,OTHR_INDVDL_SRVC_PRVDR_IND
			,PHRMCY_SRVC_PRVDR_IND
			,PA_ADVCD_PRCTC_NRSNG_PRVDR_IND
			,POD_MDCN_SRGRY_SRVCS_IND
			,RESP_DEV_REH_RESTOR_PRVDR_IND
			,SPCH_LANG_HEARG_SRVC_PRVDR_IND
			,STDNT_HLTH_CARE_PRVDR_IND
			,TT_OTHR_TCHNCL_SRVC_PRVDR_IND
			,AGNCY_PRVDR_IND
			,AMB_HLTH_CARE_FAC_PRVDR_IND
			,HOSP_UNIT_PRVDR_IND
			,HOSP_PRVDR_IND
			,LAB_PRVDR_IND
			,MCO_PRVDR_IND
			,NRSNG_CSTDL_CARE_FAC_IND
			,OTHR_NONINDVDL_SRVC_PRVDRS_IND
			,RSDNTL_TRTMT_FAC_PRVDR_IND
			,RESP_CARE_FAC_PRVDR_IND
			,SUPLR_PRVDR_IND
			,TRNSPRTN_SRVCS_PRVDR_IND
			,SUD_SRVC_PRVDR_IND
			,MH_SRVC_PRVDR_IND
			,EMER_SRVCS_PRVDR_IND
			,PRVDR_NPI_01
			,PRVDR_NPI_02
			,PRVDR_NPI_CNT
			,PRVDR_FLAG_01
			,PRVDR_FLAG_02
			,PRVDR_FLAG_03
			,PRVDR_FLAG_04
			,PRVDR_FLAG_05
			,PRVDR_FLAG_06
			,PRVDR_FLAG_07
			,PRVDR_FLAG_08
			,PRVDR_FLAG_09
			,PRVDR_FLAG_10
			,PRVDR_FLAG_11
			,PRVDR_FLAG_12
			,LCTN_SPLMTL
			,LCNS_SPLMTL
			,ID_SPLMTL
			,GRP_SPLMTL
			,PGM_SPLMTL
			,TXNMY_SPLMTL
			,ENRLMT_SPLMTL
			,BED_SPLMTL

	%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_&tblname.
		(DA_RUN_ID, PR_LINK_KEY, PR_FIL_DT, PR_VRSN, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID %basecols)
		select

		    %table_id_cols
		    %basecols

		from base_&year._final

	) by tmsis_passthrough; 

	/* Delete temp tables */

	%drop_tables(base_&year._final)          

%mend create_BASE;

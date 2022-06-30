/************************************************************************************************************/
/* Program:		AWS_OT_Macros.sas																			*/
/* Author:		Deo S. Bencio																				*/
/* Date:		12/1/2016																					*/
/* Purpose:		Program contains macros that massage data from various segments in T-MSIS					*/
/*				THIS PROGRAM DOES NOT STAND ALONE.                                                      	*/
/*				OT_build.sas - pull program for OT build													*/
/*																											*/
/* Modified:	4/2/2018  - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q1.xlsx            */
/*				10/4/2018 - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q3					*/
/*				3/7/2019  - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q1.xlsx			*/
/*							Added column LINE_NUM to identify line numbers in LINE table					*/
/*							Renamed OTHR_TOC_RX_CLM_ACTL_QTY to ACTL_SRVC_QTY and                           */
/*                                   OTHR_TOC_RX_CLM_ALOWD_QTY to ALOWD_SRVC_QTY                            */
/*							Remove dots (.) and trailing spaces from diagnosis codes						*/
/*				9/19/2019 - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q3                 */
/*							Upcased ICN ORIG and ICN ADJSTMT at the FA Header/Line Join						*/
/*				6/9/2020  - DB modified to apply TAF CCB 2020 Q2 Change Request                             */
/*              12/15/2020- DB modified to apply TAF CCB 2020 Q4 Change Request                             */
/*							-MACTAF-1583: Recode Tot_bill_amt value 9999999999.99 to Null                   */
/*							-MACTAF-1613: Exclude IA CHIP T-MSIS files from TAF Production					*/
/* 				11/08/2021- DB modified to add FASC to TAF                                                  */
/* 							-MACTAF-1821: New federally assigned TOS variable - all claims                  */
/*				12/06/2021- DB modified to add variable dgns_1_ccsr_dflt_ctgry_cd							*/
/*							-MACTAF-1802: Add default CCSR dx cat(ip lt ot) for primary dx code				*/
/*							-MACTAF-1803: Add CCS category for CPT/HCPCS codes to ot claim lines			*/
/*				05/03/2022- DB modified For V7.1															*/
/*							-MACTAF-1966, MACTAF-1943														*/	
/*								1. Increase width of XIX-MBESCBES-CATEGORY-OF-SERVICE from X(4) to X(5)	    */
/*							-MACTAF-1949- Rename data elements 												*/
/*							 COPAY-AMT: (LINE LEVEL) COPAY_AMT -> BENE_COPMT_PD_AMT
/*							 BENEFICIARY-COPAYMENT-AMOUNT: (HEADER LEVEL) BENE_COPMT_AMT -> TOT_BENE_COPMT_PD_AMT
/*							 BENEFICIARY-COINSURANCE-AMOUNT: (HEADER LEVEL) BENE_COINSRNC_AMT -> TOT_BENE_COINSRNC_PD_AMT
/*							 BENEFICIARY-DEDUCTIBLE-AMOUNT: (HEADER LEVEL) BENE_DDCTBL_AMT -> TOT_BENE_DDCTBL_PD_AMT
/*							-MACTAF-1951 - Modify data element width and rename								*/
/*							 OT-RX-CLAIM-QUANTITY-ACTUAL:  OTHR_TOC_RX_CLM_ACTL_QTY ->	SVC_QTY_ACTL
/*							 OT-RX-CLAIM-QUANTITY-ALLOWED: OTHR_TOC_RX_CLM_ALOWD_QTY -> SVC_QTY_ALOWD
/*             note to DB: MODS COMPLETE ON THIS ONE 5/4/2022
/************************************************************************************************************/
options SASTRACE=',,,ds' SASTRACELOC=Saslog nostsuffix dbidirectexec sqlgeneration=dbms msglevel=I sql_ip_trace=source;

options spool;


/* pull OT line item records for header records linked with claims family table dataset */
%macro AWS_Extract_Line_OT (TMSIS_SCHEMA, fl2, fl, tab_no, _2x_segment, analysis_date);

	/** Create a temporary line file **/
execute (

		create temp table &FL2._LINE_IN
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (SUBMTG_STATE_CD,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
		as
		select  SUBMTG_STATE_CD, %&tab_no

		from	&TMSIS_SCHEMA..&_2x_segment  A

		where  A.TMSIS_ACTV_IND = 1 							/* include active indicator = 1 */
			   and a.submtg_state_cd =%nrbquote('&state_id')
			   and a.tmsis_run_id = &RUN_ID 

	) by tmsis_passthrough;

	execute (

		create temp table &FL2._LINE_PRE_NPPES
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (PRSCRBNG_PRVDR_NPI_NUM)

		as

		select A.*,
        row_number() over (partition by A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM_LINE,A.ADJSTMT_CLM_NUM_LINE,A.ADJDCTN_DT_LINE,A.LINE_ADJSTMT_IND 
		order by A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM_LINE,A.ADJSTMT_CLM_NUM_LINE,A.ADJDCTN_DT_LINE,A.LINE_ADJSTMT_IND,A.TMSIS_FIL_NAME,A.REC_NUM ) as RN  

			,a.submtg_state_cd as new_submtg_state_cd_line
		from	&FL2._LINE_IN as A inner join FA_HDR_&FL. H

		on   	H.TMSIS_RUN_ID = A.TMSIS_RUN_ID_LINE and
		H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD and
	    H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM_LINE and
		H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM_LINE and
		H.ADJDCTN_DT = A.ADJDCTN_DT_LINE and
		H.ADJSTMT_IND = A.LINE_ADJSTMT_IND

	) by tmsis_passthrough;

	/* Join line file with NPPES to pick up servicing provider nppes taxonomy code */
	execute (

		create temp table &FL2._LINE
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)

		as

		select  A.*
 		 		,%var_set_taxo(SELECTED_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY, NEW=SRVCNG_PRVDR_NPPES_TXNMY_CD)
				,ccs.ccs as prcdr_ccs_ctgry_cd

		from	&FL2._LINE_PRE_NPPES as A
			    left join
			    nppes_npi nppes
		on nppes.prvdr_npi=a.PRSCRBNG_PRVDR_NPI_NUM  	/* misnomer on OT input */

				left join
				ccs_proc ccs
		on ccs.cd_rng=a.prcdr_cd

	) by tmsis_passthrough;

	execute (

	create temp table RN_&FL2.
	distkey (ORGNL_CLM_NUM_LINE) 
	sortkey(NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
	as
	
	select	NEW_SUBMTG_STATE_CD_LINE
			, ORGNL_CLM_NUM_LINE
			, ADJSTMT_CLM_NUM_LINE
			, ADJDCTN_DT_LINE
			, LINE_ADJSTMT_IND
			, max(RN) as NUM_CLL

	from	&FL2._LINE

	group by NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND

	) by tmsis_passthrough;


	/* Attach num_cll variable to header records as per instruction */
	execute (

	create temp table &fl2._HEADER
	distkey (ORGNL_CLM_NUM) 
	sortkey (NEW_SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
	as
	
	select  HEADER.*
			, coalesce(RN.NUM_CLL,0) as NUM_CLL

	from 	FA_HDR_&FL. HEADER left join RN_&FL2. RN							

  	on   	HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
		 	HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and 
		 	HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
			HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
			HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND

	) by tmsis_passthrough;

%drop_temp_tables(FA_HDR_&FL.);
%Get_Audt_counts_clms(&DA_SCHEMA.,&DA_RUN_ID., AWS_OT_Macros, 1.1 AWS_Extract_Line_OT);
%drop_temp_tables(RN_&fl2.);
%DROP_temp_tables(&FL2._LINE_IN);
%mend AWS_Extract_Line_OT;

 
%MACRO BUILD_OT();

	/* ORDER VARIABLES AND UPCASE, LEFT PAD WITH ZEROS AND RESET COALESCE VALUES HEADER FILE*/
	execute (

	create temp table OTH
	distkey (ORGNL_CLM_NUM)
	as

	select &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT) AS CHAR(4))|| 
	CAST(DATE_PART(MONTH,ADJDCTN_DT) AS CHAR(2))|| 
    CAST(DATE_PART(DAY,ADJDCTN_DT) AS CHAR(2)) || '-' || COALESCE(ADJSTMT_IND_CLEAN,'X')) 
	as varchar(126)) as OT_LINK_KEY 
	,%nrbquote('&VERSION.') as OT_VRSN
	,%nrbquote('&TAF_FILE_DATE.') as OT_FIL_DT
	,TMSIS_RUN_ID
	,%var_set_type1(MSIS_IDENT_NUM)
	,NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD 
	,%var_set_type3(orgnl_clm_num, cond1=~)
	,%var_set_type3(adjstmt_clm_num, cond1=~)
	,ADJSTMT_IND_CLEAN as ADJSTMT_IND
	,%var_set_rsn(ADJSTMT_RSN_CD)
    ,case when date_cmp(SRVC_BGNNG_DT_HEADER,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_BGNNG_DT_HEADER,'01JAN1960') end as SRVC_BGNNG_DT
	,nullif(SRVC_ENDG_DT_HEADER,'01JAN1960') as SRVC_ENDG_DT
	,case when date_cmp(ADJDCTN_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT,'01JAN1960') end as ADJDCTN_DT
    ,%fix_old_dates(MDCD_PD_DT)
	,%var_set_type2(SECT_1115A_DEMO_IND,0,cond1=0,cond2=1) 	
	,case when upper(clm_type_cd) in('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z') then upper(clm_type_cd)
	      else NULL
	 end as clm_type_cd
	,%var_set_type1(BILL_TYPE_CD)
	,case when lpad(pgm_type_cd,2,'0') in ('06','09') then NULL 	
	  	  else %var_set_type5(pgm_type_cd,lpad=2,lowerbound=0,upperbound=17,multiple_condition=YES)
	,%var_set_type1(MC_PLAN_ID)
	,%var_set_type1(ELGBL_LAST_NAME,upper=YES)                             
	,%var_set_type1(ELGBL_1ST_NAME,upper=YES)                              
	,%var_set_type1(ELGBL_MDL_INITL_NAME,upper=YES)                       
	,%fix_old_dates(BIRTH_DT)
	,case when lpad(wvr_type_cd,2,'0') = '88' then NULL 	
		  else %var_set_type5(wvr_type_cd,lpad=2,lowerbound=1,upperbound=33,multiple_condition=YES)
	,%var_set_type1(WVR_ID)
	,%var_set_type2(srvc_trkng_type_cd,2,cond1=00,cond2=01,cond3=02,cond4=03,cond5=04,cond6=05,cond7=06)
	,%var_set_type6(SRVC_TRKNG_PYMT_AMT, 	cond1=888888888.88)                   
	,%var_set_type2(OTHR_INSRNC_IND,0,cond1=0,cond2=1)
	,%var_set_type2(othr_tpl_clctn_cd,3,cond1=000,cond2=001,cond3=002,cond4=003,cond5=004,cond6=005,cond7=006,cond8=007)
	,%var_set_type2(FIXD_PYMT_IND,0,cond1=0,cond2=1)
	,%var_set_type4(FUNDNG_CD,YES,cond1=A,cond2=B,cond3=C,cond4=D,cond5=E,cond6=F,cond7=G,cond8=H,cond9=I)
	,%var_set_type2(fundng_src_non_fed_shr_cd,2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06)
	,%var_set_type2(BRDR_STATE_IND,0,cond1=0,cond2=1)
	,%var_set_type2(XOVR_IND,0,cond1=0,cond2=1)
	,%var_set_type1(MDCR_HICN_NUM)
	,%var_set_type1(MDCR_BENE_ID)
	,%var_set_type2(HLTH_CARE_ACQRD_COND_CD,0,cond1=0,cond2=1)
	,%var_set_fills(DGNS_1_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_1_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_1_CD_IND)
	,%var_set_fills(DGNS_2_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_2_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_2_CD_IND)
	,%var_set_type1(SRVC_PLC_CD,lpad=2)
	,%var_set_type1(PRVDR_LCTN_ID)
	,%var_set_type1(BLG_PRVDR_NUM)
	,%var_set_type1(BLG_PRVDR_NPI_NUM)
	,%var_set_taxo(BLG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_prtype(blg_prvdr_type_cd)
	,%var_set_spclty(BLG_PRVDR_SPCLTY_CD)
	,%var_set_type1(RFRG_PRVDR_NUM)
	,%var_set_type1(RFRG_PRVDR_NPI_NUM)
	,%var_set_taxo(RFRG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_prtype(rfrg_prvdr_type_cd)
	,%var_set_spclty(RFRG_PRVDR_SPCLTY_CD)
	,%var_set_type1(PRVDR_UNDER_DRCTN_NPI_NUM)
	,%var_set_taxo(PRVDR_UNDER_DRCTN_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_type1(PRVDR_UNDER_SPRVSN_NPI_NUM)
	,%var_set_taxo(PRVDR_UNDER_SPRVSN_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_type2(HH_PRVDR_IND,0,cond1=0,cond2=1)                           
	,%var_set_type1(HH_PRVDR_NPI_NUM)                       
	,%var_set_type1(HH_ENT_NAME)
	,%var_set_type1(RMTNC_NUM)
    ,%var_set_type6(DAILY_RATE,  cond1=88888, cond2=88888.80, cond3=88888.88)
	,%var_set_type2(PYMT_LVL_IND,0,cond1=1,cond2=2) 
	,%var_set_type6(TOT_BILL_AMT, 		cond1=999999.00, cond2=888888888.88, cond3=9999999.99, cond4=99999999.90, cond5=999999.99, cond6=9999999999.99)
	,%var_set_type6(TOT_ALOWD_AMT,		cond1=99999999, cond2=888888888.88)
	,%var_set_type6(TOT_MDCD_PD_AMT,	cond1=888888888.88)
	,%var_set_type6(TOT_COPAY_AMT, 		cond1=888888888.88, cond2=9999999.99, cond3=88888888888.00)
	,%var_set_type6(TOT_MDCR_DDCTBL_AMT, cond1=888888888.88, cond2=99999, cond3=88888888888.00)
	,%var_set_type6(TOT_MDCR_COINSRNC_AMT, cond1=888888888.88)
	,%var_set_type6(TOT_TPL_AMT,		cond1=888888888.88, cond2=999999.99)
	,%var_set_type6(TOT_OTHR_INSRNC_AMT,cond1=888888888.8, cond2=888888888.88)
	,%var_set_type6(TP_COINSRNC_PD_AMT,cond1=888888888.88)
	,%var_set_type6(TP_COPMT_PD_AMT,cond1=888888888.88, cond2=888888888, cond3=888888888.00, cond4=99999999999.00)
	,%var_set_type2(MDCR_CMBND_DDCTBL_IND,0,cond1=0,cond2=1)                                                     
	,%var_set_type2(mdcr_reimbrsmt_type_cd,2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06,cond7=07,cond8=08,cond9=09)
	,%var_set_type6(TOT_BENE_COINSRNC_PD_AMT,new=BENE_COINSRNC_AMT,cond1=888888888.88, cond2=888888888, cond3=88888888888.00)
	,%fix_old_dates(BENE_COINSRNC_PD_DT)                   
	,%var_set_type6(TOT_BENE_COPMT_PD_AMT,new=BENE_COPMT_AMT,   cond1=888888888.88, cond2=888888888, cond3=88888888888.00)
	,%fix_old_dates(BENE_COPMT_PD_DT)                           
	,%var_set_type6(TOT_BENE_DDCTBL_PD_AMT,new=BENE_DDCTBL_AMT,  cond1=888888888.88, cond2=888888888, cond3=88888888888.00)
	,%fix_old_dates(BENE_DDCTBL_PD_DT)                        
	,%var_set_type2(COPAY_WVD_IND,0,cond1=0,cond2=1)
	,%fix_old_dates(CPTATD_AMT_RQSTD_DT)  
	,%var_set_type6(CPTATD_PYMT_RQSTD_AMT,	cond1=888888888.88, cond2=888888888)
	,%var_set_type1(OCRNC_01_CD)
	,%var_set_type1(OCRNC_02_CD)
	,%var_set_type1(OCRNC_03_CD)
	,%var_set_type1(OCRNC_04_CD)
	,%var_set_type1(OCRNC_05_CD)
	,%var_set_type1(OCRNC_06_CD)
	,%var_set_type1(OCRNC_07_CD)
	,%var_set_type1(OCRNC_08_CD)
	,%var_set_type1(OCRNC_09_CD)
	,%var_set_type1(OCRNC_10_CD)
	,%fix_old_dates(OCRNC_01_CD_EFCTV_DT)                  
	,%fix_old_dates(OCRNC_02_CD_EFCTV_DT)                  
	,%fix_old_dates(OCRNC_03_CD_EFCTV_DT)                  
	,%fix_old_dates(OCRNC_04_CD_EFCTV_DT)                   
	,%fix_old_dates(OCRNC_05_CD_EFCTV_DT)                   
	,%fix_old_dates(OCRNC_06_CD_EFCTV_DT)                 
	,%fix_old_dates(OCRNC_07_CD_EFCTV_DT)                  
	,%fix_old_dates(OCRNC_08_CD_EFCTV_DT)                   
	,%fix_old_dates(OCRNC_09_CD_EFCTV_DT)                   
	,%fix_old_dates(OCRNC_10_CD_EFCTV_DT)                  
	,%fix_old_dates(OCRNC_01_CD_END_DT)                       
	,%fix_old_dates(OCRNC_02_CD_END_DT)                      
	,%fix_old_dates(OCRNC_03_CD_END_DT)                       
	,%fix_old_dates(OCRNC_04_CD_END_DT)                     
	,%fix_old_dates(OCRNC_05_CD_END_DT)                      
	,%fix_old_dates(OCRNC_06_CD_END_DT)                     
	,%fix_old_dates(OCRNC_07_CD_END_DT)                      
	,%fix_old_dates(OCRNC_08_CD_END_DT)                     
	,%fix_old_dates(OCRNC_09_CD_END_DT)                      
	,%fix_old_dates(OCRNC_10_CD_END_DT)                     
	,CLL_CNT 
	,NUM_CLL

	/* constructed variables */                          
	,OT_MH_DX_IND
	,OT_SUD_DX_IND
	,OT_MH_TAXONOMY_IND
	,OT_SUD_TAXONOMY_IND
	,cast(nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as char(6)) as IAP_COND_IND
	,cast(nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as char(9)) as PRMRY_HIRCHCL_COND
	,CONVERT_TIMEZONE('EDT', GETDATE()) as REC_ADD_TS
	,CONVERT_TIMEZONE('EDT', GETDATE()) as REC_UPDT_TS 
	,%fix_old_dates(SRVC_ENDG_DT_DRVD)
	,%var_set_type2(SRVC_ENDG_DT_CD,0,cond1=1,cond2=2,cond3=3,cond4=4,cond5=5)
	,%var_set_taxo(BLG_PRVDR_NPPES_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,DGNS_1_CCSR_DFLT_CTGRY_CD

	from 	(select *,
     case when ADJSTMT_IND is NOT NULL and    
               trim(ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(ADJSTMT_IND)     else NULL   end as ADJSTMT_IND_CLEAN 
     from &fl._HEADER_GROUPER) H

	) by tmsis_passthrough;

	%drop_temp_tables(&fl._HEADER_GROUPER);

	/* ORDER VARIABLES AND UPCASE, LEFT PAD WITH ZEROS AND RESET COALESCE VALUES LINE FILE*/
	execute (

	create temp table OTL
	distkey(ORGNL_CLM_NUM)
	as

	select &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD_LINE || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM_LINE,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM_LINE,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT_LINE) AS CHAR(4)) ||
	CAST(DATE_PART(MONTH,ADJDCTN_DT_LINE) AS CHAR(2)) ||
    CAST(DATE_PART(DAY,ADJDCTN_DT_LINE) AS CHAR(2)) || '-' || 
    COALESCE(LINE_ADJSTMT_IND_CLEAN,'X')) as varchar(126)) as OT_LINK_KEY 
	,%nrbquote('&VERSION.') as OT_VRSN
	,%nrbquote('&TAF_FILE_DATE.') as OT_FIL_DT
	,TMSIS_RUN_ID_LINE as TMSIS_RUN_ID
	,%var_set_type1(var=MSIS_IDENT_NUM_LINE,new=MSIS_IDENT_NUM)
	,NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD 
	,%var_set_type3(ORGNL_CLM_NUM_LINE,cond1=~,new=ORGNL_CLM_NUM)
	,%var_set_type3(ADJSTMT_CLM_NUM_LINE,cond1=~,new=ADJSTMT_CLM_NUM)
	,%var_set_type1(ORGNL_LINE_NUM)                           
	,%var_set_type1(ADJSTMT_LINE_NUM)  
	,case when date_cmp(ADJDCTN_DT_LINE,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT_LINE,'01JAN1960') end as ADJDCTN_DT	
	,LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND
	,%var_set_rsn(ADJSTMT_LINE_RSN_CD)
	,%var_set_type1(CLL_STUS_CD) 
	,case when date_cmp(SRVC_BGNNG_DT_LINE,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_BGNNG_DT_LINE,'01JAN1960') end as SRVC_BGNNG_DT
	,case when date_cmp(SRVC_ENDG_DT_LINE,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_ENDG_DT_LINE,'01JAN1960') end as SRVC_ENDG_DT
	,%var_set_type1(REV_CD,lpad=4)
	,%var_set_fillpr(PRCDR_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%fix_old_dates(PRCDR_CD_DT)
	,%var_set_proc(PRCDR_CD_IND)
	,%var_set_type1(PRCDR_1_MDFR_CD,lpad=2) 
	,case when lpad(IMNZTN_TYPE_CD,2,'0') = '88' then NULL 	
		  else %var_set_type5(IMNZTN_type_cd,lpad=2,lowerbound=0,upperbound=29,multiple_condition=YES)
	,%var_set_type6(BILL_AMT,	cond1=888888888.88, cond2=9999999999.99, cond3=999999.99, cond4=999999)
	,%var_set_type6(ALOWD_AMT,	cond1=99999999.00, cond2=888888888.88, cond3=9999999999.99)
	,%var_set_type6(BENE_COPMT_PD_AMT,new=COPAY_AMT,	cond1=88888888888.00, cond2=888888888.88)
	,%var_set_type6(TPL_AMT,	cond1=88888888.88) 
    ,%var_set_type6(MDCD_PD_AMT,	   cond1=888888888.88)
	,%var_set_type6(MDCD_FFS_EQUIV_AMT,cond1=88888888888.80, cond2=888888888.88, cond3=999999.99)
	,%var_set_type6(MDCR_PD_AMT,	   cond1=888888888.88, cond2=8888888.88, cond3=88888888888.00, cond4=99999999999.00, cond5=88888888888.88, cond6=9999999999.99)
    ,%var_set_type6(OTHR_INSRNC_AMT,   cond1=8888888888.00, cond2=888888888.88, cond3=88888888888.88)
    ,%var_set_type6(SVC_QTY_ACTL, new=ACTL_SRVC_QTY, cond1=999999.000, cond2=888888.000, cond3=999999.99)
    ,%var_set_type6(SVC_QTY_ALOWD, new=ALOWD_SRVC_QTY, cond1=999999.000, cond2=888888.000, cond3=888888.880, cond4=99999.999, cond5=99999)
    ,%var_set_tos(TOS_CD)
	,%var_set_type5(BNFT_TYPE_CD,lpad=3,lowerbound=001,upperbound=108)
	,%var_set_type2(HCBS_SRVC_CD,0,cond1=1,cond2=2,cond3=3,cond4=4,cond5=5,cond6=6,cond7=7)                                   
	,%var_set_type1(HCBS_TXNMY,lpad=5)                                    
	,%var_set_type1(SRVCNG_PRVDR_NUM)
	,%var_set_type1(PRSCRBNG_PRVDR_NPI_NUM,new=SRVCNG_PRVDR_NPI_NUM)
	,%var_set_taxo(SRVCNG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_prtype(SRVCNG_PRVDR_TYPE_CD)
	,%var_set_spclty(SRVCNG_PRVDR_SPCLTY_CD)
	,%var_set_type4(TOOTH_DSGNTN_SYS_CD,YES,cond1=JO,cond2=JP)                          
	,%var_set_type1(TOOTH_NUM)                                          
	,case when lpad(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,2,'0') in ('20','30','40') then lpad(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,2,'0') 
		  else %var_set_type5(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,lpad=2,lowerbound=0,upperbound=10,multiple_condition=YES)                  
	,%var_set_type4(TOOTH_SRFC_CD,YES,cond1=B,cond2=D,cond3=F,cond4=I,cond5=L,cond6=M,cond7=O)                                                                                                               
	,%var_set_type2(CMS_64_FED_REIMBRSMT_CTGRY_CD,2,cond1=01,cond2=02,cond3=03,cond4=04)
    ,case when XIX_SRVC_CTGRY_CD in &XIX_SRVC_CTGRY_CD_values. then XIX_SRVC_CTGRY_CD
    	else null end as XIX_SRVC_CTGRY_CD
    ,case when XXI_SRVC_CTGRY_CD in &XXI_SRVC_CTGRY_CD_values. then XXI_SRVC_CTGRY_CD
     	else null end as XXI_SRVC_CTGRY_CD
	,%var_set_type1(STATE_NOTN_TXT)
    ,%var_set_fills(NDC_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_type1(PRCDR_2_MDFR_CD,lpad=2) 
	,%var_set_type1(PRCDR_3_MDFR_CD,lpad=2) 
	,%var_set_type1(PRCDR_4_MDFR_CD,lpad=2) 
    ,%var_set_type3(HCPCS_RATE,cond1=0.0000000000,cond2=0.000000000000,spaces=NO)
	,%var_set_type2(SELF_DRCTN_TYPE_CD,3,cond1=000,cond2=001,cond3=002,cond4=003) 
	,%var_set_type1(PRE_AUTHRZTN_NUM)                              
	,%var_set_type4(UOM_CD,YES,cond1=F2,cond2=ML,cond3=GR,cond4=UN,cond5=ME)
    ,%var_set_type6(NDC_QTY, cond1=999999, cond2=999999.998, cond3=888888.000, cond4=888888.880, cond5=88888.888, cond6=888888.888)
	,RN as LINE_NUM
	,PRCDR_CCS_CTGRY_CD
    ,%var_set_taxo(SRVCNG_PRVDR_NPPES_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)

		FROM 	(select *,
     			 case when LINE_ADJSTMT_IND is NOT NULL and    
               trim(LINE_ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(LINE_ADJSTMT_IND)     else NULL   end  as LINE_ADJSTMT_IND_CLEAN 
     from &FL._LINE) H

	) by tmsis_passthrough;


	/* call program to calculate fed assigned service catg */
	%fasc_code(fl=ot);

	%drop_temp_tables(&fl._LINE);

   title ;
   EXECUTE(
    INSERT INTO &DA_SCHEMA..TAF_&FL.H	
		SELECT h.* 
			   ,fasc.fed_srvc_ctgry_cd 

		FROM &FL.H h
		
			 left join
			 &fl._hdr_rolled fasc

		ON   h.&fl._link_key=fasc.&fl._link_key
		
		) BY TMSIS_PASSTHROUGH;

   	select ht_ct into : header_ct_&STATE_ID
	from (select * from connection to tmsis_passthrough
          (select count(submtg_state_cd) as ht_ct
	      from &FL.H));

	%let HEADER_CT = %eval(&HEADER_CT+&&HEADER_CT_&STATE_ID);
    
   EXECUTE(
    INSERT INTO &DA_SCHEMA..TAF_&FL.L
		(DA_RUN_ID
		,OT_LINK_KEY 
		,OT_VRSN
		,OT_FIL_DT
		,TMSIS_RUN_ID
		,MSIS_IDENT_NUM
		,SUBMTG_STATE_CD 
		,ORGNL_CLM_NUM
		,ADJSTMT_CLM_NUM
		,ORGNL_LINE_NUM                           
		,ADJSTMT_LINE_NUM  
		,ADJDCTN_DT
		,LINE_ADJSTMT_IND
		,ADJSTMT_LINE_RSN_CD
		,CLL_STUS_CD
		,SRVC_BGNNG_DT
		,SRVC_ENDG_DT
		,REV_CD
		,PRCDR_CD
		,PRCDR_CD_DT                                       
		,PRCDR_CD_IND
		,PRCDR_1_MDFR_CD 
		,IMNZTN_TYPE_CD
		,BILL_AMT
		,ALOWD_AMT
		,COPAY_AMT
		,TPL_AMT
   		,MDCD_PD_AMT
		,MDCD_FFS_EQUIV_AMT
		,MDCR_PD_AMT
    	,OTHR_INSRNC_AMT
    	,ACTL_SRVC_QTY
    	,ALOWD_SRVC_QTY
    	,TOS_CD
		,BNFT_TYPE_CD
		,HCBS_SRVC_CD
		,HCBS_TXNMY
		,SRVCNG_PRVDR_NUM
		,SRVCNG_PRVDR_NPI_NUM
		,SRVCNG_PRVDR_TXNMY_CD
		,SRVCNG_PRVDR_TYPE_CD
		,SRVCNG_PRVDR_SPCLTY_CD
		,TOOTH_DSGNTN_SYS_CD
		,TOOTH_NUM                                          
		,TOOTH_ORAL_CVTY_AREA_DSGNTD_CD
		,TOOTH_SRFC_CD
		,CMS_64_FED_REIMBRSMT_CTGRY_CD
   	 	,XIX_SRVC_CTGRY_CD
   		,XXI_SRVC_CTGRY_CD
		,STATE_NOTN_TXT
    	,NDC_CD
		,PRCDR_2_MDFR_CD
		,PRCDR_3_MDFR_CD
		,PRCDR_4_MDFR_CD
    	,HCPCS_RATE
		,SELF_DRCTN_TYPE_CD
		,PRE_AUTHRZTN_NUM
		,UOM_CD
    	,NDC_QTY
		,LINE_NUM
		,PRCDR_CCS_CTGRY_CD
    	,SRVCNG_PRVDR_NPPES_TXNMY_CD
		)	
	SELECT * 
	FROM &FL.L
   ) BY TMSIS_PASSTHROUGH;

	select line_ct into : line_ct_&STATE_ID
	from (select * from connection to tmsis_passthrough
          (select count(submtg_state_cd) as line_ct
	      from &FL.L));

	%let LINE_CT = %eval(&LINE_CT+&&LINE_CT_&STATE_ID);
%mend BUILD_OT;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;

%macro COT00001;

  a.TMSIS_RUN_ID          
, a.TMSIS_FIL_NAME        
, a.TMSIS_OFST_BYTE_NUM   
, a.TMSIS_COMT_ID         
, a.TMSIS_DLTD_IND        
, a.TMSIS_OBSLT_IND       
, a.TMSIS_ACTV_IND        
, a.TMSIS_SQNC_NUM        
, a.TMSIS_RUN_TS          
, a.TMSIS_RPTG_PRD                                
, a.TMSIS_REC_MD5_B64_TXT                                     
, a.REC_TYPE_CD                                               
, a.DATA_DCTNRY_VRSN_NUM                                      
, a.DATA_MPNG_DOC_VRSN_NUM                                    
, a.FIL_CREATD_DT                             
, a.PRD_END_TIME                             
, a.FIL_ENCRPTN_SPEC_CD                       
, a.FIL_NAME                                 
, a.FIL_STUS_CD                                               
, a.SQNC_NUM                                       
, a.SSN_IND                                                   
, a.PRD_EFCTV_TIME                          
, a.STATE_NOTN_TXT                                            
, a.SUBMSN_TRANS_TYPE_CD                  
, a.SUBMTG_STATE_CD                         
, a.TOT_REC_CNT           

%mend COT00001;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro COT00002;

  a.TMSIS_RUN_ID               
, a.TMSIS_ACTV_IND             
, upper(a.SECT_1115A_DEMO_IND) as sect_1115a_demo_ind                         
, coalesce(a.ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT
, coalesce(upper(a.ADJSTMT_IND),'X') as ADJSTMT_IND                                   
, upper(a.ADJSTMT_RSN_CD) as adjstmt_rsn_cd                              
, coalesce(a.SRVC_BGNNG_DT,'01JAN1960') as SRVC_BGNNG_DT_HEADER  
, a.TOT_BENE_COINSRNC_PD_AMT                        
, a.BENE_COINSRNC_PD_DT                   
, a.TOT_BENE_COPMT_PD_AMT                               
, a.BENE_COPMT_PD_DT                           
, a.TOT_BENE_DDCTBL_PD_AMT                            
, a.BENE_DDCTBL_PD_DT                        
, upper(a.BLG_PRVDR_NPI_NUM) as blg_prvdr_npi_num                         
, upper(a.BLG_PRVDR_NUM) as blg_prvdr_num                               
, upper(a.BLG_PRVDR_SPCLTY_CD) as blg_prvdr_spclty_cd                       
, upper(a.BLG_PRVDR_TXNMY_CD) as blg_prvdr_txnmy_cd                        
, upper(a.BLG_PRVDR_TYPE_CD) as blg_prvdr_type_cd                           
, upper(a.BRDR_STATE_IND) as brdr_state_ind                             
, a.CPTATD_PYMT_RQSTD_AMT                 
, upper(a.CLM_DND_IND) as clm_dnd_ind   		/* needed for processing */                                   
, a.CLL_CNT                                       
, upper(a.CLM_STUS_CD) as clm_stus_cd   		/* needed for processing */                                      
, upper(a.COPAY_WVD_IND) as copay_wvd_ind                                
, upper(a.XOVR_IND) as xovr_ind                                          
, a.DAILY_RATE                                      
, a.CPTATD_AMT_RQSTD_DT                    
, a.BIRTH_DT                                               
, trim(translate(upper(a.DGNS_1_CD),'.','')) as dgns_1_cd                                       
, trim(translate(upper(a.DGNS_2_CD),'.','')) as dgns_2_cd                                         
, upper(a.DGNS_1_CD_IND) as dgns_1_cd_ind                                
, upper(a.DGNS_2_CD_IND) as dgns_2_cd_ind                               
, upper(a.DGNS_POA_1_CD_IND) as dgns_poa_1_cd_ind                         
, upper(a.DGNS_POA_2_CD_IND) as dgns_poa_2_cd_ind                         
, upper(a.ELGBL_1ST_NAME) as elgbl_1st_name                               
, upper(a.ELGBL_LAST_NAME) as elgbl_last_name                            
, upper(a.ELGBL_MDL_INITL_NAME) as elgbl_mdl_initl_name                      
, coalesce(a.SRVC_ENDG_DT,'01JAN1960') as SRVC_ENDG_DT_HEADER                              
, upper(a.FIXD_PYMT_IND) as fixd_pymt_ind                                 
, upper(a.FUNDNG_CD) as fundng_cd                                        
, upper(a.FUNDNG_SRC_NON_FED_SHR_CD) as fundng_src_non_fed_shr_cd               
, upper(a.HLTH_CARE_ACQRD_COND_CD) as hlth_care_acqrd_cond_cd           
, upper(a.HH_ENT_NAME) as hh_ent_name                              
, upper(a.HH_PRVDR_IND) as hh_prvdr_ind                          
, upper(a.HH_PRVDR_NPI_NUM) as hh_prvdr_npi_num                        
, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') AS ADJSTMT_CLM_NUM                                 
, coalesce(upper(a.ORGNL_CLM_NUM),'~') AS ORGNL_CLM_NUM                                    
, a.MDCD_PD_DT                                       
, upper(a.MDCR_BENE_ID) as mdcr_bene_id                                   
, upper(a.MDCR_CMBND_DDCTBL_IND) as mdcr_cmbnd_ddctbl_ind                     
, upper(a.MDCR_HICN_NUM) as mdcr_hicn_num                                 
, upper(a.MDCR_REIMBRSMT_TYPE_CD) as mdcr_reimbrsmt_type_cd                     
, upper(a.MSIS_IDENT_NUM) as msis_ident_num                              
, upper(a.OCRNC_01_CD) as ocrnc_01_cd                                    
, upper(a.OCRNC_02_CD) as ocrnc_02_cd                                    
, upper(a.OCRNC_03_CD) as ocrnc_03_cd                                     
, upper(a.OCRNC_04_CD) as ocrnc_04_cd                                     
, upper(a.OCRNC_05_CD) as ocrnc_05_cd                                     
, upper(a.OCRNC_06_CD) as ocrnc_06_cd                                    
, upper(a.OCRNC_07_CD) as ocrnc_07_cd                                     
, upper(a.OCRNC_08_CD) as ocrnc_08_cd                                     
, upper(a.OCRNC_09_CD) as ocrnc_09_cd                                  
, upper(a.OCRNC_10_CD) as ocrnc_10_cd                                    
, a.OCRNC_01_CD_EFCTV_DT                  
, a.OCRNC_02_CD_EFCTV_DT                  
, a.OCRNC_03_CD_EFCTV_DT                  
, a.OCRNC_04_CD_EFCTV_DT                   
, a.OCRNC_05_CD_EFCTV_DT                   
, a.OCRNC_06_CD_EFCTV_DT                 
, a.OCRNC_07_CD_EFCTV_DT                  
, a.OCRNC_08_CD_EFCTV_DT                   
, a.OCRNC_09_CD_EFCTV_DT                   
, a.OCRNC_10_CD_EFCTV_DT                  
, a.OCRNC_01_CD_END_DT                       
, a.OCRNC_02_CD_END_DT                      
, a.OCRNC_03_CD_END_DT                       
, a.OCRNC_04_CD_END_DT                     
, a.OCRNC_05_CD_END_DT                      
, a.OCRNC_06_CD_END_DT                     
, a.OCRNC_07_CD_END_DT                      
, a.OCRNC_08_CD_END_DT                     
, a.OCRNC_09_CD_END_DT                      
, a.OCRNC_10_CD_END_DT                     
, upper(a.OTHR_INSRNC_IND) as othr_insrnc_ind         
, upper(a.OTHR_TPL_CLCTN_CD) as othr_tpl_clctn_cd    
, upper(a.PYMT_LVL_IND) as pymt_lvl_ind                
, upper(a.SRVC_PLC_CD) as srvc_plc_cd                           
, upper(a.PLAN_ID_NUM) as mc_plan_id                  
, upper(a.PGM_TYPE_CD) as pgm_type_cd                       
, upper(a.PRVDR_LCTN_ID) as prvdr_lctn_id              
, upper(a.RFRG_PRVDR_NPI_NUM) as rfrg_prvdr_npi_num
, upper(a.RFRG_PRVDR_NUM) as rfrg_prvdr_num          
, upper(a.RFRG_PRVDR_SPCLTY_CD) as rfrg_prvdr_spclty_cd
, upper(a.RFRG_PRVDR_TXNMY_CD) as rfrg_prvdr_txnmy_cd
, upper(a.RFRG_PRVDR_TYPE_CD) as rfrg_prvdr_type_cd  
, upper(a.RMTNC_NUM) as rmtnc_num                                      
, a.SRVC_TRKNG_PYMT_AMT                    
, upper(a.SRVC_TRKNG_TYPE_CD) as srvc_trkng_type_cd  
, a.SUBMTG_STATE_CD 
, a.TOT_MDCR_COINSRNC_AMT                    
, a.TOT_MDCR_DDCTBL_AMT                    
, upper(a.BILL_TYPE_CD) as bill_type_cd               
, upper(a.CLM_TYPE_CD) as clm_type_cd                            
, upper(a.WVR_ID) as wvr_id                                  
, upper(a.WVR_TYPE_CD) as wvr_type_cd
, upper(a.PRVDR_UNDER_DRCTN_NPI_NUM) as prvdr_under_drctn_npi_num
, upper(a.PRVDR_UNDER_DRCTN_TXNMY_CD) as prvdr_under_drctn_txnmy_cd
, upper(a.PRVDR_UNDER_SPRVSN_NPI_NUM) as prvdr_under_sprvsn_npi_num
, upper(a.PRVDR_UNDER_SPRVSN_TXNMY_CD) as prvdr_under_sprvsn_txnmy_cd
, a.TP_COINSRNC_PD_AMT                       
, a.TP_COINSRNC_PD_DT                        
, a.TP_COPMT_PD_AMT                            
, a.TP_COPMT_PD_DT                           
, a.TOT_ALOWD_AMT                               
, a.TOT_BILL_AMT                             
, a.TOT_COPAY_AMT                                 
, a.TOT_MDCD_PD_AMT                           
, a.TOT_OTHR_INSRNC_AMT 
, a.TOT_TPL_AMT         
, coalesce(a.SRVC_ENDG_DT,a.SRVC_BGNNG_DT) as SRVC_ENDG_DT_DRVD_H
, case when a.SRVC_ENDG_DT is not null then '2' 
	   when a.SRVC_ENDG_DT is null and a.SRVC_BGNNG_DT is not null then '3'
	   else null
  end  as SRVC_ENDG_DT_CD_H

%mend COT00002;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro COT00003;

  a.TMSIS_FIL_NAME
, a.REC_NUM
, a.TMSIS_RUN_ID as TMSIS_RUN_ID_LINE 		                
, coalesce(a.ADJDCTN_DT, '01JAN1960') AS ADJDCTN_DT_LINE 		                                      
, a.ALOWD_AMT                                          
, coalesce(a.SRVC_BGNNG_DT,'01JAN1960') as SRVC_BGNNG_DT_LINE		                                        
, upper(a.BNFT_TYPE_CD) as bnft_type_cd                                      
, a.BILL_AMT                                             
, upper(a.CLL_STUS_CD) as cll_stus_cd                                     
, upper(a.CMS_64_FED_REIMBRSMT_CTGRY_CD) as cms_64_fed_reimbrsmt_ctgry_cd           
, a.BENE_COPMT_PD_AMT                                          
, coalesce(a.SRVC_ENDG_DT,'01JAN1960') as SRVC_ENDG_DT_LINE 		                                         
, upper(a.HCPCS_SRVC_CD) 		as HCBS_SRVC_CD                                   
, upper(a.HCPCS_TXNMY_CD) 		as HCBS_TXNMY                                     
, upper(a.HCPCS_RATE) as hcpcs_rate                                           
, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') AS ADJSTMT_CLM_NUM_LINE  	                               
, coalesce(upper(a.ORGNL_CLM_NUM), '~') AS ORGNL_CLM_NUM_LINE     	                                 
, upper(a.IMNZTN_TYPE_CD) as imnztn_type_cd                                    
, coalesce(upper(a.LINE_ADJSTMT_IND),'X') as LINE_ADJSTMT_IND                             
, upper(a.ADJSTMT_LINE_RSN_CD) as adjstmt_line_rsn_cd                       
, upper(a.ADJSTMT_LINE_NUM) as adjstmt_line_num                             
, upper(a.ORGNL_LINE_NUM) as orgnl_line_num                                 
, a.MDCD_FFS_EQUIV_AMT                        
, a.MDCD_PD_AMT                                       
, a.MDCR_PD_AMT                                      
, upper(a.MSIS_IDENT_NUM) 		as MSIS_IDENT_NUM_LINE                               
, upper(a.NDC_CD) as ndc_cd                                           
, a.NDC_QTY                                               
, upper(a.NDC_UOM_CD) as uom_cd                                            
, a.SVC_QTY_ACTL                   
, a.SVC_QTY_ALOWD                
, a.OTHR_INSRNC_AMT                               
, upper(a.PRE_AUTHRZTN_NUM) as pre_authrztn_num                                      
, upper(a.PRCDR_CD) as prcdr_cd                                             
, a.PRCDR_CD_DT                                       
, upper(a.PRCDR_CD_IND) as prcdr_cd_ind                                    
, upper(a.PRCDR_1_MDFR_CD) as prcdr_1_mdfr_cd                                
, upper(a.PRCDR_2_MDFR_CD) as prcdr_2_mdfr_cd                                
, upper(a.PRCDR_3_MDFR_CD) as prcdr_3_mdfr_cd                                
, upper(a.PRCDR_4_MDFR_CD) as prcdr_4_mdfr_cd                                
, upper(a.REV_CD) as rev_cd                                                
, upper(a.SELF_DRCTN_TYPE_CD) as self_drctn_type_cd                            
, upper(a.PRSCRBNG_PRVDR_NPI_NUM) as prscrbng_prvdr_npi_num                   
, upper(a.SRVCNG_PRVDR_NUM) as srvcng_prvdr_num                             
, upper(a.SRVCNG_PRVDR_SPCLTY_CD) as srvcng_prvdr_spclty_cd                    
, upper(a.SRVCNG_PRVDR_TXNMY_CD) as srvcng_prvdr_txnmy_cd                      
, upper(a.SRVCNG_PRVDR_TYPE_CD) as srvcng_prvdr_type_cd                        
, upper(a.STATE_NOTN_TXT) as state_notn_txt                                     
, a.SUBMTG_STATE_CD 	as SUBMTG_STATE_CD_LINE                                 
, upper(a.TOOTH_DSGNTN_SYS_CD) as tooth_dsgntn_sys_cd                         
, upper(a.TOOTH_NUM) as tooth_num                                           
, upper(a.TOOTH_ORAL_CVTY_AREA_DSGNTD_CD) as tooth_oral_cvty_area_dsgntd_cd                  
, upper(a.TOOTH_SRFC_CD) as tooth_srfc_cd                                   
, a.TPL_AMT                                              
, upper(a.STC_CD) as tos_cd                                                    
, upper(lpad(trim(a.XIX_SRVC_CTGRY_CD),4,'0')) as XIX_SRVC_CTGRY_CD               
, upper(lpad(trim(a.XXI_SRVC_CTGRY_CD),3,'0')) as XXI_SRVC_CTGRY_CD

%mend COT00003;




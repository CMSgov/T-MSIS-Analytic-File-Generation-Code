/************************************************************************************************************/
/* Program:		AWS_IP_Macros.sas																			*/
/* Author:		Deo S. Bencio																				*/
/* Date:		12/1/2016																					*/
/* Purpose:		Program contains macros that massage data from various segments in T-MSIS					*/
/*				THIS PROGRAM DOES NOT STAND ALONE.                                                      	*/
/*				IP_build.sas - pull program for IP build													*/
/*                                                                                                          */
/* Modified:	12/15/2017 - DB modified to read T-MSIS tables directly instead of creating temp files first*/
/*				4/2/2018   - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q1.xlsx           */
/*				10/4/2018  - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q3				*/
/*				3/7/2019   - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q1.xlsx			*/
/* 							 Added column LINE_NUM to identify line numbers in LINE table					*/
/*							 Renamed IP_LT_ACTL_SRVC_QTY to ACTL_SRVC_QTY and	                            */
/*                                   IP_LT_ALOWD_SRVC_QTY to ALOWD_SRVC_QTY		                            */
/*							 Remove dots (.) and trailing spaces from diagnosis codes						*/
/*				9/22/2019 - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q3                 */
/*							Upcased ICN ORIG and ICN ADJSTMT at the FA Header/Line Join						*/
/*				6/9/2020  - DB modified to apply TAF CCB 2020 Q2 Change Request                             */
/*                                                                                                          */
/************************************************************************************************************/
options SASTRACE=',,,ds' SASTRACELOC=Saslog nostsuffix dbidirectexec sqlgeneration=dbms msglevel=I sql_ip_trace=source;
options spool;

/* pull IP line item records for header records linked with claims family table dataset */
%macro AWS_Extract_Line_IP (TMSIS_SCHEMA, fl2, fl, tab_no, _2x_segment, analysis_date);

    /** Create a temporary line file **/
execute (

		create temp table &FL2._LINE_IN
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (SUBMTG_STATE_CD,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
		as
		select  SUBMTG_STATE_CD, %&tab_no

		from	&TMSIS_SCHEMA..&_2x_segment  A

		where  A.TMSIS_ACTV_IND = 1 							/* include active indicator = 1 */
			   and (a.submtg_state_cd,a.tmsis_run_id) in (&combined_list)

	) by tmsis_passthrough;

	/* Subset line file and attach row numbers to all records belonging to an ICN set.  Fix PA & IA   */
	execute (

		create temp table &FL2._LINE
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)

		as

		select A.*,
        row_number() over (partition by A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM_LINE,A.ADJSTMT_CLM_NUM_LINE,A.ADJDCTN_DT_LINE,A.LINE_ADJSTMT_IND 
		order by A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM_LINE,A.ADJSTMT_CLM_NUM_LINE,A.ADJDCTN_DT_LINE,A.LINE_ADJSTMT_IND,A.TMSIS_FIL_NAME,A.REC_NUM ) as RN  

		,CASE
		WHEN A.SUBMTG_STATE_CD = '97' THEN '42'
		WHEN A.SUBMTG_STATE_CD = '96' THEN '19'
	    WHEN A.SUBMTG_STATE_CD = '94' THEN '30'
	    WHEN A.SUBMTG_STATE_CD = '93' THEN '56'
		ELSE A.SUBMTG_STATE_CD
		END AS NEW_SUBMTG_STATE_CD_LINE

		from	&FL2._LINE_IN as A inner join FA_HDR_&FL. H

		on   	H.TMSIS_RUN_ID = A.TMSIS_RUN_ID_LINE and
		H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD and
	    H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM_LINE and
		H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM_LINE and
		H.ADJDCTN_DT = A.ADJDCTN_DT_LINE and
		H.ADJSTMT_IND = A.LINE_ADJSTMT_IND

	) by tmsis_passthrough;

	/* Pull out maximum row_number for each partition */
	execute (

		create temp table RN_&FL2.
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
		as

		select	NEW_SUBMTG_STATE_CD_LINE
		, ORGNL_CLM_NUM_LINE
		, ADJSTMT_CLM_NUM_LINE
		, ADJDCTN_DT_LINE
		, LINE_ADJSTMT_IND
		, max(RN) as NUM_CLL

		from	&FL2._LINE

		group by NEW_SUBMTG_STATE_CD_LINE, ORGNL_CLM_NUM_LINE, ADJSTMT_CLM_NUM_LINE, ADJDCTN_DT_LINE, LINE_ADJSTMT_IND

		) by tmsis_passthrough;

	/* Attach num_cll variable to header records as per instruction */
	execute (

		create temp table &fl2._HEADER
		distkey (ORGNL_CLM_NUM) 
		sortkey (NEW_SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
		as

		select  HEADER.*       
		,coalesce(RN.NUM_CLL,0) as NUM_CLL

		from 	FA_HDR_&FL2. HEADER left join RN_&FL2. RN							

		on   	HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
		HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and 
		HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
		HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
		HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND

		) by tmsis_passthrough;

%DROP_temp_tables(RN_&FL2);
%DROP_temp_tables(&FL2._LINE_IN);

%Get_Audt_counts_clms(&DA_SCHEMA.,&DA_RUN_ID., AWS_IP_Macros, 1.1 AWS_Extract_Line_IP);
	
%DROP_temp_tables(FA_HDR_&FL);
%mend AWS_Extract_Line_IP;

%MACRO BUILD_IP();
	/* ORDER VARIABLES AND UPCASE, LEFT PAD WITH ZEROS AND RESET COALESCE VALUES HEADER FILE*/
	execute (

	CREATE TEMP TABLE IPH 
	distkey(ORGNL_CLM_NUM)
	as      

	SELECT &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT) AS CHAR(4))|| 
	CAST(DATE_PART(MONTH,ADJDCTN_DT) AS CHAR(2))|| 
    CAST(DATE_PART(DAY,ADJDCTN_DT) AS CHAR(2)) || '-' || COALESCE(ADJSTMT_IND_CLEAN,'X')) 
	as varchar(126)) as IP_LINK_KEY
	,%nrbquote('&VERSION.') as IP_VRSN
	,%nrbquote('&TAF_FILE_DATE.') as IP_FIL_DT
	,TMSIS_RUN_ID
	,%var_set_type1(MSIS_IDENT_NUM)
	,NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD 
	,%var_set_type3(orgnl_clm_num, cond1=~)
	,%var_set_type3(adjstmt_clm_num, cond1=~)
	,ADJSTMT_IND_CLEAN as ADJSTMT_IND
	,%var_set_rsn(ADJSTMT_RSN_CD)
	,%fix_old_dates(ADMSN_DT)
	,%var_set_type5(ADMSN_HR_NUM,lpad=2,lowerbound=0,upperbound=23)
	,nullif(DSCHRG_DT,'01JAN1960') as DSCHRG_DT
	,%var_set_type5(DSCHRG_HR_NUM,lpad=2,lowerbound=0,upperbound=23)
	,case when date_cmp(ADJDCTN_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT,'01JAN1960') end as ADJDCTN_DT
    ,%fix_old_dates(MDCD_PD_DT)
	,%var_set_type2(ADMSN_TYPE_CD,0,cond1=1,cond2=2,cond3=3,cond4=4,cond5=5)
	,%var_set_type2(HOSP_TYPE_CD,2,cond1=00,cond2=01,cond3=02,cond4=03,cond5=04,cond6=05,cond7=06,cond8=07,cond9=08)
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
	,%var_set_type6(SRVC_TRKNG_PYMT_AMT, cond1=888888888.88)
	,%var_set_type2(OTHR_INSRNC_IND,0,cond1=0,cond2=1)
	,%var_set_type2(othr_tpl_clctn_cd,3,cond1=000,cond2=001,cond3=002,cond4=003,cond5=004,cond6=005,cond7=006,cond8=007)
	,%var_set_type2(FIXD_PYMT_IND,0,cond1=0,cond2=1)
	,%var_set_type4(FUNDNG_CD,YES,cond1=A,cond2=B,cond3=C,cond4=D,cond5=E,cond6=F,cond7=G,cond8=H,cond9=I)
	,%var_set_type2(fundng_src_non_fed_shr_cd,2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06)
	,%var_set_type2(BRDR_STATE_IND,0,cond1=0,cond2=1)
	,%var_set_type2(XOVR_IND,0,cond1=0,cond2=1)
	,%var_set_type1(MDCR_HICN_NUM)
	,%var_set_type1(MDCR_BENE_ID)
	,%var_set_type1(PTNT_CNTL_NUM)
	,%var_set_type2(HLTH_CARE_ACQRD_COND_CD,0,cond1=0,cond2=1)
	,%var_set_ptstatus(PTNT_STUS_CD)	
	,case when BIRTH_WT_GRMS_QTY <= 0 or BIRTH_WT_GRMS_QTY in (888889.000, 88888.888, 888888.000) then NULL 
	else cast(BIRTH_WT_GRMS_QTY as decimal(9,3))
	end  as   BIRTH_WT_GRMS_QTY
	,%var_set_fills(ADMTG_DGNS_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(ADMTG_DGNS_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_fills(DGNS_1_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_1_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_1_CD_IND)
	,%var_set_fills(DGNS_2_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_2_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_2_CD_IND)
	,%var_set_fills(DGNS_3_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_3_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_3_CD_IND)
	,%var_set_fills(DGNS_4_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_4_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_4_CD_IND)
	,%var_set_fills(DGNS_5_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_5_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_5_CD_IND)
	,%var_set_fills(DGNS_6_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_6_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_6_CD_IND)
	,%var_set_fills(DGNS_7_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_7_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_7_CD_IND)
	,%var_set_fills(DGNS_8_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_8_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_8_CD_IND)
	,%var_set_fills(DGNS_9_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_9_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_9_CD_IND)
	,%var_set_fills(DGNS_10_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_10_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_10_CD_IND)
	,%var_set_fills(DGNS_11_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_11_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_11_CD_IND)
	,%var_set_fills(DGNS_12_CD,cond1=0,cond2=8,cond3=9,cond4=#)
	,%var_set_type2(DGNS_12_CD_IND,0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_12_CD_IND)
	,DRG_CD
	,%var_set_type1(DRG_CD_IND)
	,%var_set_type1(DRG_DESC)
	,%fix_old_dates(PRCDR_1_CD_DT)
	,%var_set_fillpr(PRCDR_1_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_proc(PRCDR_1_CD_IND)
	,%fix_old_dates(PRCDR_2_CD_DT)
	,%var_set_fillpr(PRCDR_2_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_proc(PRCDR_2_CD_IND)
	,%fix_old_dates(PRCDR_3_CD_DT)
	,%var_set_fillpr(PRCDR_3_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_proc(PRCDR_3_CD_IND)
	,%fix_old_dates(PRCDR_4_CD_DT)
	,%var_set_fillpr(PRCDR_4_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_proc(PRCDR_4_CD_IND)
	,%fix_old_dates(PRCDR_5_CD_DT)
	,%var_set_fillpr(PRCDR_5_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_proc(PRCDR_5_CD_IND)
	,%fix_old_dates(PRCDR_6_CD_DT)
	,%var_set_fillpr(PRCDR_6_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_proc(PRCDR_6_CD_IND)
	,%var_set_type6(NCVRD_DAYS_CNT,   cond1=88888)
	,%var_set_type6(NCVRD_CHRGS_AMT,  cond1=88888888888.00)
	,%var_set_type6(MDCD_CVRD_IP_DAYS_CNT, cond1=88888)
	,%var_set_type6(OUTLIER_DAYS_CNT, cond1=888)
	,case when lpad(OUTLIER_CD,2,'0') in ('03','04','05') then NULL 
	else %var_set_type5(outlier_cd,lpad=2,lowerbound=0,upperbound=10,multiple_condition=YES)
	,%var_set_type1(ADMTG_PRVDR_NPI_NUM)
	,%var_set_type1(ADMTG_PRVDR_NUM)
	,%var_set_spclty(ADMTG_PRVDR_SPCLTY_CD)
	,%var_set_taxo(ADMTG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_prtype(admtg_prvdr_type_cd)
	,%var_set_type1(BLG_PRVDR_NUM)
	,%var_set_type1(BLG_PRVDR_NPI_NUM)
	,%var_set_taxo(BLG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
	,%var_set_prtype(blg_prvdr_type_cd)
	,%var_set_spclty(BLG_PRVDR_SPCLTY_CD)
	,%var_set_type1(RFRG_PRVDR_NUM)
	,%var_set_type1(RFRG_PRVDR_NPI_NUM)
	,%var_set_prtype(rfrg_prvdr_type_cd)
	,%var_set_spclty(RFRG_PRVDR_SPCLTY_CD)
	/* ,RFRG_PRVDR_TXNMY_CD - not retained */
	,%var_set_type1(PRVDR_LCTN_ID)
	,%var_set_type2(PYMT_LVL_IND,0,cond1=1,cond2=2)
	,%var_set_type6(TOT_BILL_AMT,	  cond1=888888888.88, cond2=99999999.90, cond3=9999999.99, cond4=999999.99, cond5=999999.00)
	,%var_set_type6(TOT_ALOWD_AMT,	  cond1=888888888.88, cond2=99999999.00)
	,%var_set_type6(TOT_MDCD_PD_AMT,  cond1=888888888.88)
	,%var_set_type6(TOT_COPAY_AMT,	  cond1=9999999.99, cond2=888888888.88, cond3=88888888888.00)
	,%var_set_type6(TOT_TPL_AMT,	  cond1=888888888.88, cond2=999999.99)
	,%var_set_type6(TOT_OTHR_INSRNC_AMT, cond1=888888888.88)
	,%var_set_type6(TP_COINSRNC_PD_AMT, cond1=888888888.88)
	,%var_set_type6(TP_COPMT_PD_AMT,  cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00, cond4=99999999999.00)
	,%var_set_type6(MDCD_DSH_PD_AMT,  cond1=888888888.88)
	,%var_set_type6(DRG_OUTLIER_AMT,  cond1=888888888.88)
	,case 
		when regexp_count(drg_rltv_wt_num,'[^0-9.]')>0 or regexp_count(drg_rltv_wt_num,'[.]')>1 then null
		when cast(drg_rltv_wt_num as numeric(8,0))>9999999 then null
	 else cast(drg_rltv_wt_num as numeric(11,4)) end as DRG_RLTV_WT_NUM  
	,%var_set_type6(MDCR_PD_AMT,	  cond1=888888888.88, cond2=8888888.88, cond3=88888888888.00, cond4=88888888888.88, cond5=99999999999.00, cond6=9999999999.99)
	,%var_set_type6(TOT_MDCR_DDCTBL_AMT, cond1=888888888.88, cond2=99999, cond3=88888888888.00)
	,%var_set_type6(TOT_MDCR_COINSRNC_AMT, cond1=888888888.88)
	,%var_set_type2(MDCR_CMBND_DDCTBL_IND,0,cond1=0,cond2=1)
	,%var_set_type2(mdcr_reimbrsmt_type_cd,2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06,cond7=07,cond8=08,cond9=09)
	,%var_set_type6(BENE_COINSRNC_AMT,cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)
	,%var_set_type6(BENE_COPMT_AMT,   cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)
	,%var_set_type6(BENE_DDCTBL_AMT,  cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)
	,%var_set_type2(COPAY_WVD_IND,0,cond1=0,cond2=1)
	,%fix_old_dates(OCRNC_01_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_01_CD_END_DT)
	,%var_set_type1(OCRNC_01_CD)
	,%fix_old_dates(OCRNC_02_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_02_CD_END_DT)
	,%var_set_type1(OCRNC_02_CD)
	,%fix_old_dates(OCRNC_03_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_03_CD_END_DT)
	,%var_set_type1(OCRNC_03_CD)
	,%fix_old_dates(OCRNC_04_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_04_CD_END_DT)
	,%var_set_type1(OCRNC_04_CD)
	,%fix_old_dates(OCRNC_05_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_05_CD_END_DT)
	,%var_set_type1(OCRNC_05_CD)
	,%fix_old_dates(OCRNC_06_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_06_CD_END_DT)
	,%var_set_type1(OCRNC_06_CD)
	,%fix_old_dates(OCRNC_07_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_07_CD_END_DT)
	,%var_set_type1(OCRNC_07_CD)
	,%fix_old_dates(OCRNC_08_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_08_CD_END_DT)
	,%var_set_type1(OCRNC_08_CD)
	,%fix_old_dates(OCRNC_09_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_09_CD_END_DT)
	,%var_set_type1(OCRNC_09_CD)
	,%fix_old_dates(OCRNC_10_CD_EFCTV_DT)
	,%fix_old_dates(OCRNC_10_CD_END_DT)
	,%var_set_type1(OCRNC_10_CD)
	,%var_set_type2(SPLIT_CLM_IND,0,cond1=0,cond2=1)
	,CLL_CNT
	,NUM_CLL

	/* constructed variables */

	,IP_MH_DX_IND
	,IP_SUD_DX_IND
	,IP_MH_TAXONOMY_IND as IP_MH_TXNMY_IND
	,IP_SUD_TAXONOMY_IND as IP_SUD_TXNMY_IND
	,null :: char(3) as MAJ_DGNSTC_CTGRY
	,cast(nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as char(6)) as IAP_COND_IND
	,cast(nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as char(9)) as PRMRY_HIRCHCL_COND

	FROM 
	(select *,
     case when ADJSTMT_IND is NOT NULL and    
               trim(ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(ADJSTMT_IND)     else NULL   end as ADJSTMT_IND_CLEAN 
     from &fl._HEADER_GROUPER) H
	) BY TMSIS_PASSTHROUGH;

%DROP_temp_tables(&fl._HEADER_GROUPER);

	/* ORDER VARIABLES AND UPCASE, LEFT PAD WITH ZEROS AND RESET COALESCE VALUES LINE FILE*/
	execute (

		CREATE TEMP TABLE IPL 
		distkey(ORGNL_CLM_NUM)
		AS      

		SELECT &DA_RUN_ID as DA_RUN_ID
		,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD_LINE || '-' ||
		trim(COALESCE(NULLIF(ORGNL_CLM_NUM_LINE,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM_LINE,'~'),'0')) || '-' || 
        CAST(DATE_PART_YEAR(ADJDCTN_DT_LINE) AS CHAR(4)) ||
		CAST(DATE_PART(MONTH,ADJDCTN_DT_LINE) AS CHAR(2)) ||
        CAST(DATE_PART(DAY,ADJDCTN_DT_LINE) AS CHAR(2)) || '-' || 
        COALESCE(LINE_ADJSTMT_IND_CLEAN,'X')) as varchar(126)) as IP_LINK_KEY
		,%nrbquote('&VERSION.') as IP_VRSN
		,%nrbquote('&TAF_FILE_DATE.') as IP_FIL_DT
		,TMSIS_RUN_ID_LINE as TMSIS_RUN_ID  
		,%var_set_type1(MSIS_IDENT_NUM_LINE,new=MSIS_IDENT_NUM)  
		,NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD
		,%var_set_type3(ORGNL_CLM_NUM_LINE,cond1=~,new=ORGNL_CLM_NUM)
		,%var_set_type3(ADJSTMT_CLM_NUM_LINE,cond1=~,new=ADJSTMT_CLM_NUM)
		,%var_set_type1(ORGNL_LINE_NUM)                           
		,%var_set_type1(ADJSTMT_LINE_NUM)   
	    ,case when date_cmp(ADJDCTN_DT_LINE,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT_LINE,'01JAN1960') end as ADJDCTN_DT
		,LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND
		,%var_set_tos(TOS_CD)
		,case when lpad(IMNZTN_TYPE_CD,2,'0') = '88' then NULL 
		else %var_set_type5(IMNZTN_type_cd,lpad=2,lowerbound=0,upperbound=29,multiple_condition=YES)
		,%var_set_type2(CMS_64_FED_REIMBRSMT_CTGRY_CD,2,cond1=01,cond2=02,cond3=03,cond4=04)
    	,case when XIX_SRVC_CTGRY_CD in &XIX_SRVC_CTGRY_CD_values. then XIX_SRVC_CTGRY_CD
     	else null end as XIX_SRVC_CTGRY_CD
     	,case when XXI_SRVC_CTGRY_CD in &XXI_SRVC_CTGRY_CD_values. then XXI_SRVC_CTGRY_CD
     	else null end as XXI_SRVC_CTGRY_CD
		,%var_set_type1(CLL_STUS_CD)
	    ,case when date_cmp(SRVC_BGNNG_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_BGNNG_DT,'01JAN1960') end as SRVC_BGNNG_DT
     	,case when date_cmp(SRVC_ENDG_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_ENDG_DT,'01JAN1960') end as SRVC_ENDG_DT
		,%var_set_type5(BNFT_TYPE_CD,lpad=3,lowerbound=001,upperbound=108)
		,%var_set_type1(REV_CD,lpad=4)
		,%var_set_type6(IP_LT_ACTL_SRVC_QTY, new=ACTL_SRVC_QTY, cond1=999999, cond2=88888.888, cond3=99999.990)
		,%var_set_type6(IP_LT_ALOWD_SRVC_QTY, new=ALOWD_SRVC_QTY, cond1=888888.89, cond2=88888.888)
		,%var_set_type6(REV_CHRG_AMT, cond1=88888888888.88, cond2=99999999.9, cond3=888888888.88, cond4=8888888888.88, cond5=88888888.88)
		,%var_set_type1(SRVCNG_PRVDR_NUM)
		,%var_set_type1(PRSCRBNG_PRVDR_NPI_NUM,new=SRVCNG_PRVDR_NPI_NUM)
		,%var_set_taxo(SRVCNG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
		,%var_set_prtype(SRVCNG_PRVDR_TYPE_CD)
		,%var_set_spclty(SRVCNG_PRVDR_SPCLTY_CD)
		,%var_set_type1(OPRTG_PRVDR_NPI_NUM)
		,case when PRVDR_FAC_TYPE_CD in ('100000000', '170000000', '250000000', '260000000', '270000000', '280000000', '290000000', '300000000', '310000000',
		'320000000', '330000000', '340000000', '380000000') then prvdr_fac_type_cd
		else NULL
		end  as prvdr_fac_type_cd
		,%var_set_type6(NDC_QTY, cond1=999999, cond2=888888, cond3=88888.888, cond4=888888.888, cond5=999999.998, cond6=888888.880)
		,%var_set_type1(HCPCS_RATE)
		,%var_set_fills(NDC_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
		,%var_set_type4(UOM_CD,YES,cond1=F2,cond2=ML,cond3=GR,cond4=UN,cond5=ME)
		,%var_set_type6(ALOWD_AMT,		   cond1=888888888.88, cond2=99999999.00, cond3=9999999999.99)
		,%var_set_type6(MDCD_PD_AMT,	   cond1=888888888.88)
		,%var_set_type6(OTHR_INSRNC_AMT,   cond1=888888888.88, cond2=88888888888.00, cond3=88888888888.88)
		,%var_set_type6(MDCD_FFS_EQUIV_AMT,cond1=888888888.88, cond2=88888888888.80, cond3=999999.99)
		,RN as LINE_NUM

		FROM 	(select *,
     		   case when LINE_ADJSTMT_IND is NOT NULL and    
               trim(LINE_ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(LINE_ADJSTMT_IND)     else NULL   end as LINE_ADJSTMT_IND_CLEAN 
     from &FL._LINE) H

		) BY TMSIS_PASSTHROUGH;

%DROP_temp_tables(&FL._LINE);

	EXECUTE(
		INSERT INTO &DA_SCHEMA..TAF_&FL.H	
		SELECT * 
		FROM &FL.H
		) BY TMSIS_PASSTHROUGH;
	select ht_ct into : HEADER_CT
		from (select * from connection to tmsis_passthrough
			(select count(submtg_state_cd) as ht_ct
				from &FL.H));
	EXECUTE(
		INSERT INTO &DA_SCHEMA..TAF_&FL.L	
		(  da_run_id  
			,ip_link_key 
			,ip_vrsn  
			,ip_fil_dt 
			,tmsis_run_id  
			,msis_ident_num  
			,submtg_state_cd  
			,orgnl_clm_num  
			,adjstmt_clm_num  
			,orgnl_line_num  
			,adjstmt_line_num 
			,adjdctn_dt  
			,line_adjstmt_ind 
			,tos_cd  
			,imnztn_type_cd  
			,cms_64_fed_reimbrsmt_ctgry_cd  
			,xix_srvc_ctgry_cd  
			,xxi_srvc_ctgry_cd  
			,cll_stus_cd  
			,srvc_bgnng_dt  
			,srvc_endg_dt  
			,bnft_type_cd  
			,rev_cd   
			,actl_srvc_qty  
			,alowd_srvc_qty  
			,rev_chrg_amt 
			,srvcng_prvdr_num  
			,srvcng_prvdr_npi_num  
			,srvcng_prvdr_txnmy_cd  
			,srvcng_prvdr_type_cd  
			,srvcng_prvdr_spclty_cd  
			,oprtg_prvdr_npi_num  
			,prvdr_fac_type_cd  
			,ndc_qty  
			,hcpcs_rate  
			,ndc_cd  
			,uom_cd  
			,alowd_amt  
			,mdcd_pd_amt  
			,othr_insrnc_amt  
			,mdcd_ffs_equiv_amt   
			,line_num
	 		)
	  	SELECT * 
		FROM &FL.L
		) BY TMSIS_PASSTHROUGH;
	select line_ct into : LINE_CT
		from (select * from connection to tmsis_passthrough
			(select count(submtg_state_cd) as line_ct
				from &FL.L));
%MEND BUILD_IP;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CIP00001;
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
		, a.PRD_EFCTV_TIME           
		, a.STATE_NOTN_TXT        
		, a.SUBMSN_TRANS_TYPE_CD     
		, a.SUBMTG_STATE_CD          
		, a.TOT_REC_CNT
%mend CIP00001;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CIP00002;
	a.TMSIS_RUN_ID
	, a.TMSIS_ACTV_IND
	, upper(a.SECT_1115A_DEMO_IND) as sect_1115a_demo_ind
	, coalesce(a.ADJDCTN_DT,'01JAN1960') as ADJDCTN_DT
	, COALESCE(upper(a.ADJSTMT_IND),'X') AS ADJSTMT_IND
	, upper(a.ADJSTMT_RSN_CD) as adjstmt_rsn_cd
	, a.ADMSN_DT
	, upper(a.ADMSN_HR_NUM) as admsn_hr_num
	, upper(a.ADMSN_TYPE_CD) as admsn_type_cd
	, trim(translate(upper(a.ADMTG_DGNS_CD),'.','')) as admtg_dgns_cd
	, upper(a.ADMTG_DGNS_CD_IND) as admtg_dgns_cd_ind
	, upper(a.ADMTG_PRVDR_NPI_NUM) as admtg_prvdr_npi_num
	, upper(a.ADMTG_PRVDR_NUM) as admtg_prvdr_num
	, upper(a.ADMTG_PRVDR_SPCLTY_CD) as admtg_prvdr_spclty_cd
	, upper(a.ADMTG_PRVDR_TXNMY_CD) as admtg_prvdr_txnmy_cd
	, upper(a.ADMTG_PRVDR_TYPE_CD) as admtg_prvdr_type_cd
	, a.BENE_COINSRNC_AMT
	, a.BENE_COPMT_AMT
	, a.BENE_DDCTBL_AMT
	, upper(a.BLG_PRVDR_NPI_NUM) as blg_prvdr_npi_num
	, upper(a.BLG_PRVDR_NUM) as blg_prvdr_num
	, upper(a.BLG_PRVDR_SPCLTY_CD) as blg_prvdr_spclty_cd
	, upper(a.BLG_PRVDR_TXNMY_CD) as blg_prvdr_txnmy_cd 
	, upper(a.BLG_PRVDR_TYPE_CD) as blg_prvdr_type_cd
	, a.BIRTH_WT_GRMS_QTY
	, upper(a.BRDR_STATE_IND) as brdr_state_ind
	, a.CLL_CNT
	, upper(a.CLM_STUS_CD) as clm_stus_cd
	, upper(a.COPAY_WVD_IND) as copay_wvd_ind
	, upper(a.XOVR_IND) as xovr_ind
	, a.BIRTH_DT                                               
	, trim(translate(upper(a.DGNS_1_CD),'.','')) as dgns_1_cd
	, trim(translate(upper(a.DGNS_10_CD),'.','')) as dgns_10_cd
	, trim(translate(upper(a.DGNS_11_CD),'.','')) as dgns_11_cd
	, trim(translate(upper(a.DGNS_12_CD),'.','')) as dgns_12_cd
	, trim(translate(upper(a.DGNS_2_CD),'.','')) as dgns_2_cd
	, trim(translate(upper(a.DGNS_3_CD),'.','')) as dgns_3_cd
	, trim(translate(upper(a.DGNS_4_CD),'.','')) as dgns_4_cd
	, trim(translate(upper(a.DGNS_5_CD),'.','')) as dgns_5_cd
	, trim(translate(upper(a.DGNS_6_CD),'.','')) as dgns_6_cd
	, trim(translate(upper(a.DGNS_7_CD),'.','')) as dgns_7_cd
	, trim(translate(upper(a.DGNS_8_CD),'.','')) as dgns_8_cd
	, trim(translate(upper(a.DGNS_9_CD),'.','')) as dgns_9_cd
	, upper(a.DGNS_1_CD_IND) as dgns_1_cd_ind
	, upper(a.DGNS_10_CD_IND) as dgns_10_cd_ind
	, upper(a.DGNS_11_CD_IND) as dgns_11_cd_ind
	, upper(a.DGNS_12_CD_IND) as dgns_12_cd_ind
	, upper(a.DGNS_2_CD_IND) as dgns_2_cd_ind
	, upper(a.DGNS_3_CD_IND) as dgns_3_cd_ind
	, upper(a.DGNS_4_CD_IND) as dgns_4_cd_ind
	, upper(a.DGNS_5_CD_IND) as dgns_5_cd_ind
	, upper(a.DGNS_6_CD_IND) as dgns_6_cd_ind
	, upper(a.DGNS_7_CD_IND) as dgns_7_cd_ind
	, upper(a.DGNS_8_CD_IND) as dgns_8_cd_ind
	, upper(a.DGNS_9_CD_IND) as dgns_9_cd_ind
	, upper(a.DGNS_POA_1_CD_IND) as dgns_poa_1_cd_ind
	, upper(a.DGNS_POA_10_CD_IND) as dgns_poa_10_cd_ind
	, upper(a.DGNS_POA_11_CD_IND) as dgns_poa_11_cd_ind
	, upper(a.DGNS_POA_12_CD_IND) as dgns_poa_12_cd_ind
	, upper(a.DGNS_POA_2_CD_IND) as dgns_poa_2_cd_ind
	, upper(a.DGNS_POA_3_CD_IND) as dgns_poa_3_cd_ind
	, upper(a.DGNS_POA_4_CD_IND) as dgns_poa_4_cd_ind
	, upper(a.DGNS_POA_5_CD_IND) as dgns_poa_5_cd_ind
	, upper(a.DGNS_POA_6_CD_IND) as dgns_poa_6_cd_ind
	, upper(a.DGNS_POA_7_CD_IND) as dgns_poa_7_cd_ind
	, upper(a.DGNS_POA_8_CD_IND) as dgns_poa_8_cd_ind
	, upper(a.DGNS_POA_9_CD_IND) as dgns_poa_9_cd_ind
	, upper(a.DRG_CD) as drg_cd
	, upper(a.DRG_CD_IND) as drg_cd_ind
	, coalesce(a.DSCHRG_DT,'01JAN1960') as DSCHRG_DT
	, upper(a.DSCHRG_HR_NUM) as dschrg_hr_num
	, upper(a.DRG_DESC) as drg_desc
	, a.DRG_OUTLIER_AMT 
	, a.DRG_RLTV_WT_NUM
	, upper(a.ELGBL_1ST_NAME) as elgbl_1st_name             
	, upper(a.ELGBL_LAST_NAME) as elgbl_last_name        
	, upper(a.ELGBL_MDL_INITL_NAME) as elgbl_mdl_initl_name
	, upper(a.FIXD_PYMT_IND) as fixd_pymt_ind
	, upper(a.FUNDNG_CD) as fundng_cd
	, upper(a.FUNDNG_SRC_NON_FED_SHR_CD) as fundng_src_non_fed_shr_cd
	, upper(a.HLTH_CARE_ACQRD_COND_CD) as hlth_care_acqrd_cond_cd
	, coalesce(upper(a.ADJSTMT_CLM_NUM), '~') as ADJSTMT_CLM_NUM 
	, coalesce(upper(a.ORGNL_CLM_NUM),'~') as ORGNL_CLM_NUM
	, a.MDCD_DSH_PD_AMT
	, a.MDCD_CVRD_IP_DAYS_CNT
	, a.MDCD_PD_DT
	, upper(a.MDCR_BENE_ID) as mdcr_bene_id
	, upper(a.MDCR_CMBND_DDCTBL_IND) as mdcr_cmbnd_ddctbl_ind
	, upper(a.MDCR_HICN_NUM) as mdcr_hicn_num
	, a.MDCR_PD_AMT
	, upper(a.MDCR_REIMBRSMT_TYPE_CD) as mdcr_reimbrsmt_type_cd
	, upper(a.MSIS_IDENT_NUM) as msis_ident_num
	, cast (a.NCVRD_CHRGS_AMT as decimal(13,2)) as NCVRD_CHRGS_AMT 
	, a.NCVRD_DAYS_CNT
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
	, upper(a.OUTLIER_CD) as outlier_cd
	, a.OUTLIER_DAYS_CNT 
	, upper(a.PTNT_CNTL_NUM) as ptnt_cntl_num
	, upper(a.PTNT_STUS_CD) as ptnt_stus_cd
	, upper(a.PYMT_LVL_IND) as pymt_lvl_ind
	, upper(a.PLAN_ID_NUM) as mc_plan_id
	, upper(a.PRCDR_1_CD) as prcdr_1_cd
	, upper(a.PRCDR_2_CD) as prcdr_2_cd
	, upper(a.PRCDR_3_CD) as prcdr_3_cd
	, upper(a.PRCDR_4_CD) as prcdr_4_cd
	, upper(a.PRCDR_5_CD) as prcdr_5_cd
	, upper(a.PRCDR_6_CD) as prcdr_6_cd
	, a.PRCDR_1_CD_DT
	, a.PRCDR_2_CD_DT
	, a.PRCDR_3_CD_DT
	, a.PRCDR_4_CD_DT
	, a.PRCDR_5_CD_DT
	, a.PRCDR_6_CD_DT
	, upper(a.PRCDR_1_CD_IND) as prcdr_1_cd_ind
	, upper(a.PRCDR_2_CD_IND) as prcdr_2_cd_ind
	, upper(a.PRCDR_3_CD_IND) as prcdr_3_cd_ind
	, upper(a.PRCDR_4_CD_IND) as prcdr_4_cd_ind
	, upper(a.PRCDR_5_CD_IND) as prcdr_5_cd_ind
	, upper(a.PRCDR_6_CD_IND) as prcdr_6_cd_ind
	, upper(a.PGM_TYPE_CD) as pgm_type_cd
	, upper(a.PRVDR_LCTN_ID) as prvdr_lctn_id
	, upper(a.RFRG_PRVDR_NPI_NUM) as rfrg_prvdr_npi_num
	, upper(a.RFRG_PRVDR_NUM) as rfrg_prvdr_num
	, upper(a.RFRG_PRVDR_SPCLTY_CD) as rfrg_prvdr_spclty_cd
	, upper(a.RFRG_PRVDR_TXNMY_CD) as rfrg_prvdr_txnmy_cd
	, upper(a.RFRG_PRVDR_TYPE_CD) as rfrg_prvdr_type_cd
	, a.SRVC_TRKNG_PYMT_AMT 
	, upper(a.SRVC_TRKNG_TYPE_CD) as srvc_trkng_type_cd
	, upper(a.SPLIT_CLM_IND) as split_clm_ind
	, a.SUBMTG_STATE_CD
	, a.TOT_ALOWD_AMT
	, a.TOT_BILL_AMT
	, a.TOT_COPAY_AMT
	, a.TOT_MDCD_PD_AMT
	, a.TOT_MDCR_COINSRNC_AMT
	, a.TOT_MDCR_DDCTBL_AMT
	, a.TOT_OTHR_INSRNC_AMT
	, a.TOT_TPL_AMT
	, upper(a.BILL_TYPE_CD) as bill_type_cd
	, upper(a.CLM_TYPE_CD) as clm_type_cd 
	, upper(a.HOSP_TYPE_CD) as hosp_type_cd
	, upper(a.WVR_ID) as wvr_id
	, upper(a.WVR_TYPE_CD) as wvr_type_cd
	, a.TP_COINSRNC_PD_AMT
	, a.TP_COPMT_PD_AMT
%mend CIP00002;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CIP00003;
	upper(a.TMSIS_FIL_NAME)  as TMSIS_FIL_NAME
		, a.REC_NUM
		, a.TMSIS_RUN_ID as TMSIS_RUN_ID_LINE
		, a.TMSIS_ACTV_IND as TMSIS_ACTV_IND_LINE
		, coalesce(a.ADJDCTN_DT,'01JAN1960') as ADJDCTN_DT_LINE
		, coalesce(a.SRVC_BGNNG_DT,'01JAN1960') as SRVC_BGNNG_DT
		, upper(a.BNFT_TYPE_CD) as bnft_type_cd
		, upper(a.CLL_STUS_CD) as cll_stus_cd
		, upper(a.CMS_64_FED_REIMBRSMT_CTGRY_CD) as cms_64_fed_reimbrsmt_ctgry_cd
		, coalesce(a.SRVC_ENDG_DT,'01JAN1960') as SRVC_ENDG_DT
		, upper(a.HCPCS_RATE) as hcpcs_rate
		, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') as ADJSTMT_CLM_NUM_LINE
		, coalesce(upper(a.ORGNL_CLM_NUM),'~') as ORGNL_CLM_NUM_LINE
		, upper(a.IMNZTN_TYPE_CD) as imnztn_type_cd
		, a.IP_LT_ACTL_SRVC_QTY 
		, a.IP_LT_ALOWD_SRVC_QTY
		, coalesce(upper(a.LINE_ADJSTMT_IND),'X') as LINE_ADJSTMT_IND
		, upper(a.ADJSTMT_LINE_NUM) as ADJSTMT_LINE_NUM                         
		, upper(a.ORGNL_LINE_NUM) as ORGNL_LINE_NUM
		, a.ALOWD_AMT 
		, a.MDCD_FFS_EQUIV_AMT 
		, upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM_LINE
		, upper(a.NDC_CD) as ndc_cd
		, a.NDC_QTY
		, upper(a.NDC_UOM_CD) as uom_cd
		, upper(a.OPRTG_PRVDR_NPI_NUM) as oprtg_prvdr_npi_num
		, upper(a.PRVDR_FAC_TYPE_CD) as prvdr_fac_type_cd
		, a.REV_CHRG_AMT
		, upper(a.REV_CD) as rev_cd
		, upper(a.PRSCRBNG_PRVDR_NPI_NUM) as prscrbng_prvdr_npi_num
		, upper(a.SRVCNG_PRVDR_NUM) as srvcng_prvdr_num
		, upper(a.SRVCNG_PRVDR_SPCLTY_CD) as srvcng_prvdr_spclty_cd
		, upper(a.SRVCNG_PRVDR_TXNMY_CD) as srvcng_prvdr_txnmy_cd
		, upper(a.SRVCNG_PRVDR_TYPE_CD) as srvcng_prvdr_type_cd
		, upper(a.SUBMTG_STATE_CD) as SUBMTG_STATE_CD_LINE
		, upper(a.STC_CD) as tos_cd
		, upper(lpad(trim(a.XIX_SRVC_CTGRY_CD),4,'0')) as XIX_SRVC_CTGRY_CD               
		, upper(lpad(trim(a.XXI_SRVC_CTGRY_CD),3,'0')) as XXI_SRVC_CTGRY_CD
		, a.MDCD_PD_AMT
		, a.OTHR_INSRNC_AMT

%mend  CIP00003;

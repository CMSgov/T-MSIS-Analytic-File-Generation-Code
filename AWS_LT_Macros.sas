/************************************************************************************************************/
/* Program:		AWS_LT_Macros.sas																			*/
/* Author:		Chris Rankin, Deo S. Bencio																	*/
/* Date:		12/1/2016																					*/
/* Purpose:		Program contains macros that massage data from various segments in T-MSIS					*/
/*				THIS PROGRAM DOES NOT STAND ALONE.                                                      	*/
/*				LT_Build.sas - pull program for LT build													*/
/*                                                                                                          */
/* Modified:	4/2/2018  - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q1.xlsx            */
/*				10/4/2018 - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q3					*/
/*				3/7/2019  - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q1.xlsx			*/
/*							Added column LINE_NUM to identify line numbers in LINE table					*/
/*							Renamed IP_LT_ACTL_SRVC_QTY to ACTL_SRVC_QTY and	                            */
/*                                  IP_LT_ALOWD_SRVC_QTY to ALOWD_SRVC_QTY		                            */
/*							Remove dots (.) and trailing spaces from diagnosis codes						*/
/*				9/22/2019 - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q3                 */
/*							Upcased ICN ORIG and ICN ADJSTMT at the FA Header/Line Join						*/
/*				6/9/2020  - DB modified to apply TAF CCB 2020 Q2 Change Request                             */
/*              12/15/2020- DB modified to apply TAF CCB 2020 Q4 Change Request                             */
/*							-MACTAF-1613: Exclude IA CHIP T-MSIS files from TAF Production					*/
/************************************************************************************************************/
options SASTRACE=',,,ds' SASTRACELOC=Saslog nostsuffix dbidirectexec sqlgeneration=dbms msglevel=I sql_ip_trace=source
		noerrorabend;

options spool;

/* pull line item records for header records linked with claims family table dataset */

%macro AWS_Extract_Line_LT (TMSIS_SCHEMA, fl2, fl, tab_no, _2x_segment, analysis_date);

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

			,a.submtg_state_cd as new_submtg_state_cd_line
		from	&FL2._LINE_IN as A inner join FA_HDR_&FL. H

		on   	H.TMSIS_RUN_ID = A.TMSIS_RUN_ID_LINE and
		H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD and
	    H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM_LINE and
		H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM_LINE and
		H.ADJDCTN_DT = A.ADJDCTN_DT_LINE and
		H.ADJSTMT_IND = A.LINE_ADJSTMT_IND

	) by tmsis_passthrough;

	/* Pull out maximum row_number for each partition and compute calculated variables here */
	/* Revisit coding for accommodation_paid, ancillary_paid, cvr_mh_days_over_65, cvr_mh_days_under_21 here */
	execute (

	create temp table constructed_&FL2.
	distkey (ORGNL_CLM_NUM_LINE) 
	sortkey (NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
	as
	 
	select	NEW_SUBMTG_STATE_CD_LINE
			, ORGNL_CLM_NUM_LINE
			, ADJSTMT_CLM_NUM_LINE
			, ADJDCTN_DT_LINE
			, LINE_ADJSTMT_IND
			, max(RN) as NUM_CLL
			, sum (case when substring(lpad(REV_CD,4,'0'),1,2)='01' or substring(lpad(REV_CD,4,'0'),1,3) in ('020', '021') 
                      then MDCD_PD_AMT 
                      when rev_cd is not null and mdcd_pd_amt is not null then 0   
                      end 
                  ) as ACCOMMODATION_PAID
  	 	   , sum (case when substring(lpad(REV_CD,4,'0'),1,2) <> '01' and 
                          substring(lpad(REV_CD,4,'0'),1,3) not in ('020', '021') 
                      then MDCD_PD_AMT 
                      when REV_CD is not null and MDCD_PD_AMT is not null then 0
                      end
                ) as ANCILLARY_PAID
         , max (case when lpad(TOS_CD,3,'0')in ('044','045') then 1
                   when TOS_CD is null then null
				   else 0
				   end
				  ) as MH_DAYS_OVER_65
         , max(case when lpad(TOS_CD,3,'0') = '048' then 1
                    when TOS_CD is null then null
				   else 0
				   end
				  ) as MH_DAYS_UNDER_21

	from	&FL2._LINE

	group by NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND

	) by tmsis_passthrough;


	/* Attach num_cll variable to header records as per instruction */
	/* Use this step to add in constructed variables for accomodation and ancillary paid amounts */
	/* Will probably need to move this step lower */
	execute (

	create temp table &fl._HEADER
	distkey (ORGNL_CLM_NUM) 
	sortkey (NEW_SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
	as
	
	select  HEADER.*
			, coalesce(CONSTR.NUM_CLL,0) as NUM_CLL
			, CONSTR.ACCOMMODATION_PAID
			, CONSTR.ANCILLARY_PAID
            , case when CONSTR.MH_DAYS_OVER_65 = 1 then HEADER.MDCD_CVRD_IP_DAYS_CNT
                   when CONSTR.MH_DAYS_OVER_65 = 0 and  HEADER.MDCD_CVRD_IP_DAYS_CNT is not null then 0                  
                   end as CVRD_MH_DAYS_OVER_65
            , case when CONSTR.MH_DAYS_UNDER_21 = 1 then HEADER.MDCD_CVRD_IP_DAYS_CNT
                   when CONSTR.MH_DAYS_UNDER_21 = 0 and  HEADER.MDCD_CVRD_IP_DAYS_CNT is not null then 0
                   end as CVRD_MH_DAYS_UNDER_21

	from 	FA_HDR_&FL. HEADER left join constructed_&FL2. CONSTR							

  	on   	HEADER.NEW_SUBMTG_STATE_CD = CONSTR.NEW_SUBMTG_STATE_CD_LINE and
		 	HEADER.ORGNL_CLM_NUM = CONSTR.ORGNL_CLM_NUM_LINE and 
		 	HEADER.ADJSTMT_CLM_NUM = CONSTR.ADJSTMT_CLM_NUM_LINE and
			HEADER.ADJDCTN_DT = CONSTR.ADJDCTN_DT_LINE and
			HEADER.ADJSTMT_IND = CONSTR.LINE_ADJSTMT_IND

	) by tmsis_passthrough;

	%Get_Audt_counts_clms(&DA_SCHEMA.,&DA_RUN_ID., AWS_LT_Macros, 1.1 AWS_Extract_Line_LT);

%DROP_temp_tables(constructed_&FL2.);
%DROP_temp_tables(FA_HDR_&FL);
%DROP_temp_tables(&FL2._LINE_IN);
%mend AWS_Extract_Line_LT;

%MACRO BUILD_LT();

    /* ORDER VARIABLES AND UPCASE, LEFT PAD WITH ZEROS AND RESET COALESCE VALUES HEADER FILE*/
    execute (

    CREATE TEMP TABLE LTH 
    distkey(ORGNL_CLM_NUM)
    AS      
    
	SELECT &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT) AS CHAR(4))|| 
	CAST(DATE_PART(MONTH,ADJDCTN_DT) AS CHAR(2))|| 
    CAST(DATE_PART(DAY,ADJDCTN_DT) AS CHAR(2)) || '-' || COALESCE(ADJSTMT_IND_CLEAN,'X')) 
	as varchar(126)) as LT_LINK_KEY
    ,%nrbquote('&VERSION.') as LT_VRSN
    ,%nrbquote('&TAF_FILE_DATE.') as LT_FIL_DT
    ,TMSIS_RUN_ID
    ,%var_set_type1(var=MSIS_IDENT_NUM)
    ,NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD
    ,%var_set_type3(var=ORGNL_CLM_NUM, cond1=~)
    ,%var_set_type3(var=ADJSTMT_CLM_NUM, cond1=~)
    ,ADJSTMT_IND_CLEAN as ADJSTMT_IND
    ,%var_set_rsn(ADJSTMT_RSN_CD)  
    ,case when date_cmp(SRVC_BGNNG_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_BGNNG_DT,'01JAN1960') end as SRVC_BGNNG_DT
	,nullif(SRVC_ENDG_DT,'01JAN1960')  as SRVC_ENDG_DT
    ,%fix_old_dates(ADMSN_DT)
    ,%var_set_type5(var=ADMSN_HR_NUM,lpad=2,lowerbound=0,upperbound=23)
    ,%fix_old_dates(DSCHRG_DT)
    ,%var_set_type5(var=DSCHRG_HR_NUM,lpad=2,lowerbound=0,upperbound=23)
	,case when date_cmp(ADJDCTN_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT,'01JAN1960') end as ADJDCTN_DT
    ,%fix_old_dates(MDCD_PD_DT)
    ,%var_set_type2(var=SECT_1115A_DEMO_IND,lpad=0,cond1=0,cond2=1)
    ,%var_set_type1(var=BILL_TYPE_CD)
    ,case 
      when upper(CLM_TYPE_CD) in ('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z') then upper(CLM_TYPE_CD)
      else NULL
    end as CLM_TYPE_CD
    ,case when lpad(pgm_type_cd,2,'0') in ('06','09') then NULL 
	    else %var_set_type5(var=pgm_type_cd,lpad=2,lowerbound=0,upperbound=17,multiple_condition=YES)
    ,%var_set_type1(var=MC_PLAN_ID)
    ,%var_set_type1(var=ELGBL_LAST_NAME,upper=YES)                             
    ,%var_set_type1(var=ELGBL_1ST_NAME,upper=YES)                              
    ,%var_set_type1(var=ELGBL_MDL_INITL_NAME,upper=YES)                       
    ,%fix_old_dates(BIRTH_DT)
    ,case when lpad(wvr_type_cd,2,'0') = '88' then NULL 
	   else %var_set_type5(var=wvr_type_cd,lpad=2,lowerbound=1,upperbound=33,multiple_condition=YES)
    ,%var_set_type1(var=WVR_ID)                       
    ,%var_set_type2(var=SRVC_TRKNG_TYPE_CD,lpad=2,cond1=00,cond2=01,cond3=02,cond4=03,cond5=04,cond6=05,cond7=06)
    ,%var_set_type6(SRVC_TRKNG_PYMT_AMT,	cond1=888888888.88)
    ,%var_set_type2(var=OTHR_INSRNC_IND,lpad=0,cond1=0,cond2=1)
    ,%var_set_type2(var=OTHR_TPL_CLCTN_CD,lpad=3,cond1=000,cond2=001,cond3=002,cond4=003,cond5=004,cond6=005,cond7=006,cond8=007)
	,%var_set_type2(FIXD_PYMT_IND,0,cond1=0,cond2=1)
    ,%var_set_type4(FUNDNG_CD,YES,cond1=A,cond2=B,cond3=C,cond4=D,cond5=E,cond6=F,cond7=G,cond8=H,cond9=I)
	,%var_set_type2(fundng_src_non_fed_shr_cd,2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06)
    ,%var_set_type2(var=BRDR_STATE_IND,lpad=0,cond1=0,cond2=1)
    ,%var_set_type2(var=XOVR_IND,lpad=0,cond1=0,cond2=1)
    ,%var_set_type1(var=MDCR_HICN_NUM)                       
    ,%var_set_type1(var=MDCR_BENE_ID)                       
    ,%var_set_type1(var=PTNT_CNTL_NUM) 
    ,%var_set_type2(var=HLTH_CARE_ACQRD_COND_CD,lpad=0,cond1=0,cond2=1)
	,%var_set_ptstatus(PTNT_STUS_CD)	
	,%var_set_fills(ADMTG_DGNS_CD,cond1=0,cond2=8,cond3=9,cond4=#)
    ,%var_set_type2(var=ADMTG_DGNS_CD_IND,lpad=0,cond1=1,cond2=2,cond3=3)
	,%var_set_fills(DGNS_1_CD,cond1=0,cond2=8,cond3=9,cond4=#)
    ,%var_set_type2(var=DGNS_1_CD_IND,lpad=0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_1_CD_IND)
	,%var_set_fills(DGNS_2_CD,cond1=0,cond2=8,cond3=9,cond4=#)
    ,%var_set_type2(var=DGNS_2_CD_IND,lpad=0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_2_CD_IND)
	,%var_set_fills(DGNS_3_CD,cond1=0,cond2=8,cond3=9,cond4=#)
    ,%var_set_type2(var=DGNS_3_CD_IND,lpad=0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_3_CD_IND)
	,%var_set_fills(DGNS_4_CD,cond1=0,cond2=8,cond3=9,cond4=#)
    ,%var_set_type2(var=DGNS_4_CD_IND,lpad=0,cond1=1,cond2=2,cond3=3)
    ,%var_set_poa(DGNS_POA_4_CD_IND)
	,%var_set_fills(DGNS_5_CD,cond1=0,cond2=8,cond3=9,cond4=#)
    ,%var_set_type2(var=DGNS_5_CD_IND,lpad=0,cond1=1,cond2=2,cond3=3)
	,%var_set_poa(DGNS_POA_5_CD_IND)
    ,%var_set_type6(NCVRD_DAYS_CNT, 	cond1=88888)
    ,%var_set_type6(NCVRD_CHRGS_AMT,	cond1=888888888.88)
    ,MDCD_CVRD_IP_DAYS_CNT
    ,%var_set_type6(ICF_IID_DAYS_CNT,	cond1=8888)
    ,%var_set_type6(LVE_DAYS_CNT,		cond1=88888)
    ,%var_set_type6(NRSNG_FAC_DAYS_CNT, cond1=88888)
    ,%var_set_type1(var=ADMTG_PRVDR_NPI_NUM)
    ,%var_set_type1(var=ADMTG_PRVDR_NUM)
    ,%var_set_spclty(var=ADMTG_PRVDR_SPCLTY_CD)
	,%var_set_taxo(ADMTG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
    ,%var_set_prtype(var=ADMTG_PRVDR_TYPE_CD)
    ,%var_set_type1(var=BLG_PRVDR_NPI_NUM)
    ,%var_set_type1(var=BLG_PRVDR_NUM)
	,%var_set_taxo(BLG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
    ,%var_set_prtype(var=BLG_PRVDR_TYPE_CD)
    ,%var_set_spclty(var=BLG_PRVDR_SPCLTY_CD)
    ,%var_set_type1(var=RFRG_PRVDR_NUM)
    ,%var_set_type1(var=RFRG_PRVDR_NPI_NUM)
    ,%var_set_prtype(var=RFRG_PRVDR_TYPE_CD)
    ,%var_set_spclty(var=RFRG_PRVDR_SPCLTY_CD)
    ,%var_set_type1(var=PRVDR_LCTN_ID)
    ,%var_set_type6(DAILY_RATE,  		cond1=88888.80, cond2=88888.00, cond3=88888.88)
    ,%var_set_type2(var=PYMT_LVL_IND,lpad=0,cond1=1,cond2=2)
    ,%var_set_type6(LTC_RCP_LBLTY_AMT,  cond1=9999999999.99, cond2=888888888.88)
    ,%var_set_type6(MDCR_PD_AMT, 		cond1=9999999999.99, cond2=888888888.88, cond3=88888888888.00, cond4=88888888888.88, cond5=8888888.88, cond6=99999999999.00)
	,%var_set_type6(TOT_BILL_AMT,	    cond1=9999999.99, cond2=888888888.88, cond3=99999999.90, cond4=999999.99, cond5=999999)
    ,%var_set_type6(TOT_ALOWD_AMT,		cond1=888888888.88, cond2=99999999.00)
    ,%var_set_type6(TOT_MDCD_PD_AMT,	cond1=888888888.88)
    ,%var_set_type6(TOT_MDCR_DDCTBL_AMT, cond1=888888888.88, cond2=99999, cond3=88888888888.00)
    ,%var_set_type6(TOT_MDCR_COINSRNC_AMT, cond1=888888888.88)
    ,%var_set_type6(TOT_TPL_AMT,		cond1=888888888.88, cond2=999999.99)
    ,%var_set_type6(TOT_OTHR_INSRNC_AMT, cond1=888888888.88)
    ,%var_set_type6(TP_COINSRNC_PD_AMT, cond1=888888888.88)
	,%var_set_type6(TP_COPMT_PD_AMT,    cond1=88888888888, cond2=888888888.00, cond3=888888888.88, cond4=99999999999.00)
    ,%var_set_type2(var=MDCR_CMBND_DDCTBL_IND,lpad=0,cond1=0,cond2=1)
    ,%var_set_type2(var=MDCR_REIMBRSMT_TYPE_CD,lpad=2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06,cond7=07,cond8=08,cond9=09)
    ,%var_set_type6(BENE_COINSRNC_AMT,  cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)
    ,%var_set_type6(BENE_COPMT_AMT,		cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)
	,%var_set_type6(BENE_DDCTBL_AMT,    cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)
    ,%var_set_type2(var=COPAY_WVD_IND,lpad=0,cond1=0,cond2=1)
    ,%fix_old_dates(OCRNC_01_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_01_CD_END_DT)
    ,%var_set_type1(var=OCRNC_01_CD)
    ,%fix_old_dates(OCRNC_02_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_02_CD_END_DT)
    ,%var_set_type1(var=OCRNC_02_CD)
    ,%fix_old_dates(OCRNC_03_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_03_CD_END_DT)
    ,%var_set_type1(var=OCRNC_03_CD)
    ,%fix_old_dates(OCRNC_04_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_04_CD_END_DT)
    ,%var_set_type1(var=OCRNC_04_CD)
    ,%fix_old_dates(OCRNC_05_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_05_CD_END_DT)
    ,%var_set_type1(var=OCRNC_05_CD)
    ,%fix_old_dates(OCRNC_06_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_06_CD_END_DT)
    ,%var_set_type1(var=OCRNC_06_CD)
    ,%fix_old_dates(OCRNC_07_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_07_CD_END_DT)
    ,%var_set_type1(var=OCRNC_07_CD)
    ,%fix_old_dates(OCRNC_08_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_08_CD_END_DT)
    ,%var_set_type1(var=OCRNC_08_CD)
    ,%fix_old_dates(OCRNC_09_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_09_CD_END_DT)
    ,%var_set_type1(var=OCRNC_09_CD)
    ,%fix_old_dates(OCRNC_10_CD_EFCTV_DT)
    ,%fix_old_dates(OCRNC_10_CD_END_DT)
    ,%var_set_type1(var=OCRNC_10_CD)
    ,%var_set_type2(var=SPLIT_CLM_IND,lpad=0,cond1=0,cond2=1)
    ,CLL_CNT
    ,NUM_CLL

    /* constructed variables */
    ,ACCOMMODATION_PAID as ACMDTN_PD
    ,ANCILLARY_PAID as ANCLRY_PD
    ,CVRD_MH_DAYS_OVER_65 as CVRD_MH_DAYS_OVR_65
    ,CVRD_MH_DAYS_UNDER_21
    ,LT_MH_DX_IND
    ,LT_SUD_DX_IND
    ,LT_MH_TAXONOMY_IND as LT_MH_TXNMY_IND
    ,LT_SUD_TAXONOMY_IND as LT_SUD_TXNMY_IND
    ,nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as IAP_COND_IND
    ,nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as PRMRY_HIRCHCL_COND

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

    CREATE TEMP TABLE LTL 
    distkey(ORGNL_CLM_NUM)
    AS      
    
	SELECT &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD_LINE || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM_LINE,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM_LINE,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT_LINE) AS CHAR(4)) ||
	CAST(DATE_PART(MONTH,ADJDCTN_DT_LINE) AS CHAR(2)) ||
    CAST(DATE_PART(DAY,ADJDCTN_DT_LINE) AS CHAR(2)) || '-' || 
    COALESCE(LINE_ADJSTMT_IND_CLEAN,'X')) as varchar(126)) as LT_LINK_KEY
    ,%nrbquote('&VERSION.') as LT_VRSN
    ,%nrbquote('&TAF_FILE_DATE.') as LT_FIL_DT
    ,TMSIS_RUN_ID_LINE as TMSIS_RUN_ID  
    ,%var_set_type1(var=MSIS_IDENT_NUM_LINE,new=MSIS_IDENT_NUM)
    ,NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD 
	,%var_set_type3(ORGNL_CLM_NUM_LINE,cond1=~,new=ORGNL_CLM_NUM)
	,%var_set_type3(ADJSTMT_CLM_NUM_LINE,cond1=~,new=ADJSTMT_CLM_NUM)
 	,%var_set_type1(var=ADJSTMT_LINE_NUM)
    ,%var_set_type1(var=ORGNL_LINE_NUM)
    ,case when date_cmp(ADJDCTN_DT_LINE,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT_LINE,'01JAN1960') end as ADJDCTN_DT
    ,LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND
    ,%var_set_tos(TOS_CD)
    ,case when lpad(IMNZTN_TYPE_CD,2,'0') = '88' then NULL 
    	  else %var_set_type5(var=IMNZTN_TYPE_CD,lpad=2,lowerbound=0,upperbound=29,multiple_condition=YES)
    ,%var_set_type2(var=CMS_64_FED_REIMBRSMT_CTGRY_CD,lpad=2,cond1=01,cond2=02,cond3=03,cond4=04)
    ,case when XIX_SRVC_CTGRY_CD in &XIX_SRVC_CTGRY_CD_values. then XIX_SRVC_CTGRY_CD
    	  else null end as XIX_SRVC_CTGRY_CD
    ,case when XXI_SRVC_CTGRY_CD in &XXI_SRVC_CTGRY_CD_values. then XXI_SRVC_CTGRY_CD
    	  else null end as XXI_SRVC_CTGRY_CD
    ,%var_set_type1(var=CLL_STUS_CD)
    ,case when date_cmp(SRVC_BGNNG_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_BGNNG_DT,'01JAN1960') end as SRVC_BGNNG_DT
    ,case when date_cmp(SRVC_ENDG_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(SRVC_ENDG_DT,'01JAN1960') end as SRVC_ENDG_DT
	,%var_set_type5(BNFT_TYPE_CD,lpad=3,lowerbound=001,upperbound=108)
    ,%var_set_type2(var=BLG_UNIT_CD,lpad=2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06,cond7=07)
    ,%var_set_fills(NDC_CD,cond1=0,cond2=8,cond3=9,cond4=#,spaces=YES)
	,%var_set_type4(UOM_CD,YES,cond1=F2,cond2=ML,cond3=GR,cond4=UN,cond5=ME)
    ,%var_set_type6(NDC_QTY, cond1=888888, cond2=888888.888, cond3=999999, cond4=88888.888, cond5=888888.880, cond6=999999.998)
    ,%var_set_type1(var=HCPCS_RATE)
    ,%var_set_type1(var=SRVCNG_PRVDR_NUM)
    ,%var_set_type1(var=SRVCNG_PRVDR_NPI_NUM)
    ,%var_set_taxo(SRVCNG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
    ,%var_set_prtype(var=SRVCNG_PRVDR_TYPE_CD)
    ,%var_set_spclty(var=SRVCNG_PRVDR_SPCLTY_CD)
    ,case
       when PRVDR_FAC_TYPE_CD in ('100000000','170000000','250000000','260000000','270000000','280000000',
                                  '290000000','300000000','310000000','320000000','330000000','340000000','380000000')
          then PRVDR_FAC_TYPE_CD
       else NULL
     end as PRVDR_FAC_TYPE_CD
    ,%var_set_type6(IP_LT_ACTL_SRVC_QTY, new=ACTL_SRVC_QTY,	cond1=88888.888, cond2=99999.990, cond3=999999)
    ,%var_set_type6(IP_LT_ALOWD_SRVC_QTY, new=ALOWD_SRVC_QTY,	cond1=88888.888, cond2=888888.890)
    ,%var_set_type1(var=REV_CD,lpad=4)
    ,%var_set_type6(REV_CHRG_AMT, 	cond1=8888888888.88, cond2=88888888.88, cond3=888888888.88, cond4=88888888888.88, cond5=99999999.90)
    ,%var_set_type6(ALOWD_AMT,		cond1=888888888.88, cond2=99999999.00, cond3=9999999999.99) 
    ,%var_set_type6(MDCD_PD_AMT,	cond1=888888888.88)
    ,%var_set_type6(OTHR_INSRNC_AMT,		cond1=888888888.88, cond2=88888888888.00, cond3=88888888888.88)
	,%var_set_type6(MDCD_FFS_EQUIV_AMT, 	cond1=888888888.88, cond2=88888888888.80, cond3=999999.99)
	,%var_set_type6(TPL_AMT,		cond1=888888888.88)
	,RN as LINE_NUM 

		FROM 	(select *,
     			 case when LINE_ADJSTMT_IND is NOT NULL and    
               trim(LINE_ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(LINE_ADJSTMT_IND)     else NULL   end  as LINE_ADJSTMT_IND_CLEAN 
     from &FL._LINE) H
   ) BY TMSIS_PASSTHROUGH;

%DROP_temp_tables(&fl._LINE);

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
		(DA_RUN_ID
		,LT_LINK_KEY
    	,LT_VRSN
    	,LT_FIL_DT
    	,TMSIS_RUN_ID  
    	,MSIS_IDENT_NUM
    	,SUBMTG_STATE_CD 
    	,ORGNL_CLM_NUM
    	,ADJSTMT_CLM_NUM
 		,ADJSTMT_LINE_NUM
    	,ORGNL_LINE_NUM
    	,ADJDCTN_DT
    	,LINE_ADJSTMT_IND
    	,TOS_CD
    	,IMNZTN_TYPE_CD
    	,CMS_64_FED_REIMBRSMT_CTGRY_CD
	    ,XIX_SRVC_CTGRY_CD
    	,XXI_SRVC_CTGRY_CD
    	,CLL_STUS_CD
		,SRVC_BGNNG_DT
    	,SRVC_ENDG_DT
		,BNFT_TYPE_CD
    	,BLG_UNIT_CD
    	,NDC_CD
		,UOM_CD
    	,NDC_QTY
    	,HCPCS_RATE
		,SRVCNG_PRVDR_NUM
    	,SRVCNG_PRVDR_NPI_NUM
    	,SRVCNG_PRVDR_TXNMY_CD
    	,SRVCNG_PRVDR_TYPE_CD
    	,SRVCNG_PRVDR_SPCLTY_CD
    	,PRVDR_FAC_TYPE_CD
    	,ACTL_SRVC_QTY
    	,ALOWD_SRVC_QTY
    	,REV_CD
    	,REV_CHRG_AMT
		,ALOWD_AMT
    	,MDCD_PD_AMT
    	,OTHR_INSRNC_AMT
		,MDCD_FFS_EQUIV_AMT
		,TPL_AMT
		,LINE_NUM 
		)	
	SELECT * 
	FROM &FL.L
   ) BY TMSIS_PASSTHROUGH;

	select line_ct into : LINE_CT
	from (select * from connection to tmsis_passthrough
          (select count(submtg_state_cd) as line_ct
	      from &FL.L));

%MEND BUILD_LT;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CLT00001;

  a.TMSIS_RUN_ID          
, a.TMSIS_FIL_NAME        
, a.TMSIS_ACTV_IND        
, a.FIL_CREATD_DT                             
, a.PRD_END_TIME                             
, a.FIL_NAME                                 
, a.FIL_STUS_CD                                               
, a.SQNC_NUM                                       
, a.PRD_EFCTV_TIME                          
, a.SUBMTG_STATE_CD                         
, a.TOT_REC_CNT           

%mend  CLT00001;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CLT00002;

  a.TMSIS_RUN_ID               
, upper(a.TMSIS_FIL_NAME) as tmsis_fil_name             
, a.TMSIS_ACTV_IND             
, upper(a.SECT_1115A_DEMO_IND) as sect_1115a_demo_ind                               
, coalesce(a.ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT
, coalesce(upper(a.ADJSTMT_IND),'X') AS ADJSTMT_IND
, upper(a.ADJSTMT_RSN_CD) as adjstmt_rsn_cd                                     
, a.ADMSN_DT                                                  
, upper(a.ADMSN_HR_NUM) as admsn_hr_num                                             
, trim(translate(upper(a.ADMTG_DGNS_CD),'.','')) as admtg_dgns_cd                                        
, upper(a.ADMTG_DGNS_CD_IND) as admtg_dgns_cd_ind                              
, upper(a.ADMTG_PRVDR_NPI_NUM) as admtg_prvdr_npi_num                            
, upper(a.ADMTG_PRVDR_NUM) as admtg_prvdr_num                                    
, upper(a.ADMTG_PRVDR_SPCLTY_CD) as admtg_prvdr_spclty_cd                          
, upper(a.ADMTG_PRVDR_TXNMY_CD) as admtg_prvdr_txnmy_cd                             
, upper(a.ADMTG_PRVDR_TYPE_CD) as admtg_prvdr_type_cd                               
, a.SRVC_BGNNG_DT                                            
, a.BENE_COINSRNC_AMT                               
, a.BENE_COINSRNC_PD_DT                            
, a.BENE_COPMT_AMT                                    
, a.BENE_COPMT_PD_DT                                  
, a.BENE_DDCTBL_AMT                                    
, a.BENE_DDCTBL_PD_DT                                
, upper(a.BLG_PRVDR_NPI_NUM) as blg_prvdr_npi_num                               
, upper(a.BLG_PRVDR_NUM) as blg_prvdr_num                                      
, upper(a.BLG_PRVDR_SPCLTY_CD) as blg_prvdr_spclty_cd                             
, upper(a.BLG_PRVDR_TXNMY_CD) as blg_prvdr_txnmy_cd                                 
, upper(a.BLG_PRVDR_TYPE_CD) as blg_prvdr_type_cd                                 
, upper(a.BRDR_STATE_IND) as brdr_state_ind                                      
, a.CHK_EFCTV_DT                                         
, upper(a.CHK_NUM) as chk_num                                                  
, upper(a.CLM_DND_IND) as clm_dnd_ind                                         
, a.CLL_CNT                                             
, upper(a.CLM_PYMT_REMIT_1_CD) as clm_pymt_remit_1_cd                              
, upper(a.CLM_PYMT_REMIT_2_CD) as clm_pymt_remit_2_cd                             
, upper(a.CLM_PYMT_REMIT_3_CD) as clm_pymt_remit_3_cd                             
, upper(a.CLM_PYMT_REMIT_4_CD) as clm_pymt_remit_4_cd                         
, upper(a.CLM_STUS_CD) as clm_stus_cd                                              
, upper(a.CLM_STUS_CTGRY_CD) as clm_stus_ctgry_cd                                  
, upper(a.COPAY_WVD_IND) as copay_wvd_ind                                       
, upper(a.XOVR_IND) as xovr_ind                                                 
, a.DAILY_RATE                                             
, a.BIRTH_DT                                                      
, trim(translate(upper(a.DGNS_1_CD),'.','')) as dgns_1_cd                                             
, trim(translate(upper(a.DGNS_2_CD),'.','')) as dgns_2_cd                                             
, trim(translate(upper(a.DGNS_3_CD),'.','')) as dgns_3_cd                                             
, trim(translate(upper(a.DGNS_4_CD),'.','')) as dgns_4_cd                                             
, trim(translate(upper(a.DGNS_5_CD),'.','')) as dgns_5_cd                                             
, upper(a.DGNS_1_CD_IND) as dgns_1_cd_ind                                     
, upper(a.DGNS_2_CD_IND) as dgns_2_cd_ind                                   
, upper(a.DGNS_3_CD_IND) as dgns_3_cd_ind                                     
, upper(a.DGNS_4_CD_IND) as dgns_4_cd_ind                                       
, upper(a.DGNS_5_CD_IND) as dgns_5_cd_ind                                       
, upper(a.DGNS_POA_1_CD_IND) as dgns_poa_1_cd_ind                                  
, upper(a.DGNS_POA_2_CD_IND) as dgns_poa_2_cd_ind                                  
, upper(a.DGNS_POA_3_CD_IND) as dgns_poa_3_cd_ind                               
, upper(a.DGNS_POA_4_CD_IND) as dgns_poa_4_cd_ind                               
, upper(a.DGNS_POA_5_CD_IND) as dgns_poa_5_cd_ind     
, a.DSCHRG_DT                                               
, upper(a.DSCHRG_HR_NUM) as dschrg_hr_num                                          
, upper(a.ELGBL_1ST_NAME) as elgbl_1st_name                                     
, upper(a.ELGBL_LAST_NAME) as elgbl_last_name                                  
, upper(a.ELGBL_MDL_INITL_NAME) as elgbl_mdl_initl_name                               
, coalesce(a.SRVC_ENDG_DT,'01JAN1960') as SRVC_ENDG_DT                                            
, upper(a.FIXD_PYMT_IND) as fixd_pymt_ind                                        
, upper(a.FRCD_CLM_CD) as frcd_clm_cd                                          
, upper(a.FUNDNG_CD) as fundng_cd                                            
, upper(a.FUNDNG_SRC_NON_FED_SHR_CD) as fundng_src_non_fed_shr_cd                     
, upper(a.HLTH_CARE_ACQRD_COND_CD) as hlth_care_acqrd_cond_cd                  
, upper(a.HH_ENT_NAME) as hh_ent_name                                   
, upper(a.HH_PRVDR_IND) as hh_prvdr_ind                                
, upper(a.HH_PRVDR_NPI_NUM) as hh_prvdr_npi_num                            
, a.ICF_IID_DAYS_CNT                                
, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') AS ADJSTMT_CLM_NUM                                       
, coalesce(upper(a.ORGNL_CLM_NUM),'~') AS ORGNL_CLM_NUM                                     
, a.LVE_DAYS_CNT                                           
, a.LTC_RCP_LBLTY_AMT                                 
, case when a.MDCD_CVRD_IP_DAYS_CNT=88888 then null else a.MDCD_CVRD_IP_DAYS_CNT end as MDCD_CVRD_IP_DAYS_CNT
, a.MDCD_PD_DT                                              
, upper(a.MDCR_BENE_ID) as mdcr_bene_id                                         
, upper(a.MDCR_CMBND_DDCTBL_IND) as mdcr_cmbnd_ddctbl_ind                            
, upper(a.MDCR_HICN_NUM) as mdcr_hicn_num                                         
, a.MDCR_PD_AMT                                          
, upper(a.MDCR_REIMBRSMT_TYPE_CD) as mdcr_reimbrsmt_type_cd                             
, upper(a.MSIS_IDENT_NUM) as msis_ident_num                                      
, upper(a.NATL_HLTH_CARE_ENT_ID) as natl_hlth_care_ent_id                       
, a.NCVRD_CHRGS_AMT                                      
, a.NCVRD_DAYS_CNT                                        
, a.NRSNG_FAC_DAYS_CNT                                 
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
, upper(a.PTNT_CNTL_NUM) as ptnt_cntl_num                  
, upper(a.PTNT_STUS_CD) as ptnt_stus_cd                       
, upper(a.PYMT_LVL_IND) as pymt_lvl_ind                       
, upper(a.PLAN_ID_NUM) as mc_plan_id                         
, upper(a.PGM_TYPE_CD) as pgm_type_cd                             
, a.REC_NUM  
, upper(a.PRVDR_LCTN_ID) as prvdr_lctn_id 
, upper(a.RFRG_PRVDR_NPI_NUM) as rfrg_prvdr_npi_num    
, upper(a.RFRG_PRVDR_NUM) as rfrg_prvdr_num                 
, upper(a.RFRG_PRVDR_SPCLTY_CD) as rfrg_prvdr_spclty_cd    
, upper(a.RFRG_PRVDR_TXNMY_CD) as rfrg_prvdr_txnmy_cd  
, upper(a.RFRG_PRVDR_TYPE_CD) as rfrg_prvdr_type_cd     
, upper(a.RMTNC_NUM) as rmtnc_num                                          
, a.SRVC_TRKNG_PYMT_AMT                         
, upper(a.SRVC_TRKNG_TYPE_CD) as srvc_trkng_type_cd       
, upper(a.SRC_LCTN_CD) as src_lctn_cd                          
, upper(a.SPLIT_CLM_IND) as split_clm_ind      
, upper(a.STATE_NOTN_TXT) as state_notn_txt                 
, upper(a.SBMTR_ID) as sbmtr_id                         
, a.SUBMTG_STATE_CD                
, a.TP_COINSRNC_PD_AMT                          
, a.TP_COINSRNC_PD_DT                         
, a.TP_COPMT_PD_AMT                                 
, a.TP_COPMT_PD_DT                                 
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
, upper(a.WVR_ID) as wvr_id                                     
, upper(a.WVR_TYPE_CD) as wvr_type_cd                        

%mend CLT00002;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CLT00003;
  a.TMSIS_RUN_ID as TMSIS_RUN_ID_LINE                
, a.TMSIS_FIL_NAME               
, a.TMSIS_OFST_BYTE_NUM          
, a.TMSIS_COMT_ID                
, a.TMSIS_DLTD_IND               
, a.TMSIS_OBSLT_IND              
, a.TMSIS_ACTV_IND as TMSIS_ACTV_IND_LINE               
, a.TMSIS_SQNC_NUM               
, a.TMSIS_RUN_TS                 
, a.TMSIS_RPTG_PRD               
, a.TMSIS_REC_MD5_B64_TXT        
, a.REC_TYPE_CD                  
, coalesce(a.ADJDCTN_DT, '01JAN1960') AS ADJDCTN_DT_LINE                                      
, a.ALOWD_AMT                                      
, coalesce(a.SRVC_BGNNG_DT,'01JAN1960') as SRVC_BGNNG_DT                           
, upper(a.BNFT_TYPE_CD) as bnft_type_cd                
, upper(a.BLG_UNIT_CD) as blg_unit_cd                     
, upper(a.CLL_STUS_CD) as cll_stus_cd             
, upper(a.CMS_64_FED_REIMBRSMT_CTGRY_CD) as cms_64_fed_reimbrsmt_ctgry_cd
, coalesce(a.SRVC_ENDG_DT,'01JAN1960') as SRVC_ENDG_DT                                 
, upper(a.HCPCS_RATE) as hcpcs_rate    
, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') AS ADJSTMT_CLM_NUM_LINE                         
, coalesce(upper(a.ORGNL_CLM_NUM),'~') AS ORGNL_CLM_NUM_LINE                                  
, upper(a.IMNZTN_TYPE_CD) as imnztn_type_cd             
, a.IP_LT_ACTL_SRVC_QTY              
, a.IP_LT_ALOWD_SRVC_QTY          
, coalesce(upper(a.LINE_ADJSTMT_IND),'X') as LINE_ADJSTMT_IND
, upper(a.ADJSTMT_LINE_RSN_CD) as adjstmt_line_rsn_cd
, upper(a.ADJSTMT_LINE_NUM) as adjstmt_line_num
, upper(a.ORGNL_LINE_NUM) as orgnl_line_num        
, a.MDCD_FFS_EQUIV_AMT                   
, case when a.MDCD_PD_AMT != 888888888.88 then a.MDCD_PD_AMT 
	   else NULL end as MDCD_PD_AMT 
, upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM_LINE                         
, upper(a.NDC_CD) as ndc_cd                                    
, a.NDC_QTY                                      
, upper(a.NDC_UOM_CD) as uom_cd                                   
, a.OTHR_INSRNC_AMT                      
, upper(a.OTHR_TPL_CLCTN_CD) as othr_tpl_clctn_cd                       
, upper(a.PRE_AUTHRZTN_NUM) as pre_authrztn_num                                 
, upper(a.PRVDR_FAC_TYPE_CD) as prvdr_fac_type_cd  
, a.REC_NUM                                          
, a.REV_CHRG_AMT                                 
, upper(a.REV_CD) as rev_cd                                     
, upper(a.SELF_DRCTN_TYPE_CD) as self_drctn_type_cd                      
, upper(a.PRSCRBNG_PRVDR_NPI_NUM) as SRVCNG_PRVDR_NPI_NUM
, upper(a.SRVCNG_PRVDR_NUM) as srvcng_prvdr_num                          
, upper(a.SRVCNG_PRVDR_SPCLTY_CD) as srvcng_prvdr_spclty_cd            
, upper(a.SRVCNG_PRVDR_TXNMY_CD) as srvcng_prvdr_txnmy_cd                   
, upper(a.SRVCNG_PRVDR_TYPE_CD) as srvcng_prvdr_type_cd                 
, upper(a.STATE_NOTN_TXT) as state_notn_txt                                 
, upper(a.SBMTR_ID) as sbmtg_id                                        
, upper(a.SUBMTG_STATE_CD) as SUBMTG_STATE_CD_LINE                           
, a.TPL_AMT                                        
, upper(a.STC_CD) as tos_cd                                              
, upper(lpad(trim(a.XIX_SRVC_CTGRY_CD),4,'0')) as XIX_SRVC_CTGRY_CD               
, upper(lpad(trim(a.XXI_SRVC_CTGRY_CD),3,'0')) as XXI_SRVC_CTGRY_CD

%mend CLT00003;




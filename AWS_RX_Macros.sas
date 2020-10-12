/************************************************************************************************************/
/* Program:		AWS_RX_Macros.sas																		    */
/* Author:		Deo S. Bencio																				*/
/* Date:		12/1/2016																					*/
/* Purpose:		Program contains macros that massage data from various segments in T-MSIS				    */
/*				THIS PROGRAM DOES NOT STAND ALONE.                                                          */
/*				RX_build.sas - pull program for RX build													*/
/*																	                                        */
/* Modified:    12/12/2016 - CA update RX list of variables and remove eligibility macros                   */
/*              2/20/2017  - CA change macro calls for pulling header and claims family segments to include */
/*                           distribution and sort keys & dedupe claims family and header tables before join*/                                                    
/*              3/2/2017   - CA recode PA Chip and IA Chip to state code. Change row number to a macro var  */                                                                                          
/*                                                                                                          */
/*              6/20/2017  - CA modified to include additional denied claim criteria for header & line files*/
/*              4/17/2018  - Rosie Malsberger modified to incorporate all CCB changes                       */
/*				10/4/2018  - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q3				*/
/*						     recode 0 in othr_toc_rx_clm_actl_qty to null									*/
/*				3/7/2019   - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q1.xlsx			*/
/*							 Added column LINE_NUM to identify line numbers in LINE table					*/
/*							 Renamed OTHR_TOC_RX_CLM_ACTL_QTY to ACTL_SRVC_QTY and                          */
/*                                   OTHR_TOC_RX_CLM_ALOWD_QTY to ALOWD_SRVC_QTY                            */
/*				9/22/2019 - DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q3                 */
/*							Upcased ICN ORIG and ICN ADJSTMT at the FA Header/Line Join						*/
/*				6/9/2020  - DB modified to apply TAF CCB 2020 Q2 Change Request                             */
/*                                                                                                          */
/************************************************************************************************************/
options SASTRACE=',,,ds' SASTRACELOC=Saslog nostsuffix dbidirectexec sqlgeneration=dbms msglevel=I sql_ip_trace=source;

options spool;


/* pull RX line item records (FIRST NON DENIED LINE) and join to final action header records dataset */
%macro AWS_extract_line_RX (tmsis_schema, fl, tab_no, _2x_segment, analysis_date);
	/** Create a temporary line file **/
execute (

		create temp table &FL._LINE_IN
		distkey (ORGNL_CLM_NUM_LINE) 
		sortkey (SUBMTG_STATE_CD,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
		as
		select  SUBMTG_STATE_CD, %&tab_no

		from	&TMSIS_SCHEMA..&_2x_segment  A

		where  A.TMSIS_ACTV_IND = 1 							/* include active indicator = 1 */
			   and (a.submtg_state_cd,a.tmsis_run_id) in (&combined_list)

	) by tmsis_passthrough;
	execute (

	create temp table &FL._LINE 
	distkey (ORGNL_CLM_NUM_LINE) 
	sortkey (NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
	as

	select   row_number() over (partition by A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM_LINE,A.ADJSTMT_CLM_NUM_LINE,A.ADJDCTN_DT_LINE,A.LINE_ADJSTMT_IND 
		     order by A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM_LINE,A.ADJSTMT_CLM_NUM_LINE,A.ADJDCTN_DT_LINE,A.LINE_ADJSTMT_IND,A.TMSIS_FIL_NAME,A.REC_NUM ) as RN  

	        ,a.*
			,CASE
			  WHEN A.SUBMTG_STATE_CD = '97' THEN '42'
			  WHEN A.SUBMTG_STATE_CD = '96' THEN '19'
			  WHEN A.SUBMTG_STATE_CD = '94' THEN '30'
	          WHEN A.SUBMTG_STATE_CD = '93' THEN '56'
			  ELSE A.SUBMTG_STATE_CD
             END AS NEW_SUBMTG_STATE_CD_LINE
            ,CASE
              WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
              ELSE SUBSTRING(A.DRUG_UTLZTN_CD,1,2)
			 END AS RSN_SRVC_CD
			,CASE
              WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
              ELSE SUBSTRING(A.DRUG_UTLZTN_CD,3,2)
			 END AS PROF_SRVC_CD
			,CASE
              WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
              ELSE SUBSTRING(A.DRUG_UTLZTN_CD,5,2)
			 END AS RSLT_SRVC_CD
  	from 	&FL._LINE_IN A 

   inner join FA_HDR_&FL as HEADER
	on HEADER.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM_LINE
	and HEADER.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM_LINE
	and HEADER.ADJDCTN_DT = A.ADJDCTN_DT_LINE
	and HEADER.ADJSTMT_IND =A.LINE_ADJSTMT_IND
	and HEADER.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD
	and HEADER.TMSIS_RUN_ID = A.TMSIS_RUN_ID_LINE

	) by tmsis_passthrough;

	/* Pull out maximum row_number for each partition */
	execute (

	create temp table RN_&FL.
	distkey (ORGNL_CLM_NUM_LINE) 
	sortkey (NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND)
	as
	
	select	NEW_SUBMTG_STATE_CD_LINE
			, ORGNL_CLM_NUM_LINE
			, ADJSTMT_CLM_NUM_LINE
			, ADJDCTN_DT_LINE
			, LINE_ADJSTMT_IND
			, max(RN) as NUM_CLL

	from	&FL._LINE

	group by NEW_SUBMTG_STATE_CD_LINE,ORGNL_CLM_NUM_LINE,ADJSTMT_CLM_NUM_LINE,ADJDCTN_DT_LINE,LINE_ADJSTMT_IND

	) by tmsis_passthrough;



	/* Attach num_cll variable to header records as per instruction */
	execute (

	create temp table &fl._HEADER
	distkey (ORGNL_CLM_NUM) 
	sortkey (NEW_SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
	as
	
	select  HEADER.*
			, coalesce(RN.NUM_CLL,0) as NUM_CLL

	from 	FA_HDR_&FL. HEADER left join RN_&FL. RN							

  	on   	HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
		 	HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and 
		 	HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
			HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
			HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND

	) by tmsis_passthrough;
	  
 
	%Get_Audt_counts_clms(&DA_SCHEMA.,&DA_RUN_ID., AWS_RX_Macros, 1.1 AWS_Extract_Line_RX);

%DROP_temp_tables(RN_&FL);
%DROP_temp_tables(FA_HDR_&FL);
%DROP_temp_tables(&FL._LINE_IN);
%MEND AWS_extract_line_RX;

%MACRO BUILD_RX();

   /* ORDER VARIABLES AND UPCASE, LEFT PAD WITH ZEROS AND RESET COALESCE VALUES HEADER FILE*/
   execute (

   CREATE TEMP TABLE RXH 
   distkey(ORGNL_CLM_NUM)
   AS      
   select  &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT) AS CHAR(4))|| 
	CAST(DATE_PART(MONTH,ADJDCTN_DT) AS CHAR(2))|| 
    CAST(DATE_PART(DAY,ADJDCTN_DT) AS CHAR(2)) || '-' || COALESCE(ADJSTMT_IND_CLEAN,'X')) 
	as varchar(126)) as RX_LINK_KEY
   ,%nrbquote('&VERSION.') as RX_VRSN 
   ,%nrbquote('&TAF_FILE_DATE.') as RX_FIL_DT
   ,tmsis_run_id 
   ,%var_set_type1(MSIS_IDENT_NUM)
   ,new_submtg_state_cd as submtg_state_cd 
   ,%var_set_type3(orgnl_clm_num,cond1=~)
   ,%var_set_type3(adjstmt_clm_num,cond1=~)
   ,ADJSTMT_IND_CLEAN as ADJSTMT_IND
   ,%var_set_rsn(ADJSTMT_RSN_CD)
   ,case when date_cmp(ADJDCTN_DT,'1600-01-01')=-1 then '1599-12-31'::date else nullif(ADJDCTN_DT,'01JAN1960') end as ADJDCTN_DT
   ,%fix_old_dates(mdcd_pd_dt)
   ,rx_fill_dt
   ,%fix_old_dates(prscrbd_dt) 
   ,%var_set_type2(CMPND_DRUG_IND,0,cond1=0,cond2=1)
   ,%var_set_type2(SECT_1115A_DEMO_IND,0,cond1=0,cond2=1) 	
   ,case when upper(clm_type_cd) in('1','2','3','4','5','A','B','C','D','E','U','V','W','X','Y','Z') then upper(clm_type_cd)
      else NULL
    end as clm_type_cd
   ,case when lpad(pgm_type_cd,2,'0') in ('06','09') then NULL 
	  else %var_set_type5(pgm_type_cd,lpad=2,lowerbound=0,upperbound=17,multiple_condition=YES)
   ,%var_set_type1(MC_PLAN_ID)
   ,%var_set_type1(ELGBL_LAST_NAME,upper=YES)                             
   ,%var_set_type1(ELGBL_1ST_NAME,upper=YES)                              
   ,%var_set_type1(ELGBL_MDL_INITL_NAME,upper=YES)                       
   ,%fix_old_dates(BIRTH_DT) 
   ,%var_set_type5(wvr_type_cd,lpad=2,lowerbound=1,upperbound=33)
   ,%var_set_type1(WVR_ID)
   ,%var_set_type2(srvc_trkng_type_cd,2,cond1=00,cond2=01,cond3=02,cond4=03,cond5=04,cond6=05,cond7=06)
   ,%var_set_type6(SRVC_TRKNG_PYMT_AMT,	cond1=888888888.88)
   ,%var_set_type2(OTHR_INSRNC_IND,0,cond1=0,cond2=1)
   ,%var_set_type2(othr_tpl_clctn_cd,3,cond1=000,cond2=001,cond3=002,cond4=003,cond5=004,cond6=005,cond7=006,cond8=007)
   ,%var_set_type2(FIXD_PYMT_IND,0,cond1=0,cond2=1)
   ,%var_set_type4(FUNDNG_CD,YES,cond1=A,cond2=B,cond3=C,cond4=D,cond5=E,cond6=F,cond7=G,cond8=H,cond9=I)
   ,%var_set_type2(fundng_src_non_fed_shr_cd,2,cond1=01,cond2=02,cond3=03,cond4=04,cond5=05,cond6=06)
   ,%var_set_type2(BRDR_STATE_IND,0,cond1=0,cond2=1)
   ,%var_set_type2(XOVR_IND,0,cond1=0,cond2=1)
   ,%var_set_type1(MDCR_HICN_NUM)
   ,%var_set_type1(MDCR_BENE_ID)
   ,%var_set_type1(BLG_PRVDR_NUM)
   ,%var_set_type1(BLG_PRVDR_NPI_NUM)
   ,%var_set_taxo(BLG_PRVDR_TXNMY_CD,cond1=8888888888, cond2=9999999999, cond3=000000000X, cond4=999999999X,
									  cond5=NONE, cond6=XXXXXXXXXX, cond7=NO TAXONOMY)
   ,%var_set_spclty(BLG_PRVDR_SPCLTY_CD)
   ,%var_set_type1(PRSCRBNG_PRVDR_NUM)
   ,%var_set_type1(SRVCNG_PRVDR_NPI_NUM)
   ,%var_set_type1(DSPNSNG_PD_PRVDR_NPI_NUM)
   ,%var_set_type1(DSPNSNG_PD_PRVDR_NUM)
   ,%var_set_type1(PRVDR_LCTN_ID)
   ,%var_set_type2(PYMT_LVL_IND,0,cond1=1,cond2=2)
   ,%var_set_type6(tot_bill_amt, 		cond1=999999.99, cond2=69999999999.93, cond3=999999.00, cond4=888888888.88, cond5=9999999.99, cond6=99999999.90)                    
   ,%var_set_type6(tot_alowd_amt,		cond1=888888888.88, cond2=99999999.00)                   
   ,%var_set_type6(tot_mdcd_pd_amt, 	cond1=999999.99, cond2=888888888.88)                
   ,%var_set_type6(tot_copay_amt,		cond1=88888888888.00, cond2=888888888.88, cond3=9999999.99)                  
   ,%var_set_type6(tot_tpl_amt, 		cond1=999999.99, cond2=888888888.88)
   ,%var_set_type6(tot_othr_insrnc_amt, cond1=888888888.88)
   ,%var_set_type6(tot_mdcr_ddctbl_amt, cond1=99999, cond2=88888888888.00, cond3=888888888.88)
   ,%var_set_type6(tot_mdcr_coinsrnc_amt, cond1=888888888.88)
   ,%var_set_type6(TP_COINSRNC_PD_AMT,	cond1=888888888.88)
   ,%var_set_type6(TP_COPMT_PD_AMT,		cond1=99999999999.00, cond2=888888888.88, cond3=888888888.00, cond4=88888888888.00)
   ,%var_set_type6(bene_coinsrnc_amt, 	cond1=888888888.88, cond2=888888888.00, cond3=88888888888.00)                
   ,%var_set_type6(bene_copmt_amt,		cond1=88888888888.00, cond2=888888888.88, cond3=888888888.00)
   ,%var_set_type6(bene_ddctbl_amt,		cond1=88888888888.00, cond2=888888888.88, cond3=888888888.00)
   ,%var_set_type2(COPAY_WVD_IND,0,cond1=0,cond2=1)
   ,cll_cnt
   ,num_cll 

	from 	(select *,
     case when ADJSTMT_IND is NOT NULL and    
               trim(ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(ADJSTMT_IND)     else NULL   end as ADJSTMT_IND_CLEAN 
     from &fl._HEADER) H

  ) BY TMSIS_PASSTHROUGH;

%DROP_temp_tables(&FL._HEADER);

   execute (

    CREATE TEMP TABLE &FL.L 
    distkey(ORGNL_CLM_NUM)
    AS      
    select  &DA_RUN_ID as DA_RUN_ID
	,cast ((%nrbquote('&VERSION.') || '-' || &TAF_FILE_DATE. || '-' || NEW_SUBMTG_STATE_CD_LINE || '-' ||
	trim(COALESCE(NULLIF(ORGNL_CLM_NUM_LINE,'~'),'0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM_LINE,'~'),'0')) || '-' || 
    CAST(DATE_PART_YEAR(ADJDCTN_DT_LINE) AS CHAR(4)) ||
	CAST(DATE_PART(MONTH,ADJDCTN_DT_LINE) AS CHAR(2)) ||
    CAST(DATE_PART(DAY,ADJDCTN_DT_LINE) AS CHAR(2)) || '-' || 
    COALESCE(LINE_ADJSTMT_IND_CLEAN,'X')) as varchar(126)) as RX_LINK_KEY
    ,%nrbquote('&VERSION.') as RX_VRSN 
    ,%nrbquote('&TAF_FILE_DATE.') as RX_FIL_DT
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
    ,%var_set_fills(NDC_CD, cond1=0, cond2=8, cond3=9, cond4=#)
    ,%var_set_type4(UOM_CD,YES,cond1=F2,cond2=ML,cond3=GR,cond4=UN,cond5=ME,cond6=EA,cond7=GM)
    ,%var_set_type6(suply_days_cnt, cond1=8888, cond2=999, cond3=0)        
    ,%var_set_type5(NEW_REFL_IND,lpad=2,lowerbound=0,upperbound=98)
    ,%var_set_type2(BRND_GNRC_IND,0,cond1=0,cond2=1,cond3=2,cond4=3,cond5=4)
    ,%var_set_type6(dspns_fee_amt, cond1=88888.88) 
    ,case when trim(DRUG_UTLZTN_CD) is not NULL then upper(DRUG_UTLZTN_CD)
	  else NULL
     end as DRUG_UTLZTN_CD
    ,%var_set_type6(dtl_mtrc_dcml_qty, cond1=999999.999)
    ,case when lpad(CMPND_DSG_FORM_CD,2,'0') in ('08','09') then NULL 
	  else %var_set_type5(CMPND_DSG_FORM_CD,lpad=2,lowerbound=1,upperbound=18,multiple_condition=YES)
    ,%var_set_type2(REBT_ELGBL_IND,0,cond1=0,cond2=1,cond3=2)
    ,%var_set_type5(IMNZTN_TYPE_CD,lpad=2,lowerbound=0,upperbound=29)
    ,%var_set_type5(BNFT_TYPE_CD,lpad=3,lowerbound=001,upperbound=108)
    ,%var_set_type6(othr_toc_rx_clm_alowd_qty, new=alowd_srvc_qty, cond1=99999, cond2=99999.999, cond3=888888.000, cond4=999999, cond5=888888.880)
    ,%var_set_type6(othr_toc_rx_clm_actl_qty, new=actl_srvc_qty, cond1=999999.99, cond2=888888, cond3=999999, cond4=0)
    ,%var_set_type2(CMS_64_FED_REIMBRSMT_CTGRY_CD,2,cond1=01,cond2=02,cond3=03,cond4=04)
    ,case when XIX_SRVC_CTGRY_CD in &XIX_SRVC_CTGRY_CD_values. then XIX_SRVC_CTGRY_CD
     else null end as XIX_SRVC_CTGRY_CD
     ,case when XXI_SRVC_CTGRY_CD in &XXI_SRVC_CTGRY_CD_values. then XXI_SRVC_CTGRY_CD
     else null end as XXI_SRVC_CTGRY_CD
    ,%var_set_type1(CLL_STUS_CD)
    ,%var_set_type6(bill_amt, 		cond1=9999999999.99, cond2=999999.99, cond3=999999, cond4=888888888.88)  
    ,%var_set_type6(alowd_amt, 		cond1=9999999999.99, cond2=888888888.88, cond3=99999999.00)                        
    ,%var_set_type6(copay_amt,		cond1=888888888.88, cond2=88888888888.00)
    ,%var_set_type6(tpl_amt,		cond1=888888888.88)
    ,%var_set_type6(mdcd_pd_amt,	cond1=888888888.88)
    ,%var_set_type6(mdcr_pd_amt,	cond1=88888888888.88, cond2=99999999999.00, cond3=888888888.88, cond4=88888888888.00, cond5=8888888.88, cond6=9999999999.99)
    ,%var_set_type6(mdcd_ffs_equiv_amt, cond1=999999.99, cond2=888888888.88, cond3=88888888888.80)
	,%var_set_type6(mdcr_coinsrnc_pd_amt, cond1=88888888888.00, cond2=888888888.88)
	,%var_set_type6(mdcr_ddctbl_amt, cond1=88888888888.00, cond2=888888888.88)
    ,%var_set_type6(othr_insrnc_amt, cond1=88888888888.00, cond2=88888888888.88, cond3=888888888.88)
    ,%var_set_type1(RSN_SRVC_CD,upper=YES)
    ,%var_set_type1(PROF_SRVC_CD,upper=YES)
    ,%var_set_type1(RSLT_SRVC_CD,upper=YES)
	,RN as LINE_NUM

		FROM 	(select *,
     			 case when LINE_ADJSTMT_IND is NOT NULL and    
               trim(LINE_ADJSTMT_IND)   in ('0' , '1' , '2' , '3' , '4' , '5' , '6') 
          then    trim(LINE_ADJSTMT_IND)     else NULL   end  as LINE_ADJSTMT_IND_CLEAN 
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
		(DA_RUN_ID
   		,RX_LINK_KEY
    	,RX_VRSN 
    	,RX_FIL_DT
    	,TMSIS_RUN_ID  
    	,MSIS_IDENT_NUM 
    	,SUBMTG_STATE_CD
    	,ORGNL_CLM_NUM
    	,ADJSTMT_CLM_NUM
    	,ORGNL_LINE_NUM                           
    	,ADJSTMT_LINE_NUM                         
    	,ADJDCTN_DT
    	,LINE_ADJSTMT_IND
    	,TOS_CD
    	,NDC_CD
    	,UOM_CD
    	,suply_days_cnt        
    	,NEW_REFL_IND
    	,BRND_GNRC_IND
    	,dspns_fee_amt 
    	,DRUG_UTLZTN_CD
    	,dtl_mtrc_dcml_qty
    	,CMPND_DSG_FORM_CD
    	,REBT_ELGBL_IND
    	,IMNZTN_TYPE_CD
    	,BNFT_TYPE_CD
    	,alowd_srvc_qty
    	,actl_srvc_qty
    	,CMS_64_FED_REIMBRSMT_CTGRY_CD
    	,XIX_SRVC_CTGRY_CD
    	,XXI_SRVC_CTGRY_CD
    	,CLL_STUS_CD
    	,bill_amt  
    	,alowd_amt                        
    	,copay_amt
    	,tpl_amt
    	,mdcd_pd_amt
    	,mdcr_pd_amt
		,mdcd_ffs_equiv_amt
		,mdcr_coinsrnc_pd_amt
		,mdcr_ddctbl_amt
    	,othr_insrnc_amt
    	,RSN_SRVC_CD
    	,PROF_SRVC_CD
    	,RSLT_SRVC_CD
		,LINE_NUM
		)
	SELECT * 
	FROM &FL.L
   ) BY TMSIS_PASSTHROUGH;

	select line_ct into : LINE_CT
	from (select * from connection to tmsis_passthrough
          (select count(submtg_state_cd) as line_ct
	      from &FL.L));

%MEND BUILD_RX;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CRX00001;

  a.TMSIS_RUN_ID 
, a.SUBMTG_STATE_CD
, a.TMSIS_ACTV_IND            

%mend CRX00001;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CRX00002;

  a.TMSIS_RUN_ID 
, a.TMSIS_ACTV_IND 
, a.TMSIS_RPTG_PRD               
, upper(a.SECT_1115A_DEMO_IND) as SECT_1115A_DEMO_IND              
, coalesce(a.ADJDCTN_DT,'01JAN1960') as ADJDCTN_DT
, upper(COALESCE(a.ADJSTMT_IND,'X')) AS ADJSTMT_IND   
, upper(a.ADJSTMT_RSN_CD) as ADJSTMT_RSN_CD  
, a.BENE_COINSRNC_AMT 
, a.BENE_COPMT_AMT   
, a.BENE_DDCTBL_AMT   
, upper(a.BLG_PRVDR_NPI_NUM) as BLG_PRVDR_NPI_NUM         
, upper(a.BLG_PRVDR_NUM) as BLG_PRVDR_NUM                      
, upper(a.BLG_PRVDR_SPCLTY_CD) as BLG_PRVDR_SPCLTY_CD                
, upper(a.BLG_PRVDR_TXNMY_CD) as BLG_PRVDR_TXNMY_CD                
, upper(a.BRDR_STATE_IND) as BRDR_STATE_IND
, a.CLL_CNT  
, upper(a.CLM_STUS_CTGRY_CD) as CLM_STUS_CTGRY_CD                    
, upper(a.CMPND_DRUG_IND) as CMPND_DRUG_IND                       
, upper(a.COPAY_WVD_IND) as COPAY_WVD_IND                         
, upper(a.XOVR_IND) as XOVR_IND                         
, a.PRSCRBD_DT                               
, upper(a.DSPNSNG_PD_PRVDR_NPI_NUM) as DSPNSNG_PD_PRVDR_NPI_NUM    
, upper(a.DSPNSNG_PD_PRVDR_NUM) as DSPNSNG_PD_PRVDR_NUM  
, upper(a.FIXD_PYMT_IND) as FIXD_PYMT_IND      
, upper(a.FUNDNG_CD) as FUNDNG_CD                             
, upper(a.FUNDNG_SRC_NON_FED_SHR_CD) as FUNDNG_SRC_NON_FED_SHR_CD 
, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') AS ADJSTMT_CLM_NUM                       
, coalesce(upper(a.ORGNL_CLM_NUM),'~') AS ORGNL_CLM_NUM       
, upper(a.MDCR_BENE_ID) as MDCR_BENE_ID     
, upper(a.MDCR_HICN_NUM) as MDCR_HICN_NUM   
, upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM      
, upper(a.OTHR_INSRNC_IND) as OTHR_INSRNC_IND                   
, upper(a.OTHR_TPL_CLCTN_CD) as OTHR_TPL_CLCTN_CD
, upper(a.PYMT_LVL_IND) as PYMT_LVL_IND                          
, upper(a.PLAN_ID_NUM) as MC_PLAN_ID                            
, upper(a.SRVCNG_PRVDR_NPI_NUM) as SRVCNG_PRVDR_NPI_NUM        
, upper(a.PRSCRBNG_PRVDR_NUM) as PRSCRBNG_PRVDR_NUM    
, a.RX_FILL_DT                              
, upper(a.PGM_TYPE_CD) as PGM_TYPE_CD                               
, upper(a.PRVDR_LCTN_ID) as PRVDR_LCTN_ID          
, a.SRVC_TRKNG_PYMT_AMT            
, upper(a.SRVC_TRKNG_TYPE_CD) as SRVC_TRKNG_TYPE_CD
, a.SUBMTG_STATE_CD 
, a.TOT_ALOWD_AMT                        
, a.TOT_BILL_AMT                          
, a.TOT_COPAY_AMT                        
, a.TOT_MDCD_PD_AMT                    
, a.TOT_MDCR_COINSRNC_AMT           
, a.TOT_MDCR_DDCTBL_AMT            
, a.TOT_OTHR_INSRNC_AMT            
, a.TOT_TPL_AMT                            
, upper(a.CLM_TYPE_CD) as CLM_TYPE_CD                               
, upper(a.WVR_ID) as WVR_ID                                      
, upper(a.WVR_TYPE_CD) as WVR_TYPE_CD 
, a.MDCD_PD_DT 
, upper(a.ELGBL_1ST_NAME) as ELGBL_1ST_NAME
, upper(a.ELGBL_LAST_NAME) as ELGBL_LAST_NAME
, upper(a.ELGBL_MDL_INITL_NAME) as ELGBL_MDL_INITL_NAME
, a.BIRTH_DT
, a.TP_COINSRNC_PD_AMT
, a.TP_COPMT_PD_AMT

%mend CRX00002;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro CRX00003;

  upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM_LINE       
, a.ALOWD_AMT  
, a.TMSIS_RUN_ID as TMSIS_RUN_ID_LINE
, a.TMSIS_ACTV_IND as TMSIS_ACTV_IND3
, upper(a.BNFT_TYPE_CD) as BNFT_TYPE_CD           
, upper(a.CLL_STUS_CD) as CLL_STUS_CD                          
, a.BILL_AMT                                           
, upper(a.BRND_GNRC_IND) as BRND_GNRC_IND           
, upper(a.CMS_64_FED_REIMBRSMT_CTGRY_CD) as CMS_64_FED_REIMBRSMT_CTGRY_CD        
, upper(a.CMPND_DSG_FORM_CD) as CMPND_DSG_FORM_CD                            
, a.COPAY_AMT
, coalesce(a.ADJDCTN_DT, '01JAN1960') AS ADJDCTN_DT_LINE
, coalesce(upper(a.ADJSTMT_CLM_NUM),'~') AS ADJSTMT_CLM_NUM_LINE                       
, coalesce(upper(a.ORGNL_CLM_NUM),'~') AS ORGNL_CLM_NUM_LINE 
, upper(a.ORGNL_LINE_NUM) as ORGNL_LINE_NUM                         
, upper(a.ADJSTMT_LINE_NUM) as ADJSTMT_LINE_NUM                           
, a.SUPLY_DAYS_CNT                                   
, a.DSPNS_FEE_AMT                                     
, upper(a.DRUG_UTLZTN_CD) as DRUG_UTLZTN_CD                               
, a.DTL_MTRC_DCML_QTY  
, upper(a.IMNZTN_TYPE_CD) as IMNZTN_TYPE_CD                             
, a.MDCD_FFS_EQUIV_AMT                       
, a.MDCD_PD_AMT            
, upper(a.NDC_CD) as NDC_CD                                         
, upper(a.NEW_REFL_IND) as NEW_REFL_IND                                   
, a.OTHR_TOC_RX_CLM_ACTL_QTY                 
, a.OTHR_TOC_RX_CLM_ALOWD_QTY 
, a.MDCR_COINSRNC_PD_AMT
, a.MDCR_DDCTBL_AMT 
, a.OTHR_INSRNC_AMT   
, upper(a.REBT_ELGBL_IND) as REBT_ELGBL_IND       
, upper(a.SUBMTG_STATE_CD) as SUBMTG_STATE_CD_LINE                              
, a.TPL_AMT 
, upper(a.stc_cd) as TOS_CD                                                   
, upper(a.UOM_CD) as UOM_CD                                                   
, upper(lpad(trim(a.XIX_SRVC_CTGRY_CD),4,'0')) as XIX_SRVC_CTGRY_CD               
, upper(lpad(trim(a.XXI_SRVC_CTGRY_CD),3,'0')) as XXI_SRVC_CTGRY_CD
, a.TMSIS_FIL_NAME
, a.REC_NUM  
, upper(COALESCE(a.LINE_ADJSTMT_IND,'X')) AS LINE_ADJSTMT_IND 
, a.MDCR_PD_AMT 

%mend CRX00003;

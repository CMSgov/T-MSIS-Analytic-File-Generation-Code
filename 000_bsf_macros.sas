/*********************************************************************************************/
/*Program: 000_bsf_macros.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Module macros for BSF. 
/* 
/*Modified: 12/6/2021 - MACTAF-1859-"Retire"_1115a_prtcpnt_flag from TAF CCB 2021Q4
/*						MACTAF-1817-Add elgbl_aftr_eom_ind
/*		 	05/03/2022- DB modified For V7.1													  
/*					    MACTAF-1946- Rename data elements   									  
/*						(ELG003) PRIMARY-LANGUAGE-ENGL-PROF-CODE-> ENGLSH_PRFCNCY_CD
/*						(ELG016) CERTIFIED-AMERICA-INDIAN-ALASKAN-NATIVE-INDICATOR-> AMRCN_INDN_ALSKA_NTV_IND

/**********************************************************************************************/
options minoperator;

%macro AWS_MAXID_pull_bsf; 
%global combined_list;

select cats("'",submtg_state_cd,"'") into :CUTOVER_FILTER
separated by ','
from
(select * from connection to tmsis_passthrough
(
select * from &da_schema..state_tmsis_cutovr_dt
where &TAF_FILE_DATE >= cast(tmsis_cutovr_dt as integer) 
));

	select cats("('",submtg_state_cd,"',",tmsis_run_id,")")
	     into :combined_list separated by ','
	from connection to tmsis_passthrough
	(select 
		 submtg_state_cd
		,max(tmsis_run_id) as tmsis_run_id
	from &TMSIS_SCHEMA..tmsis_fil_prcsg_job_cntl
	where  job_stus = 'success'
		and tot_actv_rcrds_elg02 > 0
		and submtg_state_cd <> '96'
		and submtg_state_cd <> '94'

	     %if %sysfunc(FIND(&ST_FILTER,%str(ALL))) = 0 %then %do;
         and &ST_FILTER
	   %end;

	   and submtg_state_cd in(&CUTOVER_FILTER)

	group by submtg_state_cd)
    order by submtg_state_cd;

%put combined_list = &combined_list;

/** Create table with SSN ind **/
	execute(
			create temp table ssn_ind
			distkey(submtg_state_cd)
			sortkey(submtg_state_cd)
	        as
			select
		%if &sub_env in(prod) %then
		   %do;
		     /** In higher environments, enforce a distinct ssn_ind **/
		       distinct
		   %end;
				 submtg_state_cd

        %if &sub_env in(pre_dev uat_val) %then
		   %do;
		     /** In lower environments, take a max ssn_ind **/
		       ,max(ssn_ind) as ssn_ind
		   %end; %else
		   %do;
		    /** In prod it should be distinct, so throw error otherwise**/
		       ,ssn_ind 
		   %end;
				          
				
			from &TMSIS_SCHEMA..tmsis_fhdr_rec_elgblty
			where tmsis_actv_ind = 1
			      and tmsis_rptg_prd is not null
				  and tot_rec_cnt > 0 			
                  and ssn_ind in('1','0') 
	              and (submtg_state_cd,tmsis_run_id) in (&combined_list)

		%if &sub_env in(pre_dev uat_val) %then
		   %do;
		       group by submtg_state_cd
		   %end;

	      ) by tmsis_passthrough;

	/* Check for ssn_ind duplicates and abort if so */
	select max_ct into :max_ssn_ct from
	(select * from connection to tmsis_passthrough
	(
	  select max(ssn_ind_ct) as max_ct
	  from (select submtg_state_cd, count(ssn_ind) as ssn_ind_ct
	        from ssn_ind group by submtg_state_cd) s
	) );

	%if %eval(&max_ssn_ct>1) %then
	%do;
	 %put ERROR: More than one SSN_IND was found for a state.;
	%end;
	%else %do; %put NOTE: Only one SSN_IND per state found.; %end;

%mend AWS_MAXID_pull_bsf;
%macro create_initial_table(tab_no, _2x_segment, eff_date, end_date, orderby=MSIS_IDENT_NUM);
%let st_fil_type = %upcase(%tslit(%substr(&tab_no,1,3)));

   execute(

     create temp table &tab_no 
	  distkey(msis_ident_num)
	  sortkey(submtg_state_cd,&orderby) as

  	 select %&tab_no
	        ,a.MSIS_IDENT_NUM
            ,a.TMSIS_RPTG_PRD

    from &TMSIS_SCHEMA..&_2x_segment a 

		 left join &DA_SCHEMA..state_submsn_type s
		 on a.submtg_state_cd = s.submtg_state_cd
		 and upper(s.fil_type) = &st_fil_type

  		where (a.TMSIS_ACTV_IND = 1 and (date_cmp(&eff_date,&RPT_PRD) in(-1,0) and
										 (date_cmp(&end_date,&st_dt) in(0,1) or &end_date is NULL)))

		and ((upper(coalesce(s.submsn_type,'X')) <> 'CSO'  
               and date_cmp(a.TMSIS_RPTG_PRD,&st_dt) in(1,0)) or
		     (upper(coalesce(s.submsn_type,'X')) = 'CSO'))

        and (a.submtg_state_cd,a.tmsis_run_id) in (&combined_list)
		and a.msis_ident_num is not null

		%if &test_run=YES %then %do; LIMIT 100000 %end;

	 ) by tmsis_passthrough;

execute (update &tab_no
         set msis_ident_num = upper(msis_ident_num)
          ) by tmsis_passthrough;

title "Number of records pull for &tab_no";
select * from connection to tmsis_passthrough
 ( select count(submtg_state_cd) as records
   from &tab_no
 );

%mend create_initial_table;

%macro MultiIds(sort_key=,where=,suffix=,val=);
  execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no.&suffix._multi_all 
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num)  as 
	 select t1.*
            %if %length(&created_vars)>1 %then %do;
        ,&created_vars %end;
	       
		from &tab_no&val t1
        inner join &tab_no.&suffix._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt>1

		%if %length(&where)>1 %then %do;
		 where &where
		%end; 

    ) by tmsis_passthrough;

execute(

     create temp table &tab_no.&suffix._multi_step2  
	 distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num,keep_flag) as

	 select *,
	        row_number() over (partition by submtg_state_cd 
                                      ,msis_ident_num
					     order by submtg_state_cd, 
                                  msis_ident_num,
								  TMSIS_RPTG_PRD desc,
								  &eff_date desc,
								  &end_date desc, 
								  REC_NUM desc,
                                  &sort_key) as KEEP_FLAG
     from &tab_no.&suffix._multi_all

	  %if %length(&where)>1 %then %do;
		 where &where
	  %end; 
	  
    ) by tmsis_passthrough;

execute(

     create temp table &tab_no.&suffix._multi  
	 distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num) as

	 select *
     from &tab_no.&suffix._multi_step2
	 where keep_flag=1
	  
    ) by tmsis_passthrough;

%mend MultiIds;

%macro set_to_null(var,value);
%let valids =;
%do I=1 %to %sysfunc(countw(&value));
 %let val = %sysfunc(trim(%scan(&value,&I)));
 %if &I=1 %then %let valids = %unquote(%nrbquote('&val'));
 %else %let valids = &valids,%unquote(%nrbquote('&val'));
%end;
case when upper(trim(&tbl..&var)) in(&valids) then upper(trim(&tbl..&var)) else null end as &var 
%mend set_to_null;


%macro ELG00001;

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

%mend ELG00001;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00002(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>1) ne 1 %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD
, &tbl..REC_NUM 
, &tbl..DEATH_DT  
,%end;
 
 &tbl..BIRTH_DT          
,%set_to_null(GNDR_CD,%str(M F))
, &tbl..ELGBL_1ST_NAME
, &tbl..ELGBL_LAST_NAME
, &tbl..ELGBL_MDL_INITL_NAME            
, &tbl..PRMRY_DMGRPHC_ELE_EFCTV_DT     
, &tbl..PRMRY_DMGRPHC_ELE_END_DT         

%if %eval(&final>=1)=1  %then %do;
,DEATH_DATE as DEATH_DT
%end;

%mend ELG00002;

%macro ELG00002A(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

 &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM 
, &tbl..DEATH_DT  
, &tbl..PRMRY_DMGRPHC_ELE_EFCTV_DT     
, &tbl..PRMRY_DMGRPHC_ELE_END_DT    

%mend ELG00002A;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00003(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD  
, &tbl..REC_NUM 
,%end;
 
 lpad(cast(&tbl..SSN_NUM as char(9)),9,'0') as SSN_NUM
,%set_to_null(MRTL_STUS_CD,%str(01 02 03 04 05 06 07 08 09 10 11 12 13 14))
,%set_to_null(SSN_VRFCTN_IND,%str(0 1 2))
,%set_to_null(INCM_CD,%str(01 02 03 04 05 06 07 08))
,%set_to_null(VET_IND,%str(0 1))
,%set_to_null(CTZNSHP_IND,%str(0 1 2))
,%set_to_null(CTZNSHP_VRFCTN_IND,%str(0 1))
,%set_to_null(IMGRTN_STUS_CD,%str(1 2 3 8))
, &tbl..IMGRTN_STUS_5_YR_BAR_END_DT
,%set_to_null(IMGRTN_VRFCTN_IND,%str(0 1))      
,upper(&tbl..PRMRY_LANG_CD) as PRMRY_LANG_CD
,%set_to_null(ENGLSH_PRFCNCY_CD,%str(0 1 2 3))     /* TO BE MOD */
,%set_to_null(HSEHLD_SIZE_CD,%str(01 02 03 04 05 06 07 08)) 
,%set_to_null(PRGNT_IND,%str(0 1)) 
, &tbl..MDCR_HICN_NUM       
, &tbl..MDCR_BENE_ID     
,%set_to_null(CHIP_CD,%str(0 1 2 3 4))           
, &tbl..VAR_DMGRPHC_ELE_EFCTV_DT        
, &tbl..VAR_DMGRPHC_ELE_END_DT 
 
%mend ELG00003;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00003A(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

  &tbl..TMSIS_RUN_ID    
, &tbl..TMSIS_ACTV_IND  
, &tbl..PRGNT_IND               
, &tbl..SUBMTG_STATE_CD     
, &tbl..VAR_DMGRPHC_ELE_EFCTV_DT        
, &tbl..VAR_DMGRPHC_ELE_END_DT 
, &tbl..REC_NUM 

%mend ELG00003A;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00004(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
, &tbl..ELGBL_LINE_1_ADR            
, &tbl..ELGBL_LINE_2_ADR            
, &tbl..ELGBL_LINE_3_ADR            
, &tbl..ELGBL_ADR_TYPE_CD    
, &tbl..ELGBL_CITY_NAME    
, &tbl..ELGBL_CNTY_CD   
, &tbl..ELGBL_PHNE_NUM             
,%set_to_null(ELGBL_STATE_CD,%str(1 2 4 5 6 8 9 01 02 04 05 06 08 09 10 11 12 13 15 16 17 18 19 20
                                  21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 44
                                  45 46 47 48 49 50 51 53 54 55 56 60 64 66 67 68 69 70 71 72 74 76 78
                                  79 81 84 86 89 95))

 ,&tbl..ELGBL_ZIP_CD
,%end;

 
 &tbl..ELGBL_ADR_EFCTV_DT     
,&tbl..ELGBL_ADR_END_DT         
            
%if %eval(&final>=1)=1  %then %do;
,&tbl..ELGBL_LINE_1_ADR_HOME            
, &tbl..ELGBL_LINE_2_ADR_HOME            
, &tbl..ELGBL_LINE_3_ADR_HOME            
, &tbl..ELGBL_CITY_NAME_HOME    
, &tbl..ELGBL_CNTY_CD_HOME   
, &tbl..ELGBL_PHNE_NUM_HOME             
, &tbl..ELGBL_STATE_CD_HOME    
, &tbl..ELGBL_ZIP_CD_HOME    

,&tbl..ELGBL_LINE_1_ADR_MAIL            
, &tbl..ELGBL_LINE_2_ADR_MAIL            
, &tbl..ELGBL_LINE_3_ADR_MAIL            
, &tbl..ELGBL_CITY_NAME_MAIL    
, &tbl..ELGBL_CNTY_CD_MAIL   
, &tbl..ELGBL_PHNE_NUM_MAIL             
, &tbl..ELGBL_STATE_CD_MAIL    
, &tbl..ELGBL_ZIP_CD_MAIL   
%end;

%mend ELG00004;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00005(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%set_to_null(ELGBLTY_GRP_CD,%str(01 02 03 04 05 06 07 08 09 1 2 3 4 5 6 7 8 9 11 12 13 14 15 16 17 18 19 20
                                      21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45
                                      46 47 48 49 50 51 52 53 54 55 56 59 60 61 62 63 64 65 66 67 68 69 70 71 72
                                      73 74 75 76))
,%set_to_null(DUAL_ELGBL_CD,%str(00 01 02 03 04 05 06 08 09 10 0 1 2 3 4 5 6 8 9))  
,%set_to_null(ELGBLTY_CHG_RSN_CD,%str(01 02 03 04 05 06 07 08 09 1 2 3 4 5 6 7 8 9 11 12 13 14 15 16 17 18 19 20 21 22))
,%end;
 
 &tbl..MSIS_CASE_NUM 
,%set_to_null(ELGBLTY_MDCD_BASIS_CD,%str(00 01 02 03 04 05 06 07 08 10 11))     
,%set_to_null(CARE_LVL_STUS_CD,%str(001 002 003 004 005 01 02 03 04 05 1 2 3 4 5)) 
,%set_to_null(SSDI_IND,%str(0 1)) 
,%set_to_null(SSI_IND,%str(0 1)) 
,%set_to_null(SSI_STATE_SPLMT_STUS_CD,%str(000 001 002)) 
,%set_to_null(SSI_STUS_CD,%str(000 001 002))   
, &tbl..STATE_SPEC_ELGBLTY_FCTR_TXT    
,%set_to_null(BIRTH_CNCPTN_IND,%str(0 1))  
,%set_to_null(MAS_CD,%str(0 1 2 3 4 5))        
,%set_to_null(RSTRCTD_BNFTS_CD,%str(0 1 2 3 4 5 6 7 A B C D E F))        
,%set_to_null(TANF_CASH_CD,%str(0 1 2))        
, &tbl..ELGBLTY_DTRMNT_EFCTV_DT  
, &tbl..ELGBLTY_DTRMNT_END_DT
,%set_to_null(PRMRY_ELGBLTY_GRP_IND,%str(0 1))

%if %eval(&final>=1)=1  %then %do;
,ELGBLTY_GRP_CODE as ELGBLTY_GRP_CD
,DUAL_ELGBL_CODE as DUAL_ELGBL_CD
,lpad(trim(ELGBLTY_CHG_RSN_CD),2,'0') as ELGBLTY_CHG_RSN_CD

%end;

%mend ELG00005;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00006(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;

  &tbl..HH_ENT_NAME   
, &tbl..HH_SNTRN_NAME
, &tbl..HH_SNTRN_PRTCPTN_EFCTV_DT       
, &tbl..HH_SNTRN_PRTCPTN_END_DT           
 
%mend ELG00006;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00007(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM 
,%end;
 
  &tbl..HH_PRVDR_NUM  
, &tbl..HH_SNTRN_PRVDR_EFCTV_DT           
, &tbl..HH_SNTRN_PRVDR_END_DT   


%mend ELG00007;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00008(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..HH_CHRNC_CD  
, &tbl..HH_CHRNC_EFCTV_DT           
, &tbl..HH_CHRNC_END_DT   


%mend ELG00008;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00009(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..LCKIN_EFCTV_DT    
, &tbl..LCKIN_END_DT        
, &tbl..LCKIN_PRVDR_NUM  
,%set_to_null(LCKIN_PRVDR_TYPE_CD,%str(1 2 3 4 5 6 7 8 9 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18
                                       19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42
                                       43 44 45 46 47 48 49 50 51 52 53 54 55 56 57))


%mend ELG00009;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00010(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..MFP_ENRLMT_EFCTV_DT      
, &tbl..MFP_ENRLMT_END_DT   
,%set_to_null(MFP_LVS_WTH_FMLY_CD,%str(0 1 2))
,%set_to_null(MFP_QLFYD_INSTN_CD,%str(00 01 02 03 04 05 0 1 2 3 4 5))
,%set_to_null(MFP_QLFYD_RSDNC_CD,%str(00 01 02 03 04 05 0 1 2 3 4 5))
,%set_to_null(MFP_PRTCPTN_ENDD_RSN_CD,%str(00 01 02 03 04 05 06 07 0 1 2 3 4 5 6 7)) 
,%set_to_null(MFP_RINSTLZD_RSN_CD,%str(00 01 02 03 04 05 06 07 08 0 1 2 3 4 5 6 7 8))    

%mend ELG00010;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00011(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..STATE_PLAN_OPTN_EFCTV_DT  
, &tbl..STATE_PLAN_OPTN_END_DT      
,%set_to_null(STATE_PLAN_OPTN_TYPE_CD,%str(00 01 02 03 04 05 06 0 1 2 3 4 5 6))

%mend ELG00011;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00012(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..WVR_ENRLMT_EFCTV_DT      
, &tbl..WVR_ENRLMT_END_DT          
, &tbl..WVR_ID        
,%set_to_null(WVR_TYPE_CD,%str(1 2 3 4 5 6 7 8 9 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33))

%mend ELG00012;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00013(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;   
 
 &tbl..LTSS_ELGBLTY_EFCTV_DT     
, &tbl..LTSS_ELGBLTY_END_DT      
,%set_to_null(LTSS_LVL_CARE_CD,%str(1 2 3))
, &tbl..LTSS_PRVDR_NUM       

%mend ELG00013;
*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00014(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
, %end;
 
&tbl..MC_PLAN_ID
,%set_to_null(ENRLD_MC_PLAN_TYPE_CD,%str(0 1 2 3 4 5 6 7 8 9 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 60 70 80))
, &tbl..MC_PLAN_ENRLMT_EFCTV_DT      
, &tbl..MC_PLAN_ENRLMT_END_DT          


%mend ELG00014;
*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00015(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..ETHNCTY_DCLRTN_EFCTV_DT           
, &tbl..ETHNCTY_DCLRTN_END_DT   
,%set_to_null(ETHNCTY_CD,%str(0 1 2 3 4 5))

%mend ELG00015;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00016(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
, lpad(trim(&tbl..RACE_CD),3,'0') as RACE_CD
, &tbl..RACE_DCLRTN_EFCTV_DT  
, &tbl..RACE_DCLRTN_END_DT    
, &tbl..RACE_OTHR_TXT  
, %end;
  
%set_to_null(AMRCN_INDN_ALSKA_NTV_IND,%str(0 1 2))        /* TO BE MOD */

%mend ELG00016;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00017(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
, lpad(trim(&tbl..DSBLTY_TYPE_CD),2,'0') as DSBLTY_TYPE_CD
,%end;
 
 &tbl..DSBLTY_TYPE_EFCTV_DT    
, &tbl..DSBLTY_TYPE_END_DT        


%mend ELG00017;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00018(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 %set_to_null(SECT_1115A_DEMO_IND,%str(0 1))
, &tbl..SECT_1115A_DEMO_EFCTV_DT       
, &tbl..SECT_1115A_DEMO_END_DT 


%mend ELG00018;

*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00020(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

 %if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
, lpad(trim(&tbl..NDC_UOM_CHRNC_NON_HH_CD),3,'0') as NDC_UOM_CHRNC_NON_HH_CD
,%end;
 
 &tbl..NDC_UOM_CHRNC_NON_HH_EFCTV_DT   
, &tbl..NDC_UOM_CHRNC_NON_HH_END_DT       


%mend ELG00020;


*********************************************************************
*pull only the required data elements from each segment				*
*********************************************************************;
%macro ELG00021(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM  
,%end;
 
 &tbl..ENRLMT_EFCTV_DT   
, &tbl..ENRLMT_END_DT       
, &tbl..ENRLMT_TYPE_CD        


%mend ELG00021;

%macro ELG00022(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM   
,%end;
 
 &tbl..ELGBL_ID_EFCTV_DT   
, &tbl..ELGBL_ID_END_DT       
, &tbl..ELGBL_ID_TYPE_CD  
, &tbl..ELGBL_ID 
, &tbl..ELGBL_ID_ISSG_ENT_ID_TXT
, &tbl..RSN_FOR_CHG 

%if %eval(&final>=1)=1  %then %do;
, &tbl..ELGBL_ID_ADDTNL
, &tbl..ELGBL_ID_ADDTNL_ENT_ID 
, &tbl..ELGBL_ID_ADDTNL_RSN_CHG
, &tbl..ELGBL_ID_MSIS_XWALK
, &tbl..ELGBL_ID_MSIS_XWALK_ENT_ID 
, &tbl..ELGBL_ID_MSIS_XWALK_RSN_CHG
%end; 


%mend ELG00022;

%macro TPL00002(final);
%if %eval(&final>=1)=1 %then %let tbl=t&final;
%else %let tbl=a;

%if %eval(&final>=1)=0  %then %do;
  &tbl..TMSIS_RUN_ID  
, &tbl..TMSIS_ACTV_IND 
, &tbl..SUBMTG_STATE_CD 
, &tbl..REC_NUM 
,%end;
 
 &tbl..ELGBL_PRSN_MN_EFCTV_DT         
, &tbl..ELGBL_PRSN_MN_END_DT 
            
, %set_to_null(TPL_INSRNC_CVRG_IND,%str(0 1))
, %set_to_null(TPL_OTHR_CVRG_IND,%str(0 1)) 


%mend TPL00002;

%macro FINAL_FORMAT;
 DA_RUN_ID
,BSF_FIL_DT
,BSF_VRSN
,MSIS_IDENT_NUM    
,SSN_NUM
,upper(nullif(trim(SSN_IND),'')) AS SSN_IND
,upper(nullif(trim(SSN_VRFCTN_IND),'')) AS SSN_VRFCTN_IND
,upper(nullif(trim(MDCR_BENE_ID),'')) AS MDCR_BENE_ID
,upper(nullif(trim(MDCR_HICN_NUM),'')) AS MDCR_HICN_NUM
,TMSIS_RUN_ID
,SUBMTG_STATE_CD     
,REGION  AS REG_FLAG 
,upper(nullif(trim(MSIS_CASE_NUM),'')) AS MSIS_CASE_NUM
,SINGLE_ENR_FLAG AS SNGL_ENRLMT_FLAG
%do I=1 %to 16;
,MDCD_ENRLMT_EFF_DT_&I 
,MDCD_ENRLMT_END_DT_&I
%end;
%do I=1 %to 16;
,CHIP_ENRLMT_EFF_DT_&I 
,CHIP_ENRLMT_END_DT_&I
%end;
,upper(nullif(trim(ELGBL_1ST_NAME),'')) AS ELGBL_1ST_NAME
,upper(nullif(trim(ELGBL_LAST_NAME),'')) AS ELGBL_LAST_NAME
,upper(nullif(trim(ELGBL_MDL_INITL_NAME),'')) AS ELGBL_MDL_INITL_NAME
,%fix_old_dates(BIRTH_DT)
,DEATH_DT
,AGE  AS AGE_NUM        
,AGE_GROUP_FLAG AS AGE_GRP_FLAG    
,DECEASED_FLAG AS DCSD_FLAG 
,upper(nullif(trim(GNDR_CODE),'')) AS GNDR_CD
,upper(nullif(trim(MRTL_STUS_CD),'')) AS MRTL_STUS_CD
,upper(nullif(trim(INCM_CD),'')) AS INCM_CD
,upper(nullif(trim(VET_IND),'')) AS VET_IND
,upper(nullif(trim(CTZNSHP_IND),'')) AS CTZNSHP_IND
,upper(nullif(trim(CTZNSHP_VRFCTN_IND),'')) AS CTZNSHP_VRFCTN_IND
,case when IMGRTN_STUS_CD ='8' then '0' 
      else upper(nullif(trim(IMGRTN_STUS_CD),'')) end AS IMGRTN_STUS_CD
,upper(nullif(trim(IMGRTN_VRFCTN_IND),'')) AS IMGRTN_VRFCTN_IND
,%fix_old_dates(IMGRTN_STUS_5_YR_BAR_END_DT)
,upper(nullif(trim(PRMRY_LANG_CODE),'')) AS OTHR_LANG_HOME_CD
,PRMRY_LANG_FLAG
,upper(nullif(trim(ENGLSH_PRFCNCY_CD),'')) AS PRMRY_LANG_ENGLSH_PRFCNCY_CD		/* TO BE MOD */
,upper(nullif(trim(HSEHLD_SIZE_CD),'')) AS HSEHLD_SIZE_CD
,null AS PRGNT_IND
,null::smallint AS PRGNCY_FLAG
,upper(nullif(trim(AMRCN_INDN_ALSKA_NTV_IND),'')) AS CRTFD_AMRCN_INDN_ALSKN_NTV_IND /* TO BE MOD */
,upper(nullif(trim(ETHNCTY_CD),'')) AS ETHNCTY_CD
,upper(nullif(trim(ELGBL_LINE_1_ADR_HOME),'')) AS ELGBL_LINE_1_ADR_HOME
,upper(nullif(trim(ELGBL_LINE_2_ADR_HOME),'')) AS ELGBL_LINE_2_ADR_HOME
,upper(nullif(trim(ELGBL_LINE_3_ADR_HOME),'')) AS ELGBL_LINE_3_ADR_HOME
,upper(nullif(trim(ELGBL_CITY_NAME_HOME),'')) AS ELGBL_CITY_NAME_HOME
,upper(nullif(trim(ELGBL_ZIP_CD_HOME),'')) AS ELGBL_ZIP_CD_HOME
,upper(nullif(trim(ELGBL_CNTY_CD_HOME),'')) AS ELGBL_CNTY_CD_HOME
,upper(nullif(trim(ELGBL_STATE_CD_HOME),'')) AS ELGBL_STATE_CD_HOME
,upper(nullif(trim(ELGBL_PHNE_NUM_HOME),'')) AS ELGBL_PHNE_NUM_HOME
,upper(nullif(trim(ELGBL_LINE_1_ADR_MAIL),'')) AS ELGBL_LINE_1_ADR_MAIL
,upper(nullif(trim(ELGBL_LINE_2_ADR_MAIL),'')) AS ELGBL_LINE_2_ADR_MAIL
,upper(nullif(trim(ELGBL_LINE_3_ADR_MAIL),'')) AS ELGBL_LINE_3_ADR_MAIL
,upper(nullif(trim(ELGBL_CITY_NAME_MAIL),'')) AS ELGBL_CITY_NAME_MAIL
,upper(nullif(trim(ELGBL_ZIP_CD_MAIL),'')) AS ELGBL_ZIP_CD_MAIL
,upper(nullif(trim(ELGBL_CNTY_CD_MAIL),'')) AS ELGBL_CNTY_CD_MAIL
,upper(nullif(trim(ELGBL_STATE_CD_MAIL),'')) AS ELGBL_STATE_CD_MAIL
,upper(nullif(trim(CARE_LVL_STUS_CODE),'')) AS CARE_LVL_STUS_CD
,DEAF_DISAB_FLAG AS DEAF_DSBL_FLAG    
,BLIND_DISAB_FLAG AS BLND_DSBL_FLAG   
,DIFF_CONC_DISAB_FLAG  AS DFCLTY_CONC_DSBL_FLAG
,DIFF_WALKING_DISAB_FLAG  AS DFCLTY_WLKG_DSBL_FLAG   
,DIFF_DRESSING_BATHING_DISAB_FLAG  AS DFCLTY_DRSNG_BATHG_DSBL_FLAG 
,DIFF_ERRANDS_ALONE_DISAB_FLAG   AS DFCLTY_ERRANDS_ALN_DSBL_FLAG
,OTHER_DISAB_FLAG    AS OTHR_DSBL_FLAG
,HCBS_AGED_NON_HHCC_FLAG
,HCBS_PHYS_DISAB_NON_HHCC_FLAG  AS HCBS_PHYS_DSBL_NON_HHCC_FLAG
,HCBS_INTEL_DISAB_NON_HHCC_FLAG AS HCBS_INTEL_DSBL_NON_HHCC_FLAG
,HCBS_AUTISM_SP_DIS_NON_HHCC_FLAG AS HCBS_AUTSM_NON_HHCC_FLAG
,HCBS_DD_NON_HHCC_FLAG
,HCBS_MI_SED_NON_HHCC_FLAG
,HCBS_BRAIN_INJ_NON_HHCC_FLAG AS HCBS_BRN_INJ_NON_HHCC_FLAG 
,HCBS_HIV_AIDS_NON_HHCC_FLAG
,HCBS_TECH_DEP_MF_NON_HHCC_FLAG
,HCBS_DISAB_OTHER_NON_HHCC_FLAG AS HCBS_DSBL_OTHR_NON_HHCC_FLAG
,ENROLLMENT_TYPE_FLAG  AS ENRL_TYPE_FLAG      
,DAYS_ELIG_IN_MO_CNT  
,ELIGIBLE_ENTIRE_MONTH_IND AS ELGBL_ENTIR_MO_IND
,ELIGIBLE_LAST_DAY_OF_MONTH_IND AS ELGBL_LAST_DAY_OF_MO_IND
,upper(nullif(trim(CHIP_CD),'')) AS CHIP_CD
,upper(nullif(trim(ELGBLTY_GRP_CD),'')) AS ELGBLTY_GRP_CD
,upper(nullif(trim(PRMRY_ELGBLTY_GRP_IND),'')) AS PRMRY_ELGBLTY_GRP_IND
,ELIGIBILITY_GROUP_CATEGORY_FLAG AS ELGBLTY_GRP_CTGRY_FLAG
,upper(nullif(trim(MAS_CD),'')) AS MAS_CD
,upper(nullif(trim(ELGBLTY_MDCD_BASIS_CD),'')) AS ELGBLTY_MDCD_BASIS_CD
,upper(MASBOE) as MASBOE_CD
,upper(nullif(trim(STATE_SPEC_ELGBLTY_FCTR_TXT),'')) AS STATE_SPEC_ELGBLTY_FCTR_TXT
,upper(nullif(trim(DUAL_ELGBL_CD),'')) AS DUAL_ELGBL_CD
,DUAL_ELIGIBLE_FLAG  AS DUAL_ELGBL_FLAG
,upper(nullif(trim(RSTRCTD_BNFTS_CD),'')) AS RSTRCTD_BNFTS_CD
,upper(nullif(trim(SSDI_IND),'')) AS SSDI_IND
,upper(nullif(trim(SSI_IND),'')) AS SSI_IND
,upper(nullif(trim(SSI_STATE_SPLMT_STUS_CD),'')) AS SSI_STATE_SPLMT_STUS_CD
,upper(nullif(trim(SSI_STUS_CD),'')) AS SSI_STUS_CD
,upper(nullif(trim(BIRTH_CNCPTN_IND),'')) AS BIRTH_CNCPTN_IND
,upper(nullif(trim(TANF_CASH_CD),'')) AS TANF_CASH_CD
,HH_PROGRAM_PARTICIPANT_FLAG AS HH_PGM_PRTCPNT_FLAG
,upper(nullif(trim(HH_PRVDR_NUM),'')) AS HH_PRVDR_NUM
,upper(nullif(trim(HH_ENT_NAME),'')) AS HH_ENT_NAME
,MH_HH_CHRONIC_COND_FLAG AS MH_HH_CHRNC_COND_FLAG   
,SA_HH_CHRONIC_COND_FLAG AS SA_HH_CHRNC_COND_FLAG
,ASTHMA_HH_CHRONIC_COND_FLAG AS ASTHMA_HH_CHRNC_COND_FLAG  
,DIABETES_HH_CHRONIC_COND_FLAG AS DBTS_HH_CHRNC_COND_FLAG
,HEART_DIS_HH_CHRONIC_COND_FLAG AS HRT_DIS_HH_CHRNC_COND_FLAG
,OVERWEIGHT_HH_CHRONIC_COND_FLAG AS OVRWT_HH_CHRNC_COND_FLAG
,HIV_AIDS_HH_CHRONIC_COND_FLAG AS HIV_AIDS_HH_CHRNC_COND_FLAG
,OTHER_HH_CHRONIC_COND_FLAG AS OTHR_HH_CHRNC_COND_FLAG
%do I=1 %to 3;
,upper(nullif(trim(LCKIN_PRVDR_NUM&I),'')) AS LCKIN_PRVDR_NUM&I
,upper(nullif(trim(LCKIN_PRVDR_TYPE_CD&I),'')) AS LCKIN_PRVDR_TYPE_CD&I
%end; 
,LOCK_IN_FLAG AS LCKIN_FLAG  
%do I=1 %to 3;
,upper(nullif(trim(LTSS_PRVDR_NUM&I),'')) AS LTSS_PRVDR_NUM&I
,upper(nullif(trim(LTSS_LVL_CARE_CD&I),'')) AS LTSS_LVL_CARE_CD&I
%end;
%do I=1 %to 16;
,upper(nullif(trim(MC_PLAN_ID&I),'')) AS MC_PLAN_ID&I
,upper(nullif(trim(MC_PLAN_TYPE_CD&I),'')) AS MC_PLAN_TYPE_CD&I
%end;
,upper(nullif(trim(MFP_LVS_WTH_FMLY_CD),'')) AS MFP_LVS_WTH_FMLY_CD
,upper(nullif(trim(MFP_QLFYD_INSTN_CODE),'')) AS MFP_QLFYD_INSTN_CD
,upper(nullif(trim(MFP_QLFYD_RSDNC_CODE),'')) AS MFP_QLFYD_RSDNC_CD
,upper(nullif(trim(MFP_PRTCPTN_ENDD_RSN_CODE),'')) AS MFP_PRTCPTN_ENDD_RSN_CD
,upper(nullif(trim(MFP_RINSTLZD_RSN_CODE),'')) AS MFP_RINSTLZD_RSN_CD
,MFP_PARTICIPANT_FLAG AS MFP_PRTCPNT_FLAG  
,COMMUNITY_FIRST_CHOICE_SPO_FLAG AS CMNTY_1ST_CHS_SPO_FLAG
,_1915I_SPO_FLAG
,_1915J_SPO_FLAG
,_1932A_SPO_FLAG
,_1915A_SPO_FLAG
,_1937_ABP_SPO_FLAG
,_1115A_PARTICIPANT_FLAG AS _1115A_PRTCPNT_FLAG    

%do I=1 %to 10;
,upper(nullif(trim(WVR_ID&I),'')) AS WVR_ID&I
,upper(nullif(trim(WVR_TYPE_CD&I),'')) AS WVR_TYPE_CD&I
%end;
,upper(nullif(trim(TPL_INSRNC_CVRG_IND),'')) AS TPL_INSRNC_CVRG_IND
,upper(nullif(trim(TPL_OTHR_CVRG_IND),'')) AS TPL_OTHR_CVRG_IND
,upper(nullif(trim(SECT_1115A_DEMO_IND),'')) AS SECT_1115A_DEMO_IND
,NATIVE_HI_FLAG as NTV_HI_FLAG
,GUAM_CHAMORRO_FLAG
,SAMOAN_FLAG
,OTHER_PAC_ISLANDER_FLAG as othr_pac_islndr_flag
,UNK_PAC_ISLANDER_FLAG as unk_pac_islndr_flag
,ASIAN_INDIAN_FLAG as asn_indn_flag
,CHINESE_FLAG
,FILIPINO_FLAG
,JAPANESE_FLAG
,KOREAN_FLAG
,VIETNAMESE_FLAG
,OTHER_ASIAN_FLAG as othr_asn_flag
,UNKNOWN_ASIAN_FLAG as unk_asn_flag
,WHITE_FLAG as wht_flag
,BLACK_AFRICAN_AMERICAN_FLAG as black_afrcn_amrcn_flag
,AIAN_FLAG
,RACE_ETHNICITY_FLAG as RACE_ETHNCTY_FLAG
,RACE_ETHNCTY_EXP_FLAG
,HISPANIC_ETHNICITY_FLAG as HSPNC_ETHNCTY_FLAG
,upper(nullif(trim(ELGBL_ID_ADDTNL),'')) as ELGBL_ID_ADDTNL
,upper(nullif(trim(ELGBL_ID_ADDTNL_ENT_ID),'')) as  ELGBL_ID_ADDTNL_ENT_ID
,upper(nullif(trim(ELGBL_ID_ADDTNL_RSN_CHG),'')) as ELGBL_ID_ADDTNL_RSN_CHG
,upper(nullif(trim(ELGBL_ID_MSIS_XWALK),'')) as ELGBL_ID_MSIS_XWALK
,upper(nullif(trim(ELGBL_ID_MSIS_XWALK_ENT_ID),'')) as ELGBL_ID_MSIS_XWALK_ENT_ID
,upper(nullif(trim(ELGBL_ID_MSIS_XWALK_RSN_CHG),'')) as ELGBL_ID_MSIS_XWALK_RSN_CHG
,upper(nullif(trim(ELGBLTY_CHG_RSN_CD),'')) as ELGBLTY_CHG_RSN_CD
,ELGBL_AFTR_EOM_IND
%mend FINAL_FORMAT;
  %macro drop_table_multi(dsn_list);
    %let tbl_ct = %sysfunc(countw(&dsn_list));

	%do I=1 %to &tbl_ct;

      %let tbl = %scan(&dsn_list,&I);
	  execute 
	  (
        drop table if exists &tbl 
	  ) by tmsis_passthrough;

	%end;

	%mend drop_table_multi;

%macro extract_table(tbl,limit,where,lib);
%if %length(&lib)>0 %then %let dsn=&lib..&tbl; 
%else %let dsn=work.&tbl;
create table &dsn as 
select * from connection to tmsis_passthrough
 ( select * 
   from &tbl 
%if %length(&where)>1 %then %do;
 where &where
%end; 
 %if %eval(&limit>0) %then %do;
 LIMIT &limit
 %end; 
);


%mend extract_table;

%macro describe_table(tbl);
create table tbl1 as
select * from connection to tmsis_passthrough
 ( select *
   from pg_table_def 
  );
title "Contents of &tbl";
select column, type, encoding, distkey, sortkey, notnull from tbl1
where lowcase(tablename) = lowcase("&tbl");

%mend describe_table;

%macro timestamp_log;
  %let actual_time = %sysfunc(putn(%sysfunc(datetime()),datetime22.));
  %put *--------------------------------------------------*;
  %put Timestamp: &actual_time. ;
  %put *--------------------------------------------------*;
%mend timestamp_log;

%macro list_libnames;
	proc sql;
		/* create list of currently allocated libnames */
		create table libnames as
			select 'logviewer libname=' || trim(b.libname) ||
		           ' path=' || trim(b.path) ||
		           ' engine=' || trim(b.engine) || ' <end>' as libname_line
				from (select libname, sysname, min(level) as level from dictionary.libnames
				 where sysname in ('Filename', 'Schema/User')  
							   and libname ne 'WORK' 
		         group by libname, sysname) a
				inner join  dictionary.libnames b
				   on b.libname = a.libname and b.level = a.level and b.sysname = a.sysname		  
				;
	quit;

	/* display list of libnames in the log */
	data _null_;
		set libnames;
		put libname_line;
	run;

%mend list_libnames;

/*** Macro to run at the start of each program **/
%macro header_macro(path,program);
proc printto new
log="&path./&program..log"
print="&path./&program..lst";
run;
%timestamp_log;
%LIST_LIBNAMES;
%mend header_macro;

%macro RUNPROG(PATH);
%let sas_path = %substr(&path,1,%length(&path)-%length(%scan(&path,-1,'/'))-1);
%put &sas_path;
%let sas_prog = %sysfunc(tranwrd(%sysfunc(tranwrd(&path,&sas_path./,%str())),%str(.sas),%str()));
%put &sas_prog;
title1;
footnote1;
%header_macro(&sas_path,&sas_prog);
%INCLUDE "&PATH." / SOURCE2;
proc printto; run;
%mend RUNPROG;

%macro count_records(table);

title "Number of records in &table";
	  select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &table
	  );

%mend count_records;

%macro dedup_tbl_joiner(tblnum);
		left join (select * from &tab_no._step2 where keeper=&tblnum) t&tblnum. 
          on t1.submtg_state_cd=t&tblnum..submtg_state_cd
		 and t1.msis_ident_num =t&tblnum..msis_ident_num
%mend dedup_tbl_joiner;

%macro check_max_keep(desired_max);
%let maxt = %trim(&max_keep);
%if %eval(&max_keep > &desired_max) %then 
%do;
  %if &sub_env in(pre_dev uat_val) %then
   %do;
       %PUT INFO: Max Keep value of &maxt exceeds &desired_max for &tab_no.;
   %end; %else
   %do;
       %PUT ERROR: Max Keep value of &maxt exceeds &desired_max for &tab_no.;
   %end;

%end;
%mend check_max_keep;

%macro BSF_INSERT_ORDER;
DA_RUN_ID
,BSF_FIL_DT
,BSF_VRSN
,MSIS_IDENT_NUM
,SSN_NUM
,SSN_IND
,SSN_VRFCTN_IND
,MDCR_BENE_ID
,MDCR_HICN_NUM
,TMSIS_RUN_ID
,SUBMTG_STATE_CD
,REG_FLAG
,MSIS_CASE_NUM
,SNGL_ENRLMT_FLAG
,MDCD_ENRLMT_EFF_DT_1
,MDCD_ENRLMT_END_DT_1
,MDCD_ENRLMT_EFF_DT_2
,MDCD_ENRLMT_END_DT_2
,MDCD_ENRLMT_EFF_DT_3
,MDCD_ENRLMT_END_DT_3
,MDCD_ENRLMT_EFF_DT_4
,MDCD_ENRLMT_END_DT_4
,MDCD_ENRLMT_EFF_DT_5
,MDCD_ENRLMT_END_DT_5
,MDCD_ENRLMT_EFF_DT_6
,MDCD_ENRLMT_END_DT_6
,MDCD_ENRLMT_EFF_DT_7
,MDCD_ENRLMT_END_DT_7
,MDCD_ENRLMT_EFF_DT_8
,MDCD_ENRLMT_END_DT_8
,MDCD_ENRLMT_EFF_DT_9
,MDCD_ENRLMT_END_DT_9
,MDCD_ENRLMT_EFF_DT_10
,MDCD_ENRLMT_END_DT_10
,MDCD_ENRLMT_EFF_DT_11
,MDCD_ENRLMT_END_DT_11
,MDCD_ENRLMT_EFF_DT_12
,MDCD_ENRLMT_END_DT_12
,MDCD_ENRLMT_EFF_DT_13
,MDCD_ENRLMT_END_DT_13
,MDCD_ENRLMT_EFF_DT_14
,MDCD_ENRLMT_END_DT_14
,MDCD_ENRLMT_EFF_DT_15
,MDCD_ENRLMT_END_DT_15
,MDCD_ENRLMT_EFF_DT_16
,MDCD_ENRLMT_END_DT_16
,CHIP_ENRLMT_EFF_DT_1
,CHIP_ENRLMT_END_DT_1
,CHIP_ENRLMT_EFF_DT_2
,CHIP_ENRLMT_END_DT_2
,CHIP_ENRLMT_EFF_DT_3
,CHIP_ENRLMT_END_DT_3
,CHIP_ENRLMT_EFF_DT_4
,CHIP_ENRLMT_END_DT_4
,CHIP_ENRLMT_EFF_DT_5
,CHIP_ENRLMT_END_DT_5
,CHIP_ENRLMT_EFF_DT_6
,CHIP_ENRLMT_END_DT_6
,CHIP_ENRLMT_EFF_DT_7
,CHIP_ENRLMT_END_DT_7
,CHIP_ENRLMT_EFF_DT_8
,CHIP_ENRLMT_END_DT_8
,CHIP_ENRLMT_EFF_DT_9
,CHIP_ENRLMT_END_DT_9
,CHIP_ENRLMT_EFF_DT_10
,CHIP_ENRLMT_END_DT_10
,CHIP_ENRLMT_EFF_DT_11
,CHIP_ENRLMT_END_DT_11
,CHIP_ENRLMT_EFF_DT_12
,CHIP_ENRLMT_END_DT_12
,CHIP_ENRLMT_EFF_DT_13
,CHIP_ENRLMT_END_DT_13
,CHIP_ENRLMT_EFF_DT_14
,CHIP_ENRLMT_END_DT_14
,CHIP_ENRLMT_EFF_DT_15
,CHIP_ENRLMT_END_DT_15
,CHIP_ENRLMT_EFF_DT_16
,CHIP_ENRLMT_END_DT_16
,ELGBL_1ST_NAME
,ELGBL_LAST_NAME
,ELGBL_MDL_INITL_NAME
,BIRTH_DT
,DEATH_DT
,AGE_NUM
,AGE_GRP_FLAG
,DCSD_FLAG
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
,PRGNT_IND
,PRGNCY_FLAG
,CRTFD_AMRCN_INDN_ALSKN_NTV_IND
,ETHNCTY_CD
,ELGBL_LINE_1_ADR_HOME
,ELGBL_LINE_2_ADR_HOME
,ELGBL_LINE_3_ADR_HOME
,ELGBL_CITY_NAME_HOME
,ELGBL_ZIP_CD_HOME
,ELGBL_CNTY_CD_HOME
,ELGBL_STATE_CD_HOME
,ELGBL_PHNE_NUM_HOME
,ELGBL_LINE_1_ADR_MAIL
,ELGBL_LINE_2_ADR_MAIL
,ELGBL_LINE_3_ADR_MAIL
,ELGBL_CITY_NAME_MAIL
,ELGBL_ZIP_CD_MAIL
,ELGBL_CNTY_CD_MAIL
,ELGBL_STATE_CD_MAIL
,CARE_LVL_STUS_CD
,DEAF_DSBL_FLAG
,BLND_DSBL_FLAG
,DFCLTY_CONC_DSBL_FLAG
,DFCLTY_WLKG_DSBL_FLAG
,DFCLTY_DRSNG_BATHG_DSBL_FLAG
,DFCLTY_ERRANDS_ALN_DSBL_FLAG
,OTHR_DSBL_FLAG
,HCBS_AGED_NON_HHCC_FLAG
,HCBS_PHYS_DSBL_NON_HHCC_FLAG
,HCBS_INTEL_DSBL_NON_HHCC_FLAG
,HCBS_AUTSM_NON_HHCC_FLAG
,HCBS_DD_NON_HHCC_FLAG
,HCBS_MI_SED_NON_HHCC_FLAG
,HCBS_BRN_INJ_NON_HHCC_FLAG
,HCBS_HIV_AIDS_NON_HHCC_FLAG
,HCBS_TECH_DEP_MF_NON_HHCC_FLAG
,HCBS_DSBL_OTHR_NON_HHCC_FLAG
,ENRL_TYPE_FLAG
,DAYS_ELIG_IN_MO_CNT
,ELGBL_ENTIR_MO_IND
,ELGBL_LAST_DAY_OF_MO_IND
,CHIP_CD
,ELGBLTY_GRP_CD
,PRMRY_ELGBLTY_GRP_IND
,ELGBLTY_GRP_CTGRY_FLAG
,MAS_CD
,ELGBLTY_MDCD_BASIS_CD
,MASBOE_CD
,STATE_SPEC_ELGBLTY_FCTR_TXT
,DUAL_ELGBL_CD
,DUAL_ELGBL_FLAG
,RSTRCTD_BNFTS_CD
,SSDI_IND
,SSI_IND
,SSI_STATE_SPLMT_STUS_CD
,SSI_STUS_CD
,BIRTH_CNCPTN_IND
,TANF_CASH_CD
,HH_PGM_PRTCPNT_FLAG
,HH_PRVDR_NUM
,HH_ENT_NAME
,MH_HH_CHRNC_COND_FLAG
,SA_HH_CHRNC_COND_FLAG
,ASTHMA_HH_CHRNC_COND_FLAG
,DBTS_HH_CHRNC_COND_FLAG
,HRT_DIS_HH_CHRNC_COND_FLAG
,OVRWT_HH_CHRNC_COND_FLAG
,HIV_AIDS_HH_CHRNC_COND_FLAG
,OTHR_HH_CHRNC_COND_FLAG
,LCKIN_PRVDR_NUM1
,LCKIN_PRVDR_TYPE_CD1
,LCKIN_PRVDR_NUM2
,LCKIN_PRVDR_TYPE_CD2
,LCKIN_PRVDR_NUM3
,LCKIN_PRVDR_TYPE_CD3
,LCKIN_FLAG
,LTSS_PRVDR_NUM1
,LTSS_LVL_CARE_CD1
,LTSS_PRVDR_NUM2
,LTSS_LVL_CARE_CD2
,LTSS_PRVDR_NUM3
,LTSS_LVL_CARE_CD3
,MC_PLAN_ID1
,MC_PLAN_TYPE_CD1
,MC_PLAN_ID2
,MC_PLAN_TYPE_CD2
,MC_PLAN_ID3
,MC_PLAN_TYPE_CD3
,MC_PLAN_ID4
,MC_PLAN_TYPE_CD4
,MC_PLAN_ID5
,MC_PLAN_TYPE_CD5
,MC_PLAN_ID6
,MC_PLAN_TYPE_CD6
,MC_PLAN_ID7
,MC_PLAN_TYPE_CD7
,MC_PLAN_ID8
,MC_PLAN_TYPE_CD8
,MC_PLAN_ID9
,MC_PLAN_TYPE_CD9
,MC_PLAN_ID10
,MC_PLAN_TYPE_CD10
,MC_PLAN_ID11
,MC_PLAN_TYPE_CD11
,MC_PLAN_ID12
,MC_PLAN_TYPE_CD12
,MC_PLAN_ID13
,MC_PLAN_TYPE_CD13
,MC_PLAN_ID14
,MC_PLAN_TYPE_CD14
,MC_PLAN_ID15
,MC_PLAN_TYPE_CD15
,MC_PLAN_ID16
,MC_PLAN_TYPE_CD16
,MFP_LVS_WTH_FMLY_CD
,MFP_QLFYD_INSTN_CD
,MFP_QLFYD_RSDNC_CD
,MFP_PRTCPTN_ENDD_RSN_CD
,MFP_RINSTLZD_RSN_CD
,MFP_PRTCPNT_FLAG
,CMNTY_1ST_CHS_SPO_FLAG
,_1915I_SPO_FLAG
,_1915J_SPO_FLAG
,_1932A_SPO_FLAG
,_1915A_SPO_FLAG
,_1937_ABP_SPO_FLAG
,_1115A_PRTCPNT_FLAG
,WVR_ID1
,WVR_TYPE_CD1
,WVR_ID2
,WVR_TYPE_CD2
,WVR_ID3
,WVR_TYPE_CD3
,WVR_ID4
,WVR_TYPE_CD4
,WVR_ID5
,WVR_TYPE_CD5
,WVR_ID6
,WVR_TYPE_CD6
,WVR_ID7
,WVR_TYPE_CD7
,WVR_ID8
,WVR_TYPE_CD8
,WVR_ID9
,WVR_TYPE_CD9
,WVR_ID10
,WVR_TYPE_CD10
,TPL_INSRNC_CVRG_IND
,TPL_OTHR_CVRG_IND
,SECT_1115A_DEMO_IND
,NTV_HI_FLAG
,GUAM_CHAMORRO_FLAG
,SAMOAN_FLAG
,OTHR_PAC_ISLNDR_FLAG
,UNK_PAC_ISLNDR_FLAG
,ASN_INDN_FLAG
,CHINESE_FLAG
,FILIPINO_FLAG
,JAPANESE_FLAG
,KOREAN_FLAG
,VIETNAMESE_FLAG
,OTHR_ASN_FLAG
,UNK_ASN_FLAG
,WHT_FLAG
,BLACK_AFRCN_AMRCN_FLAG
,AIAN_FLAG
,RACE_ETHNCTY_FLAG
,RACE_ETHNCTY_EXP_FLAG
,HSPNC_ETHNCTY_FLAG
,ELGBL_ID_ADDTNL
,ELGBL_ID_ADDTNL_ENT_ID 
,ELGBL_ID_ADDTNL_RSN_CHG
,ELGBL_ID_MSIS_XWALK
,ELGBL_ID_MSIS_XWALK_ENT_ID 
,ELGBL_ID_MSIS_XWALK_RSN_CHG
,ELGBLTY_CHG_RSN_CD
,ELGBL_AFTR_EOM_IND
%mend BSF_INSERT_ORDER;

%MACRO BUILD_BSF();

   EXECUTE(
    INSERT INTO &DA_SCHEMA..&TABLE_NAME	
	( %BSF_INSERT_ORDER	)
	SELECT %BSF_INSERT_ORDER 
	FROM BSF_&RPT_OUT._&BSF_FILE_DATE.
   ) BY TMSIS_PASSTHROUGH;

%MEND BUILD_BSF;

%macro check_if_exists(tbl);
%global table_check;
create table tbl1 as
select * from connection to tmsis_passthrough
 ( select *
   from pg_table_def 
  );
title "Check if table '&tbl' exists";
select count(*) into : table_check
from tbl1
where lowcase(tablename) = lowcase("&tbl");
%mend check_if_exists;

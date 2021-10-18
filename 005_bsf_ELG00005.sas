/**********************************************************************************************/
/*Program: 005_bsf_ELG00005.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_ELGBLTY_DTRMNT and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/

%macro create_ELG00005(tab_no, _2x_segment, eff_date, end_date);
%let ELGBLTY_GRP_CODE = lpad(trim(ELGBLTY_GRP_CD),2,'0') ;
%let DUAL_ELGBL_CODE = lpad(trim(DUAL_ELGBL_CD),2,'0') ;

%let created_vars = 
    &ELGBLTY_GRP_CODE as ELGBLTY_GRP_CODE,
	&DUAL_ELGBL_CODE as DUAL_ELGBL_CODE,

	lpad(trim(CARE_LVL_STUS_CD),3,'0')  as CARE_LVL_STUS_CODE,

    case when DUAL_ELGBL_CODE in('02','04','08')      	then 1 /* Full Dual */
         when DUAL_ELGBL_CODE in('01','03','05','06')  then 2 /* Partial Dual */
         when DUAL_ELGBL_CODE in('09','10')		    then 3 /* Other Dual */
         when DUAL_ELGBL_CODE in('00')				    then 4 /* Non-Dual */
		 else null end as DUAL_ELIGIBLE_FLG,

    case when (ELGBLTY_GRP_CODE between '01' and '09') or
              (ELGBLTY_GRP_CODE between '72' and '75') then 1
         when (ELGBLTY_GRP_CODE between '11' and '19') or
              (ELGBLTY_GRP_CODE between '20' and '26') then 2
		 when (ELGBLTY_GRP_CODE between '27' and '29') or 
              (ELGBLTY_GRP_CODE between '30' and '36') or 
              (ELGBLTY_GRP_CODE = '76')                then 3
         when (ELGBLTY_GRP_CODE between '37' and '39') or 
              (ELGBLTY_GRP_CODE between '40' and '49') or 
              (ELGBLTY_GRP_CODE between '50' and '52') then 4
		 when (ELGBLTY_GRP_CODE between '53' and '56') then 5
		 when (ELGBLTY_GRP_CODE in('59','60'))         then 6
         when (ELGBLTY_GRP_CODE in('61','62','63')) then 7
         when (ELGBLTY_GRP_CODE in('64','65','66')) then 8
         when (ELGBLTY_GRP_CODE in('67','68')) then 9
         when (ELGBLTY_GRP_CODE in('69','70','71'))    then 10
         else null end as ELIGIBILITY_GROUP_CATEGORY_FLG,

		 case when MAS_CD = '.' or ELGBLTY_MDCD_BASIS_CD='.' then '.'
		      else (MAS_CD || ELGBLTY_MDCD_BASIS_CD) end as MASBOE

;
execute (
     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._recCt 
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num,recCt) as
	 select 
        submtg_state_cd, 
        msis_ident_num,
		count(TMSIS_RUN_ID) as recCt
		from &tab_no
		where PRMRY_ELGBLTY_GRP_IND='1'
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select recCt,count(msis_ident_num) as beneficiaries from &tab_no._recCt group by recCt ) order by recCt;

 execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no._uniq 
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select t1.*,

         &created_vars,

		 1 as KEEP_FLAG

		from &tab_no t1
        inner join &tab_no._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1
		where PRMRY_ELGBLTY_GRP_IND='1'
		) by tmsis_passthrough;

title "Number of beneficiary with unique records in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._uniq );

%MultiIds(sort_key=%str(coalesce(trim(MSIS_CASE_NUM),'x') || coalesce(trim(elgblty_mdcd_basis_cd),'x')  || 
                        coalesce(trim(dual_elgbl_cd),'x')  || coalesce(trim(elgblty_grp_cd),'x')  || 
                        coalesce(trim(care_lvl_stus_cd),'x')  ||coalesce( trim(ssdi_ind),'x')  || coalesce(trim(ssi_ind),'x')  || 
                        coalesce(trim(ssi_state_splmt_stus_cd),'x') || coalesce(trim(ssi_stus_cd),'x')  || 
                        coalesce(trim(state_spec_elgblty_fctr_txt),'x')  || coalesce(trim(birth_cncptn_ind),'x')  || 
                        coalesce(trim(mas_cd),'x')  || coalesce(trim(rstrctd_bnfts_cd),'x')  || 
                        coalesce(trim(tanf_cash_cd),'x')  || coalesce(trim(prmry_elgblty_grp_ind),'x')  ||
						coalesce(trim(elgblty_chg_rsn_cd),'x')),
		  where=%str(PRMRY_ELGBLTY_GRP_IND='1'))

 execute(

     /* Union together tables for a permanent table */
     create temp table &tab_no._&BSF_FILE_DATE._uniq 
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num) as
	 select *

	 from (
	 select * from &tab_no._uniq
	 union all
	 select * from &tab_no._multi) 

    )  by tmsis_passthrough;

/* extract to SAS or load to permanent TMSIS table */
title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 005_bsf_ELG00005, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 005_bsf_ELG00005, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._recCt &tab_no._uniq &tab_no._multi &tab_no._multi_all &tab_no._multi_step2);

%mend create_ELG00005;


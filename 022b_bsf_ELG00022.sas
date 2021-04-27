/**********************************************************************************************/
/*Program: 022b_bsf_ELG00022.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 9/2/2020
/*Purpose: Process TMSIS_ELIG_IDENTIFERS and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_ELG00022(tab_no, _2x_segment, eff_date, end_date);


%macro create_identifiers(type, code);
/* confirm input variable names */
%let created_vars = ELGBL_ID as ELGBL_ID_&TYPE ,
                    ELGBL_ID_ISSG_ENT_ID_TXT as ELGBL_ID_&TYPE._ENT_ID ,
					RSN_FOR_CHG as ELGBL_ID_&TYPE._RSN_CHG ;

execute(

     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._&TYPE._recCt 
     distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num,recCt) as
	 select 
        submtg_state_cd, 
        msis_ident_num,
		count(msis_ident_num) as recCt
		from &tab_no
        where ELGBL_ID_TYPE_CD = &code
		  and nullif(trim(elgbl_id),'') is not null
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records per beneficiary in &tab_no -- &type";
select * from connection to tmsis_passthrough
 ( select recCt,count(msis_ident_num) as beneficiaries from &tab_no._&type._recCt group by recCt ) order by recCt;


execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no._&type._uniq 
	 distkey(msis_ident_num)
     sortkey(submtg_state_cd,msis_ident_num) as
	 select t1.*,
	       &created_vars,
			1 as KEEP_FLAG

		from &tab_no t1
        inner join &tab_no._&type._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1

		where ELGBL_ID_TYPE_CD = &code
		  and nullif(trim(elgbl_id),'') is not null

		) by tmsis_passthrough;

title "Number of beneficiary with unique records in &tab_no -- &type";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._&type._uniq );

 /** De-dupe records where multiples for the same type qualifying in the period -- exclude records where ELG_IDENTIFIER is null **/
%MultiIds(sort_key=%str(coalesce(trim(elgbl_id),'xx') || coalesce(trim(elgbl_id_issg_ent_id_txt),'xx') || 
                        coalesce(trim(rsn_for_chg),'xx')),
          where=%str(ELGBL_ID_TYPE_CD = &code and nullif(trim(elgbl_id),'') is not null),suffix=_&type);


title "Number of beneficiares who were processed for duplicates in &tab_no - &type";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._&type._multi );

%mend create_identifiers;
%create_identifiers(ADDTNL, '1');
%create_identifiers(MSIS_XWALK, '2');

	 /* Union together tables for a permanent table */
 execute(
     create temp table &tab_no._uniq_step1
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
	 select * from &tab_no._ADDTNL_uniq
	 union all
	 select * from &tab_no._ADDTNL_multi
	)  by tmsis_passthrough;

execute(
     create temp table &tab_no._uniq_step2
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
     select * from &tab_no._MSIS_XWALK_uniq
	 union all
	 select * from &tab_no._MSIS_XWALK_multi)  by tmsis_passthrough;

execute(
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
     sortkey(submtg_state_cd,msis_ident_num) as
     select coalesce(t1.msis_ident_num,t2.msis_ident_num) as msis_ident_num,
	        coalesce(t1.submtg_state_cd,t2.submtg_state_cd) as submtg_state_cd,
			t1.ELGBL_ID_ADDTNL,
			t1.ELGBL_ID_ADDTNL_ENT_ID ,
			t1.ELGBL_ID_ADDTNL_RSN_CHG,
			t2.ELGBL_ID_MSIS_XWALK,
			t2.ELGBL_ID_MSIS_XWALK_ENT_ID ,
            t2.ELGBL_ID_MSIS_XWALK_RSN_CHG

          from &tab_no._uniq_step1 t1
          full join &tab_no._uniq_step2 t2
          on t1.msis_ident_num=t2.msis_ident_num
          and t1.submtg_state_cd=t2.submtg_state_cd)  by tmsis_passthrough;

title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));


%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 022b_bsf_ELG00022, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 022b_bsf_ELG00022, 0.2. MultiIds);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 022b_bsf_ELG00022, 0.3 create segment output);

%drop_table_multi(&tab_no. &tab_no._ADDTNL_recCt &tab_no._ADDTNL_uniq &tab_no._MSIS_XWALK_recCt &tab_no._MSIS_XWALK_uniq &tab_no._ADDTNL_multi 
&tab_no._MSIS_XWALK_multi &tab_no._ADDTNL_multi_all &tab_no._ADDTNL_multi_step2 &tab_no._MSIS_XWALK_multi_all &tab_no._MSIS_XWALK_multi_step2);
%mend create_ELG00022;

/**********************************************************************************************/
/*Program: 011_bsf_ELG00011.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_STATE_PLAN_PRTCPTN and create unique output for BSF.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
 
%macro create_ELG00011(tab_no, _2x_segment, eff_date, end_date);
%let STATE_PLAN_OPTN_TYPE_CODE =
case when STATE_PLAN_OPTN_TYPE_CD <> '.' and length(STATE_PLAN_OPTN_TYPE_CD)= 1
     then lpad(STATE_PLAN_OPTN_TYPE_CD,2,'0') else STATE_PLAN_OPTN_TYPE_CD end;

execute (
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        submtg_state_cd, 
        msis_ident_num,

		max(case when STATE_PLAN_OPTN_TYPE_CD='.' then null when &STATE_PLAN_OPTN_TYPE_CODE in('01') then 1 else 0 end) as COMMUNITY_FIRST_CHOICE_SPO_FLG,
		max(case when STATE_PLAN_OPTN_TYPE_CD='.' then null when &STATE_PLAN_OPTN_TYPE_CODE in('02') then 1 else 0 end) as _1915I_SPO_FLG,
		max(case when STATE_PLAN_OPTN_TYPE_CD='.' then null when &STATE_PLAN_OPTN_TYPE_CODE in('03') then 1 else 0 end) as _1915J_SPO_FLG,
		max(case when STATE_PLAN_OPTN_TYPE_CD='.' then null when &STATE_PLAN_OPTN_TYPE_CODE in('04') then 1 else 0 end) as _1932A_SPO_FLG,
		max(case when STATE_PLAN_OPTN_TYPE_CD='.' then null when &STATE_PLAN_OPTN_TYPE_CODE in('05') then 1 else 0 end) as _1915A_SPO_FLG,
		max(case when STATE_PLAN_OPTN_TYPE_CD='.' then null when &STATE_PLAN_OPTN_TYPE_CODE in('06') then 1 else 0 end) as _1937_ABP_SPO_FLG
		
		from &tab_no
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;


title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 011_bsf_ELG00011, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 011_bsf_ELG00011, 0.2. MultiIds);

%drop_table_multi(&tab_no.);

%mend create_ELG00011;


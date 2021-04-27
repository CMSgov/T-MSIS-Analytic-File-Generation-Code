/**********************************************************************************************/
/*Program: 014_bsf_ELG00014.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_MC_PRTCPTN_DATA and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_ELG00014(tab_no, _2x_segment, eff_date, end_date);
%let ENRLD_MC_PLAN_TYPE_CODE = lpad(trim(enrld_mc_plan_type_cd),2,'0');

%let mc_plan =
case when trim(mc_plan_id)  in ('0','00','000','0000','00000','000000','0000000',
						                      '00000000','000000000','0000000000','00000000000','000000000000',
											  '8','88','888','8888','88888','888888','8888888',
						                      '88888888','888888888','8888888888','88888888888','888888888888',
											  '9','99','999','9999','99999','999999','9999999',
						                      '99999999','999999999','9999999999','99999999999','999999999999','')
     then null else trim(mc_plan_id) end;

execute (
     /* Reset plan type code for specific case  */
     create temp table &tab_no._step1
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select distinct
        submtg_state_cd, 
        msis_ident_num,
		rec_num,
		&eff_date,
		&end_date,
		tmsis_rptg_prd,
	    &MC_PLAN as MC_PLAN_IDENTIFIER, 
        case when MC_PLAN_IDENTIFIER is null and &ENRLD_MC_PLAN_TYPE_CODE = '00' then null
             else &ENRLD_MC_PLAN_TYPE_CODE end as ENRLD_MC_PLAN_TYPE_CODE,

		row_number() over (partition by submtg_state_cd,
		                        msis_ident_num,
								MC_PLAN_IDENTIFIER,
							    ENRLD_MC_PLAN_TYPE_CODE

                       order by submtg_state_cd,
		                        msis_ident_num,
                                TMSIS_RPTG_PRD desc,
								&eff_date desc,
								&end_date desc,
								REC_NUM desc,
								MC_PLAN_IDENTIFIER,
                                ENRLD_MC_PLAN_TYPE_CODE) as mc_deduper

		from (select * from &tab_no.
		      where enrld_mc_plan_type_cd is not null
		         or mc_plan_id is not null) t1
		) by tmsis_passthrough;


execute (
     
     create temp table &tab_no._step2
	 distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num,keeper) as
	 select *,
		row_number() over (partition by submtg_state_cd,
		                        msis_ident_num
                       order by submtg_state_cd,
		                        msis_ident_num,								
                                TMSIS_RPTG_PRD desc,
								&eff_date desc,
								&end_date desc,
								REC_NUM desc,
								MC_PLAN_IDENTIFIER,
                                ENRLD_MC_PLAN_TYPE_CODE) as keeper

		from (select * from &tab_no._step1
		      where (enrld_mc_plan_type_code is not null
		         or mc_plan_identifier is not null) and mc_deduper=1) t1
		) by tmsis_passthrough;

title "Number of waiver codes per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select keeper,count(msis_ident_num) as plans from &tab_no._step2 group by keeper ) order by keeper;

/** Determine Max number of Keeper Records **/
select max_keep into :max_keep
from (select * from connection to tmsis_passthrough
      (select max(keeper) as max_keep from &tab_no._step2));

%check_max_keep(16);

execute (
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        t1.submtg_state_cd 
        ,t1.msis_ident_num

		%do I=1 %to 16;
		   %if %eval(&I <= &max_keep) %then 
            %do;
	           ,t&I..MC_PLAN_IDENTIFIER as MC_PLAN_ID&I.
		       ,t&I..ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD&I.

			%end; %else
			%do;
               ,cast(null as varchar(12)) as MC_PLAN_ID&I.
			   ,cast(null as varchar(2)) as MC_PLAN_TYPE_CD&I.
			%end;
        %end;

		from (select * from &tab_no._step2 where keeper=1) t1

		%do I=2 %to 16;
		   %if %eval(&I <= &max_keep) %then 
            %do;
	          %dedup_tbl_joiner(&I)
			%end; 
        %end;

		) by tmsis_passthrough;

	/* extract to SAS or load to permanent TMSIS table */
title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 014_bsf_ELG00014, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 014_bsf_ELG00014, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._step1 &tab_no._step2);
%mend create_ELG00014;

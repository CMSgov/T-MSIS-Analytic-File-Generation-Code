/**********************************************************************************************/
/*Program: 013_bsf_ELG00013.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_LTSS_PRTCPTN_DATA and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_ELG00013(tab_no, _2x_segment, eff_date, end_date);
execute (
     create temp table &tab_no._step1
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num,ltss_deduper) as
	 select distinct
        submtg_state_cd, 
        msis_ident_num,
		TMSIS_RPTG_PRD,								
        &eff_date,
		&end_date,
		REC_NUM,
		LTSS_PRVDR_NUM,
		LTSS_LVL_CARE_CD,

				row_number() over (partition by submtg_state_cd,
		                        msis_ident_num,
								ltss_prvdr_num,
								ltss_lvl_care_cd
                       order by submtg_state_cd,
		                        msis_ident_num,
                                TMSIS_RPTG_PRD desc,								
                                &eff_date desc,
								&end_date desc,
								REC_NUM desc,
                                ltss_prvdr_num,
                                ltss_lvl_care_cd) as ltss_deduper

		from (select * from &tab_no
			  where ltss_prvdr_num is not null
		         or ltss_lvl_care_cd is not null) t2
		) by tmsis_passthrough;

execute (
     create temp table &tab_no._step2
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num,keeper) as
	 select distinct
        submtg_state_cd, 
        msis_ident_num,
		LTSS_PRVDR_NUM,
		LTSS_LVL_CARE_CD,

				row_number() over (partition by submtg_state_cd,
		                        msis_ident_num
                       order by submtg_state_cd,
		                        msis_ident_num,
                                TMSIS_RPTG_PRD desc,								
                                &eff_date desc,
								&end_date desc,
								REC_NUM desc,
                                ltss_prvdr_num,
                                ltss_lvl_care_cd) as keeper

		from &tab_no._step1
		where ltss_deduper=1
		) by tmsis_passthrough;

title "Number of LTSS providers per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select keeper,count(msis_ident_num) as ltss_prvdrs from &tab_no._step2 group by keeper ) order by keeper;

/** Determine Max number of Keeper Records **/
select max_keep into :max_keep
from (select * from connection to tmsis_passthrough
      (select max(keeper) as max_keep from &tab_no._step2));

execute (
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        t1.submtg_state_cd
        ,t1.msis_ident_num

		%do I=1 %to 3;
		  %if %eval(&I <= &max_keep) %then
		   %do;
	          ,t&I..LTSS_PRVDR_NUM as LTSS_PRVDR_NUM&I.
		      ,t&I..LTSS_LVL_CARE_CD as LTSS_LVL_CARE_CD&I
		   %end; %else
		   %do;
	          ,cast(null as varchar(30)) as LTSS_PRVDR_NUM&I.
		      ,cast(null as varchar(1)) as LTSS_LVL_CARE_CD&I.
		   %end;
        %end;
		
		from (select * from &tab_no._step2 where keeper=1) t1

	    %do I=2 %to 3;
		   %if %eval(&I <= &max_keep) %then 
            %do;
	          %dedup_tbl_joiner(&I)
			%end; 
        %end;


		) by tmsis_passthrough;


title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 013_bsf_ELG00013, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 013_bsf_ELG00013, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._step1);
%mend create_ELG00013;



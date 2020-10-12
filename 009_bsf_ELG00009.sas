/**********************************************************************************************/
/*Program: 009_bsf_ELG00009.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_LCKIN_INFO and create unique output for BSF.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
 
%macro create_ELG00009(tab_no, _2x_segment, eff_date, end_date);

execute (
     create temp table &tab_no._step1
	 distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num,lckin_deduper) as
	 select distinct
        submtg_state_cd, 
        msis_ident_num,
		lckin_prvdr_num,
		TMSIS_RPTG_PRD,
		&eff_date,
		&end_date,
		REC_NUM,
		lpad(lckin_prvdr_type_cd,2,'0') as lckin_prvdr_type_code,

		row_number() over (partition by submtg_state_cd,
		                        msis_ident_num,
								lckin_prvdr_num,
								lckin_prvdr_type_code
                       order by submtg_state_cd,
		                        msis_ident_num,
                                TMSIS_RPTG_PRD desc,
						        &eff_date desc,
						        &end_date desc,
								REC_NUM desc,
                                lckin_prvdr_num,
								lckin_prvdr_type_code) as lckin_deduper

		from (select * from &tab_no where lckin_prvdr_num is not null
		                               or lckin_prvdr_type_cd is not null) t1
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
                                lckin_prvdr_num,
								lckin_prvdr_type_code) as keeper

		from &tab_no._step1
		where lckin_deduper=1
		) by tmsis_passthrough;

title "Number of lock in providers per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select keeper,count(msis_ident_num) as lock_in_providers from &tab_no._step2 group by keeper ) order by keeper;

/** Determine Max number of Keeper Records **/
select max_keep into :max_keep
from (select * from connection to tmsis_passthrough
      (select max(keeper) as max_keep from &tab_no._step2));

execute (
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 sortkey(submtg_state_cd,msis_ident_num) as

	 select 
        t1.submtg_state_cd
        ,t1.msis_ident_num

		%do I=1 %to 3;
		  %if %eval(&I <= &max_keep) %then
		   %do;
	          ,t&I..LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM&I.
		      ,t&I..LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD&I
		   %end; %else
		   %do;
	          ,cast(null as varchar(30)) as LCKIN_PRVDR_NUM&I.
		      ,cast(null as varchar(2)) as LCKIN_PRVDR_TYPE_CD&I.
		   %end;
        %end;

  ,case when LCKIN_PRVDR_NUM1 is not null or
             LCKIN_PRVDR_NUM2 is not null or
			 LCKIN_PRVDR_NUM3 is not null
		then 1 else 2 end as LOCK_IN_FLG

		from (select * from &tab_no._step2 where keeper=1) t1

	    %do I=2 %to 3;
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

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 009_bsf_ELG00009, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 009_bsf_ELG00009, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._step1);


%mend create_ELG00009;



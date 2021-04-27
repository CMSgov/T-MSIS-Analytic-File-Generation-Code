/**********************************************************************************************/
/*Program: 012_bsf_ELG00012.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_WVR_PRTCPTN_DATA and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_ELG00012(tab_no, _2x_segment, eff_date, end_date);
%let WVR_TYPE_CD = lpad(trim(WVR_TYPE_CD),2,'0') ;

execute (
     create temp table &tab_no._step1
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num,waiver_deduper) as
	 select distinct
        submtg_state_cd, 
        msis_ident_num,
		rec_num,
		&eff_date,
		&end_date,
		tmsis_rptg_prd,
		wvr_id,

        &WVR_TYPE_CD as WVR_TYPE_CODE ,

		row_number() over (partition by submtg_state_cd,
		                        msis_ident_num,
								wvr_id, 
                                wvr_type_code
                       order by submtg_state_cd,
		                        msis_ident_num,
                                TMSIS_RPTG_PRD desc,
								&eff_date desc,
								&end_date desc,
								REC_NUM desc,
                                wvr_id, 
                                wvr_type_code) as waiver_deduper

		from (select * from &tab_no 
		      where wvr_id is not null
		         or wvr_type_cd is not null) t1
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
                                wvr_id, 
                                wvr_type_code) as keeper

		from &tab_no._step1
		where waiver_deduper=1
		) by tmsis_passthrough;

title "Number of waiver codes per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select keeper,count(msis_ident_num) as waiver_cds from &tab_no._step2 group by keeper ) order by keeper;

/** Determine Max number of Keeper Records **/
select max_keep into :max_keep
from (select * from connection to tmsis_passthrough
      (select max(keeper) as max_keep from &tab_no._step2));

%check_max_keep(10);

execute (
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        t1.submtg_state_cd 
        ,t1.msis_ident_num

		%do I=1 %to 10;
		  %if %eval(&I <= &max_keep) %then
		   %do;
	          ,t&I..WVR_ID as WVR_ID&I.
		      ,t&I..WVR_TYPE_CODE as WVR_TYPE_CD&I
		   %end; %else
		   %do;
	          ,cast(null as varchar(20)) as WVR_ID&I.
		      ,cast(null as varchar(2)) as WVR_TYPE_CD&I
		   %end;
        %end;
		
		from (select * from &tab_no._step2 where keeper=1) t1

        %do I=2 %to 10;
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

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 012_bsf_ELG00012, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 012_bsf_ELG00012, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._step1 &tab_no._step2);

%mend create_ELG00012;


** ========================================================================== 
** program documentation 
** program     : 003_mc_selection_macros.sas
** project     : MACBIS - MC TAF
** programmer  : Heidi Cohen
** description : processing macros for the MC segments
** input data  : n/a
** output data : n/a
** calls       : copy_activerows
**               copy_activerows_nts
**               count_rows
**               screen_runid
**               screen_dates
**               remove_duprecs
**
** -------------------------------------------------------------------------- 
** history 
** date        | action 
** ------------+------------------------------------------------------------- 
** 03/16/2017  | program written (D. Whalen)
** 05/02/2017  | program updated (H. Cohen)
** 09/05/2017  | program updated (H. Cohen)
** 07/05/2018  | program updated (H. Cohen) CCB changes
** 10/08/2018  | program updated (H. Cohen) CCB changes
** 03/20/2019  | program updated (H. Cohen) CCB changes
** 02/12/2020  | program updated (H. Cohen) CCB changes
** 04/09/2020  | program updated (H. Cohen) CCB changes
** --------------------------------------------------------------------------;
** ==========================================================================;
** macros;

page;

%macro AWS_MAXID_pull_non_claim (TMSIS_SCHEMA, table, hdrtable); 
/* applies cutover date while identifying max run id of last successful T-MSIS load by state - results stored in combined_list */
/* hdrtable already has the header records selected for tms_is_active=1 and tms_reporting_period is not null and tot_rec_cnt > 0 and ST_FILTER  */

%global RUN_IDS STATE_IDS combined_list;

select cats("'",submtg_state_cd,"'") into :CUTOVER_FILTER
separated by ','
from
(select * from connection to tmsis_passthrough
(
select * from &da_schema..state_tmsis_cutovr_dt
where &TAF_FILE_DATE >= cast(tmsis_cutovr_dt as integer) 
));

	select tmsis_run_id, submtg_state_cd, cats("('",submtg_state_cd,"',",tmsis_run_id,")")
	     into :run_ids separated by ' ',
		      :state_ids separated by ' ',
			  :combined_list separated by ','
	from connection to tmsis_passthrough
	(select 
		 j.submtg_state_cd as submtg_state_cd
		,max(j.tmsis_run_id) as tmsis_run_id
	from &TMSIS_SCHEMA..&table. as j
	  join &hdrtable. as h
	  on h.submitting_state = j.submtg_state_cd and h.tms_run_id = j.tmsis_run_id
	where j.job_stus = 'success'
		and j.tot_actv_rcrds_mcp02 > 0
		and j.submtg_state_cd in(&CUTOVER_FILTER)

	group by submtg_state_cd)
    order by submtg_state_cd;

%put run_ids = &run_ids;
%put state_ids = &state_ids;
%put combined_list = &combined_list;

%mend AWS_MAXID_pull_non_claim;

** --------------------------------------------------------------------------;
** 000-01 header segment;
%macro process_01_MCheader (outtbl=);
  %put NOTE: ****** PROCESS_01_MCHEADER Start ******;
  %local cols01;
  %let cols01 = tms_run_id, 
                submitting_state;

  ** copy MC header table;
  execute( 
    %copy_activerows_nts(intbl=File_Header_Record_Managed_Care,
                     collist=cols01,
                     outtbl=#MC01_Header_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#MC01_Header_Copy, 
               cntvar=cnt_active, 
               outds=MC01_Active);

/* applies cutover date while identifying max run id of last successful T-MSIS load by state - results stored in combined_list */
%AWS_MAXID_pull_non_claim (&TMSIS_SCHEMA., tmsis_fil_prcsg_job_cntl, #MC01_Header_Copy); 

/* combined_list from macro above used in creation of &outtbl so that SPCL is presevred and subsequent programming is unchanged */
  ** extract the latest  (largest) run for each state and identify CHIP/TPA states;
  ** 1. identify the latest (largest) run number;
  execute(
    create table &outtbl 
           distkey(submitting_state)
           compound sortkey(tms_run_id, submitting_state) as
      select H.submitting_state,
	         S.SPCL,
             max(H.tms_run_id) as tms_run_id
	  from #MC01_Header_Copy H
	    left join #SPCLlst as S on H.submitting_state=S._mcstart
	  where (H.submitting_state,H.tms_run_id) in (&combined_list)
      group by H.submitting_state, S.SPCL
      order by submitting_state;
  ) by tmsis_passthrough;
 
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_latest, 
               outds=MC01_Latest);
  
  title3 "QC[01]: Summary Managed Care Header Extract";
   select * from connection to tmsis_passthrough
      (select A.submitting_state, cnt_active, cnt_latest
         from MC01_Active A
         left join MC01_Latest L on A.submitting_state=L.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_01_MCheader);

  ** clean-up;
  execute(
    drop table MC01_Active;
    drop table MC01_Latest;
	drop table #MC01_Header_Copy;
  ) by tmsis_passthrough;

%mend process_01_MCheader;


page;
** --------------------------------------------------------------------------;
** 000-02 main segment;
%macro process_02_MCmain (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_02_MCMAIN Start ******;

  ** screen out all but the latest run id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state;
  execute( 
    %screen_runid (intbl=Managed_Care_Main, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#MC02_Main_Latest1, 
                   runtyp=M);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC02_Main_Latest1, 
               cntvar=cnt_latest, 
               outds=MC02_Latest);

  %local cols02;
  %let cols02 = tms_run_id,
                tms_reporting_period,
                submitting_state,
                record_number,
				%upper_case(state_plan_id_num) as state_plan_id_num,
                managed_care_main_rec_eff_date,
                managed_care_main_rec_end_date,
                %fix_old_dates(managed_care_contract_eff_date),
                case 
                  when managed_care_contract_eff_date is not null and managed_care_contract_end_date is null then '9999-12-31'::date 
                  when date_cmp(managed_care_contract_end_date,'1600-01-01')=-1 then '1599-12-31'::date
                  else managed_care_contract_end_date
				end as MC_CNTRCT_END_DT,
                managed_care_contract_end_date,
                managed_care_name,
                managed_care_program,
                managed_care_plan_type,
                reimbursement_arrangement,
                managed_care_profit_status,
                core_based_statistical_area_code,
                percent_business,
                managed_care_service_area;
				
  %local whr02;
  %let whr02 = state_plan_id_num is not null;
  execute( 
    %copy_activerows(intbl=#MC02_Main_Latest1,
                     collist=cols02,
                     whr=&whr02,
                     outtbl=#MC02_Main_Copy);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC02_Main_Copy, 
               cntvar=cnt_active, 
               outds=MC02_Active);

  ** screen for MC contracted during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 state_plan_id_num;
  execute( 
    %screen_dates (intbl=#MC02_Main_Copy,
                   keyvars=keylist,
                   dtvar_beg=managed_care_main_rec_eff_date,
                   dtvar_end=managed_care_main_rec_end_date,
                   outtbl=#MC02_Main_Latest2);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC02_Main_Latest2, 
               cntvar=cnt_date, 
               outds=MC02_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num;
  execute( 
    %remove_duprecs (intbl=#MC02_Main_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=managed_care_contract_eff_date,
                     dtvar_end=MC_CNTRCT_END_DT,
                     ordvar=managed_care_name,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=MC02_Final);

  title3 "QC[02]: Summary MC Main Extract"; 
   select * from connection to tmsis_passthrough
      (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
         from MC02_Latest L
         left join MC02_Active A on L.submitting_state=A.submitting_state
         left join MC02_Date D   on L.submitting_state=D.submitting_state
         left join MC02_Final F  on L.submitting_state=F.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_02_MCmain);

  ** clean-up;
  execute(
    drop table MC02_Active;
    drop table MC02_Latest;
    drop table MC02_Date;
    drop table MC02_Final;
    drop table #MC02_Main_Copy;
    drop table #MC02_Main_Latest1;
    drop table #MC02_Main_Latest2;
) by tmsis_passthrough;

%mend process_02_MCmain;


** 000-03 location segment;
%macro process_03_location (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_03_location Start ******;

  ** screen out all but the latest (selected) run id - plan id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 state_plan_id_num;
  execute( 
    %screen_runid (intbl=Managed_care_location_and_contact_info, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#MC03_Location_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#MC03_Location_Latest1, 
               cntvar=cnt_latest, 
               outds=MC03_Latest);

  %local cols03;
  %let cols03 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				%upper_case(state_plan_id_num) as state_plan_id_num,
                %upper_case(managed_care_location_id) as managed_care_location_id,
                %fix_old_dates(managed_care_location_and_contact_info_eff_date),
                %set_end_dt(managed_care_location_and_contact_info_end_date) as managed_care_location_and_contact_info_end_date,
                %upper_case(managed_care_addr_ln1) as managed_care_addr_ln1,
                managed_care_addr_ln2,
                managed_care_addr_ln3,
                managed_care_addr_type,
                managed_care_city,
                managed_care_county,
				%upper_case(managed_care_state) as managed_care_state,
                managed_care_zip_code;

  ** copy 03 (Location) Managed Care rows;
  %local whr03;
  %let whr03 = managed_care_addr_type=3;
  execute( 
    %copy_activerows(intbl=#MC03_Location_Latest1,
                     collist=cols03,
                     whr=&whr03,
                     outtbl=#MC03_Location_Copy);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC03_Location_Copy,
               cntvar=cnt_active,
               outds=MC03_Active);

  ** screen for location during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 state_plan_id_num,
                 managed_care_location_id;
  execute( 
    %screen_dates (intbl=#MC03_Location_Copy,
                   keyvars=keylist,
                   dtvar_beg=managed_care_location_and_contact_info_eff_date,
                   dtvar_end=managed_care_location_and_contact_info_end_date,
                   outtbl=#MC03_Location_Latest2);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC03_Location_Latest2, 
               cntvar=cnt_date, 
               outds=MC03_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num,
                 managed_care_location_id,
                 managed_care_addr_ln1;
  execute( 
    %remove_duprecs (intbl=#MC03_Location_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=managed_care_location_and_contact_info_eff_date,
                     dtvar_end=managed_care_location_and_contact_info_end_date,
                     ordvar=managed_care_location_id,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=MC03_Final);

  title3 "QC[03]: Summary Managed Care Location Extract"; 
    select * from connection to tmsis_passthrough
      (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
         from MC03_Latest L
         left join MC03_Active A on L.submitting_state=A.submitting_state
         left join MC03_Date D   on L.submitting_state=D.submitting_state
         left join MC03_Final F  on L.submitting_state=F.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_03_location);

  ** clean-up;
  execute (
    drop table MC03_Active;
	drop table MC03_Latest;
	drop table MC03_Date;
	drop table MC03_Final;
    drop table #MC03_Location_Copy;
    drop table #MC03_Location_Latest1;
    drop table #MC03_Location_Latest2;
  ) by tmsis_passthrough;
%mend process_03_location;


** 000-04 service_area segment;
%macro process_04_service_area (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_04_service_area Start ******;

  ** screen out all but the latest (selected) run id - plan id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 state_plan_id_num;
  execute( 
    %screen_runid (intbl=Managed_care_service_area, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#MC04_Service_Area_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#MC04_Service_Area_Latest1, 
               cntvar=cnt_latest, 
               outds=MC04_Latest);

  %local cols04;
  %let cols04 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				%upper_case(state_plan_id_num) as state_plan_id_num,
				%upper_case(managed_care_service_area_name) as managed_care_service_area_name,
                %fix_old_dates(managed_care_service_area_eff_date),
                %set_end_dt(managed_care_service_area_end_date) as managed_care_service_area_end_date;

  ** copy 04 (service_area) Managed Care rows;
  %local whr04;
  %let whr04 = managed_care_service_area_name is not null;
  execute( 
    %copy_activerows(intbl=#MC04_Service_Area_Latest1,
                     collist=cols04,
                     whr=&whr04,
                     outtbl=#MC04_Service_Area_Copy);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC04_Service_Area_Copy, 
               cntvar=cnt_active, 
               outds=MC04_Active);

  ** screen for service_area during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 state_plan_id_num,
                 managed_care_service_area_name;
  execute( 
    %screen_dates (intbl=#MC04_Service_Area_Copy, 
                   keyvars=keylist,
                   dtvar_beg=managed_care_service_area_eff_date,
                   dtvar_end=managed_care_service_area_end_date, 
                   outtbl=#MC04_Service_Area_Latest2);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC04_Service_Area_Latest2, 
               cntvar=cnt_date, 
               outds=MC04_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num,
                 managed_care_service_area_name;
				 
  execute( 
    %remove_duprecs (intbl=#MC04_Service_Area_Latest2, 
                     grpvars=grplist,
                     dtvar_beg=managed_care_service_area_eff_date,
                     dtvar_end=managed_care_service_area_end_date,
                     ordvar=managed_care_service_area_name,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=MC04_Final);

  title3 "QC[04]: Summary Managed Care Service_Area Extract"; 
    select * from connection to tmsis_passthrough
      (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
         from MC04_Latest L
         left join MC04_Active A on L.submitting_state=A.submitting_state
         left join MC04_Date D   on L.submitting_state=D.submitting_state
         left join MC04_Final F  on L.submitting_state=F.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_04_service_area);

  ** clean-up;
  execute (
    drop table MC04_Active;
	drop table MC04_Latest;
	drop table MC04_Date;
	drop table MC04_Final;
    drop table #MC04_Service_Area_Copy;
    drop table #MC04_Service_Area_Latest1;
    drop table #MC04_Service_Area_Latest2;
  ) by tmsis_passthrough;
%mend process_04_service_area;


** 000-05 Operating_Authority segment;
%macro process_05_operating_authority (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_05_Operating_Authority Start ******;

  ** screen out all but the latest (selected) run id - plan id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 state_plan_id_num;
  execute( 
    %screen_runid (intbl=Managed_care_operating_authority, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#MC05_Operating_Authority_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#MC05_Operating_Authority_Latest1, 
               cntvar=cnt_latest, 
               outds=MC05_Latest);

  %local cols05;
  %let cols05 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				%upper_case(state_plan_id_num) as state_plan_id_num,
				%zero_pad(operating_authority, 2),
                %upper_case(waiver_id) as waiver_id,
                managed_care_op_authority_eff_date,
                managed_care_op_authority_end_date;

  ** copy 05 (Operating_Authority) managed care rows;
  %local whr05;
  %let whr05 = operating_authority is not null or waiver_id is not null;
  execute( 
    %copy_activerows(intbl=#MC05_Operating_Authority_Latest1,
                     collist=cols05,
                     whr=&whr05,
                     outtbl=#MC05_Operating_Authority_Copy);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC05_Operating_Authority_Copy, 
               cntvar=cnt_active, 
               outds=MC05_Active);

  ** screen for Operating_Authority_organization during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 state_plan_id_num,
                 waiver_id,
                 operating_authority;
  execute( 
    %screen_dates (intbl=#MC05_Operating_Authority_Copy, 
                   keyvars=keylist,
                   dtvar_beg=managed_care_op_authority_eff_date,
                   dtvar_end=managed_care_op_authority_end_date,
                   outtbl=#MC05_Operating_Authority_Latest2);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC05_Operating_Authority_Latest2, 
               cntvar=cnt_date, 
               outds=MC05_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num,
                 waiver_id,
                 operating_authority;
  execute( 
    %remove_duprecs (intbl=#MC05_Operating_Authority_Latest2, 
                     grpvars=grplist,
                     dtvar_beg=managed_care_op_authority_eff_date,
                     dtvar_end=managed_care_op_authority_end_date,
                     ordvar=waiver_id,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=MC05_Final);

  title3 "QC[05]: Summary Managed Care Operating_Authority Extract"; 
    select * from connection to tmsis_passthrough
      (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
         from MC05_Latest L
         left join MC05_Active A on L.submitting_state=A.submitting_state
         left join MC05_Date D   on L.submitting_state=D.submitting_state
         left join MC05_Final F  on L.submitting_state=F.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_05_operating_authority);

  ** clean-up;
  execute (
    drop table MC05_Active;
	drop table MC05_Latest;
	drop table MC05_Date;
	drop table MC05_Final;
    drop table #MC05_Operating_Authority_Copy;
    drop table #MC05_Operating_Authority_Latest1;
    drop table #MC05_Operating_Authority_Latest2;
  ) by tmsis_passthrough;
%mend process_05_Operating_authority;


** 000-06 population segment;
%macro process_06_population (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_06_population Start ******;

  ** screen out all but the latest (selected) run id - plan id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 state_plan_id_num;
  execute( 
    %screen_runid (intbl=Managed_care_plan_population_enrolled, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#MC06_Population_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#MC06_Population_Latest1, 
               cntvar=cnt_latest, 
               outds=MC06_Latest);

  %local cols06;
  %let cols06 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				%upper_case(state_plan_id_num) as state_plan_id_num,
                %zero_pad(managed_care_plan_pop, 2),
                %fix_old_dates(managed_care_plan_pop_eff_date),
                %set_end_dt(managed_care_plan_pop_end_date) as managed_care_plan_pop_end_date;

  ** copy 06 (population) MC rows;
  %local whr06;
  %let whr06 = managed_care_plan_pop is not null;
  execute( 
    %copy_activerows(intbl=#MC06_Population_Latest1,
                     collist=cols06,
                     whr=&whr06,
                     outtbl=#MC06_Population_Copy);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC06_Population_Copy, 
               cntvar=cnt_active, 
               outds=MC06_Active);

  ** screen for population enrolled during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 state_plan_id_num,
                 managed_care_plan_pop;
  execute( 
    %screen_dates (intbl=#MC06_Population_Copy, 
                   keyvars=keylist, 
                   dtvar_beg=managed_care_plan_pop_eff_date,
                   dtvar_end=managed_care_plan_pop_end_date, 
                   outtbl=#MC06_Population_Latest2);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC06_Population_Latest2, 
               cntvar=cnt_date, 
               outds=MC06_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num,
                 managed_care_plan_pop;
  execute( 
    %remove_duprecs (intbl=#MC06_Population_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=managed_care_plan_pop_eff_date,
                     dtvar_end=managed_care_plan_pop_end_date,
                     ordvar=managed_care_plan_pop,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=MC06_Final);

  title3 "QC[06]: Summary MC Population Extract"; 
    select * from connection to tmsis_passthrough
      (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
         from MC06_Latest L
         left join MC06_Active A on L.submitting_state=A.submitting_state
         left join MC06_Date D   on L.submitting_state=D.submitting_state
         left join MC06_Final F  on L.submitting_state=F.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_06_population);

  ** clean-up;
  execute (
    drop table MC06_Active;
	drop table MC06_Latest;
	drop table MC06_Date;
	drop table MC06_Final;
    drop table #MC06_Population_Copy;
    drop table #MC06_Population_Latest1;
    drop table #MC06_Population_Latest2;
  ) by tmsis_passthrough;
%mend process_06_population;

** 000-07 Accreditation segment;
%macro process_07_accreditation (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_07_Accreditation Start ******;

  ** screen out all but the latest (selected) run id - plan id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 state_plan_id_num;
  execute( 
    %screen_runid (intbl=Managed_care_accreditation_organization, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#MC07_Accreditation_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#MC07_Accreditation_Latest1, 
               cntvar=cnt_latest, 
               outds=MC07_Latest);

  %local cols07;
  %let cols07 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				%upper_case(state_plan_id_num) as state_plan_id_num,
                %zero_pad(accreditation_organization, 2),
                %fix_old_dates(date_accreditation_achieved),
                %set_end_dt(date_accreditation_end) as date_accreditation_end;

  ** copy 07 (Accreditation) managed care rows;
  %local whr07;
  %let whr07 = accreditation_organization is not null;
  execute( 
    %copy_activerows(intbl=#MC07_Accreditation_Latest1,
                     collist=cols07,
                     whr=&whr07,
                     outtbl=#MC07_Accreditation_Copy);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=#MC07_Accreditation_Copy, 
               cntvar=cnt_active, 
               outds=MC07_Active);

  ** screen for accreditation_organization during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 state_plan_id_num,
                 accreditation_organization;
  execute( 
    %screen_dates (intbl=#MC07_Accreditation_Copy, 
                   keyvars=keylist,
                   dtvar_beg=date_accreditation_achieved,
                   dtvar_end=date_accreditation_end,
                   outtbl=#MC07_Accreditation_Latest2);
  ) by tmsis_passthrough;
 
  ** row count;
  %count_rows (intbl=#MC07_Accreditation_Latest2, 
               cntvar=cnt_date, 
               outds=MC07_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 state_plan_id_num,
                 accreditation_organization;
  execute( 
    %remove_duprecs (intbl=#MC07_Accreditation_Latest2, 
                     grpvars=grplist,
                     dtvar_beg=date_accreditation_achieved,
                     dtvar_end=date_accreditation_end,
                     ordvar=accreditation_organization,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=MC07_Final);

  title3 "QC[07]: Summary Managed Care Accreditation Extract"; 
    select * from connection to tmsis_passthrough
      (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
         from MC07_Latest L
         left join MC07_Active A on L.submitting_state=A.submitting_state
         left join MC07_Date D   on L.submitting_state=D.submitting_state
         left join MC07_Final F  on L.submitting_state=F.submitting_state;)
      order by submitting_state;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_mc_macros.sas, process_07_accreditation);

  ** clean-up;
  execute (
    drop table MC07_Active;
	drop table MC07_Latest;
	drop table MC07_Date;
	drop table MC07_Final;
    drop table #MC07_Accreditation_Copy;
    drop table #MC07_Accreditation_Latest1;
    drop table #MC07_Accreditation_Latest2;
  ) by tmsis_passthrough;
%mend process_07_accreditation;

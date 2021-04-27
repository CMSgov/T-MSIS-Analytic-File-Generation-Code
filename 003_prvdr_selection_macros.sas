** ========================================================================== 
** program documentation 
** program     : 003_prvdr_selection_macros.sas
** description : selection macros for the T-MSIS provider segments
** calls       : copy_activerows
**               copy_activerows_nts
**               count_rows
**               screen_runid
**               screen_dates
**               remove_duprecs
** -------------------------------------------------------------------------- 
** history 
** date        | action 
** ------------+------------------------------------------------------------- 
** 03/16/2017  | program written (D. Whalen)
** 07/27/2017  | program updated (H. Cohen)
** 07/05/2018  | program updated (H. Cohen) CCB changes
** 10/08/2018  | program updated (H. Cohen) CCB changes
** 03/20/2019  | program updated (H. Cohen) CCB changes
** 02/24/2020  | program updated (H. Cohen) CCB changes
** 04/10/2020  | program updated (H. Cohen) CCB changes
** 09/08/2020  | program updated (H. Cohen) CCB changes
** ==========================================================================;


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
		and j.tot_actv_rcrds_prv02 > 0
		and j.submtg_state_cd in(&CUTOVER_FILTER)

	group by submtg_state_cd)
    order by submtg_state_cd;

%put run_ids = &run_ids;
%put state_ids = &state_ids;
%put combined_list = &combined_list;

%mend AWS_MAXID_pull_non_claim;

** --------------------------------------------------------------------------;

** 000-01 header segment;
%macro process_01_header (outtbl=);
  %put NOTE: ****** PROCESS_01_HEADER Start ******;
  %local cols01;
  %let cols01 = tms_run_id, 
                submitting_state;

  ** copy provider header table;
  execute( 
    %copy_activerows_nts(intbl=File_Header_Record_Provider,
                     collist=cols01,
                     outtbl=#Prov01_Header_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov01_Header_Copy, 
               cntvar=cnt_active, 
               outds=PRV01_Active);

%AWS_MAXID_pull_non_claim (&TMSIS_SCHEMA., tmsis_fil_prcsg_job_cntl, #Prov01_Header_Copy); 

/* combined_list from macro above used in creation of &outtbl */

  ** extract the latest (largest) T-MSIS run id for each state;
  execute(
    create table &outtbl 
           distkey(submitting_state)
           compound sortkey(tms_run_id, submitting_state) as
      select submitting_state,
             max(tms_run_id) as tms_run_id
      from #Prov01_Header_Copy
	  where (submitting_state,tms_run_id) in (&combined_list)
      group by submitting_state
      order by submitting_state;
  ) by tmsis_passthrough;

  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_latest, 
               outds=PRV01_Latest);

  title3 "QC[01]: Summary Provider Header Extract"; 
  select * from connection to tmsis_passthrough
   (select A.submitting_state, cnt_active, cnt_latest
      from PRV01_Active A
      left join PRV01_Latest L on A.submitting_state=L.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_01_header);

  ** clean-up;
  execute(
    drop table PRV01_Active;
    drop table PRV01_Latest;
    drop table #Prov01_Header_Copy;
  ) by tmsis_passthrough;
%mend process_01_header;


page;
** --------------------------------------------------------------------------;
** 000-02 main segment;
%macro process_02_main (runtbl=, outtbl=);
  %put NOTE: ****** PROCESS_02_MAIN Start ******;

  ** screen out all but the latest (largest) T-MSIS run id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state;
  execute( 
    %screen_runid (intbl=Prov_Attributes_Main, 
                   runtbl=&runtbl, 
                   runvars=runlist, 
                   outtbl=#Prov02_Main_Latest1, 
                   runtyp=M);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov02_Main_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV02_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols02;
  %let cols02 = tms_run_id,
                tms_reporting_period,
                submitting_state,
                submitting_state as submtg_state_cd,
                record_number,			
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
                prov_attributes_eff_date,
                prov_attributes_end_date,
                prov_doing_business_as_name,
                prov_legal_name,
                prov_organization_name,
                prov_tax_name,
                facility_group_individual_code,
                teaching_ind,
                prov_first_name,
                prov_middle_initial,
                prov_last_name,
                sex,
                ownership_code,
                prov_profit_status,
                %fix_old_dates(date_of_birth),
                %fix_old_dates(date_of_death),
                accepting_new_patients_ind;
				
  %local whr02;
  %let whr02 = %upper_case(submitting_state_prov_id) is not null;
  execute( 
  %copy_activerows(intbl=#Prov02_Main_Latest1,
                     collist=cols02,
					 whr=&whr02,
                     outtbl=#Prov02_Main_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov02_Main_Copy, 
               cntvar=cnt_active, 
               outds=PRV02_Active);

  ** screen for eligibility during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_dates (intbl=#Prov02_Main_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_attributes_eff_date, 
                   dtvar_end=prov_attributes_end_date, 
                   outtbl=#Prov02_Main_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov02_Main_Latest2, 
               cntvar=cnt_date, 
               outds=PRV02_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id;
  execute( 
    %remove_duprecs (intbl=#Prov02_Main_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_attributes_eff_date,
                     dtvar_end=prov_attributes_end_date,
                     ordvar=facility_group_individual_code,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV02_Final);

  title3 "QC[02]: Summary Provider Main Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV02_Latest L
         left join PRV02_Active A on L.submitting_state=A.submitting_state
         left join PRV02_Date D   on L.submitting_state=D.submitting_state
         left join PRV02_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_02_main);

  ** clean-up;
  execute(
    drop table PRV02_Active;
    drop table PRV02_Latest;
    drop table PRV02_Date;
    drop table PRV02_Final;
    drop table #Prov02_Main_Copy;
    drop table #Prov02_Main_Latest1;
    drop table #Prov02_Main_Latest2;
  ) by tmsis_passthrough;
%mend process_02_main;


page;
** 000-03 location data extract;
%macro process_03_locations (maintbl=, outtbl=);
  %put NOTE: ***** -process_03_loc_0_extract ------;

  ** screen out all but the latest (largest) T-MSIS run id ;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_runid (intbl=Prov_Location_And_Contact_Info, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#Prov03_Locations_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov03_Locations_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV03_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols03;
  %let cols03 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
				%upper_case(submitting_state_prov_id) as submitting_state_prov_id,
				%upper_case(prov_location_id) as prov_location_id,
                prov_addr_type,
                prov_location_and_contact_info_eff_date,
                prov_location_and_contact_info_end_date,
                addr_ln1,
                addr_ln2,
                addr_ln3,
                addr_city,
                %upper_case(addr_state) as addr_state,
                addr_zip_code,
                addr_county,
                addr_border_state_ind;

  %local whr03;
  %let whr03 = prov_addr_type=1 or prov_addr_type=3 or prov_addr_type=4;
  execute( 
    %copy_activerows(intbl=#Prov03_Locations_Latest1,
                     collist=cols03,
                     whr=&whr03,
                     outtbl=#Prov03_Locations_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov03_Locations_Copy, 
               cntvar=cnt_active, 
               outds=PRV03_Active);

  ** screen for locations during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_addr_type,
                 prov_location_id;
  execute( 
    %screen_dates (intbl=#Prov03_Locations_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_location_and_contact_info_eff_date, 
                   dtvar_end=prov_location_and_contact_info_end_date, 
                   outtbl=#Prov03_Locations_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov03_Locations_Latest2, 
               cntvar=cnt_date, 
               outds=PRV03_Date);
				 
  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 prov_addr_type,
                 prov_location_id;
  execute( 
    %remove_duprecs (intbl=#Prov03_Locations_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_location_and_contact_info_eff_date,
                     dtvar_end=prov_location_and_contact_info_end_date,
                     ordvar=prov_location_id,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV03_Final);

** create a location file _g0 with only tms_run_id submitting_state submitting_state_prov_id prov_location_id, and includes records with prov_location_id=000 from segments 4, 5, and 10 that are not in the location segment;
  execute(
      create table #loc_g
           diststyle key distkey(submitting_state_prov_id)
           compound sortkey(tms_run_id, submitting_state, submitting_state_prov_id) as
      select M.tms_run_id, M.submitting_state, M.submitting_state_prov_id, '000' as prov_location_id
	  from &maintbl M 
			 left join (select tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id from Prov_Licensing_Info where prov_location_id='000') L 
			 on M.tms_run_id=L.tms_run_id and M.submitting_state=L.submitting_state and M.submitting_state_prov_id=%upper_case(L.submitting_state_prov_id)
			 left join (select tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id from Prov_Identifiers where prov_location_id='000') I 
			 on M.tms_run_id=I.tms_run_id and M.submitting_state=I.submitting_state and M.submitting_state_prov_id=%upper_case(I.submitting_state_prov_id)
			 left join (select tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id from Prov_Bed_Type_Info where prov_location_id='000') B 
			 on M.tms_run_id=B.tms_run_id and M.submitting_state=B.submitting_state and M.submitting_state_prov_id=%upper_case(B.submitting_state_prov_id)
			 where L.prov_location_id='000' or I.prov_location_id='000' or B.prov_location_id='000'
			 group by M.tms_run_id, M.submitting_state, M.submitting_state_prov_id
      order by tms_run_id, submitting_state, submitting_state_prov_id;
  ) by tmsis_passthrough;

  execute(
	  create table #Prov03_Locations_g0
           diststyle key distkey(submitting_state_prov_id)
           compound sortkey(tms_run_id, submitting_state, submitting_state_prov_id) as
      select tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id 
	  from (select tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id from &outtbl 
			union all
			select * from #loc_g) 
	  group by tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id
      order by tms_run_id, submitting_state, submitting_state_prov_id, prov_location_id;
  ) by tmsis_passthrough;

  title3 "QC[03]: Summary Provider Locations Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV03_Latest L
         left join PRV03_Active A on L.submitting_state=A.submitting_state
         left join PRV03_Date D   on L.submitting_state=D.submitting_state
         left join PRV03_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_03_locations);

  ** clean-up;
  execute (
    drop table PRV03_Active;
    drop table PRV03_Latest;
    drop table PRV03_Date;
    drop table PRV03_Final;
    drop table #Prov03_Locations_Copy;
    drop table #Prov03_Locations_Latest1;
    drop table #Prov03_Locations_Latest2;
    drop table #loc_g;
  ) by tmsis_passthrough;

%mend process_03_locations;


page;
** --------------------------------------------------------------------------;
** 000-04 licensing segment;
%macro process_04_licensing (loctbl=, outtbl=);
  %put NOTE: ****** PROCESS_04_LICENSING Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id - location id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_location_id;
  execute( 
    %screen_runid (intbl=Prov_Licensing_Info, 
                   runtbl=&loctbl, 
                   runvars=runlist, 
                   outtbl=#Prov04_Licensing_Latest1, 
                   runtyp=L);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov04_Licensing_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV04_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols04;
  %let cols04 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
				%upper_case(prov_location_id) as prov_location_id,
                %upper_case(license_or_accreditation_number) as license_or_accreditation_number,
                license_type,
                %upper_case(license_issuing_entity_id) as license_issuing_entity_id,
                prov_license_eff_date,
                prov_license_end_date;

  %local whr04;
  %let whr04 = license_type is not null and %upper_case(license_or_accreditation_number) is not null;
  execute( 
    %copy_activerows(intbl=#Prov04_Licensing_Latest1,
                     collist=cols04,
                     whr=&whr04,
                     outtbl=#Prov04_Licensing_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov04_Licensing_Copy, 
               cntvar=cnt_active, 
               outds=PRV04_Active);

  ** screen for licensing during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_location_id,
                 license_type,
                 license_or_accreditation_number,
                 license_issuing_entity_id;
  execute( 
    %screen_dates (intbl=#Prov04_Licensing_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_license_eff_date, 
                   dtvar_end=prov_license_end_date, 
                   outtbl=#Prov04_Licensing_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov04_Licensing_Latest2, 
               cntvar=cnt_date, 
               outds=PRV04_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 prov_location_id,
                 license_type,
                 license_or_accreditation_number,
                 license_issuing_entity_id;
  execute( 
    %remove_duprecs (intbl=#Prov04_Licensing_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_license_eff_date,
                     dtvar_end=prov_license_end_date,
                     ordvar=license_type,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV04_Final);

  title3 "QC[04]: Summary Provider Licensing Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV04_Latest L
         left join PRV04_Active A on L.submitting_state=A.submitting_state
         left join PRV04_Date D   on L.submitting_state=D.submitting_state
         left join PRV04_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_04_licensing);

  ** clean-up;
  execute (
    drop table PRV04_Active;
    drop table PRV04_Latest;
    drop table PRV04_Date;
    drop table PRV04_Final;
    drop table #Prov04_Licensing_Copy;
    drop table #Prov04_Licensing_Latest1;
    drop table #Prov04_Licensing_Latest2;
  ) by tmsis_passthrough;

%mend process_04_licensing;


** --------------------------------------------------------------------------;
** 000-05 identifiers segment;
%macro process_05_identifiers (loctbl=, outtbl=);
  %put NOTE: ****** PROCESS_05_IDENTIFIERS Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id - location id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_location_id;
  execute( 
    %screen_runid (intbl=Prov_Identifiers, 
                   runtbl=&loctbl, 
                   runvars=runlist, 
                   outtbl=#Prov05_Identifiers_Latest1, 
                   runtyp=L);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov05_Identifiers_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV05_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols05;
  %let cols05 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
				%upper_case(prov_location_id) as prov_location_id,
                %upper_case(prov_identifier) as prov_identifier,
                prov_identifier_type,
                %upper_case(prov_identifier_issuing_entity_id) as prov_identifier_issuing_entity_id,
                prov_identifier_eff_date,
                prov_identifier_end_date;

  %local whr05;
  %let whr05 = prov_identifier_type is not null and %upper_case(prov_identifier) is not null;
  execute( 
    %copy_activerows(intbl=#Prov05_Identifiers_Latest1,
                     collist=cols05,
                     whr=&whr05,
                     outtbl=#Prov05_Identifiers_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov05_Identifiers_Copy, 
               cntvar=cnt_active, 
               outds=PRV05_Active);

  ** screen for Identifiers during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_location_id,
                 prov_identifier_type,
                 prov_identifier,
                 prov_identifier_issuing_entity_id;
  execute( 
    %screen_dates (intbl=#Prov05_Identifiers_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_identifier_eff_date, 
                   dtvar_end=prov_identifier_end_date, 
                   outtbl=#Prov05_Identifiers_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov05_Identifiers_Latest2, 
               cntvar=cnt_date, 
               outds=PRV05_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 prov_location_id,
                 prov_identifier_type,
                 prov_identifier,
                 prov_identifier_issuing_entity_id;
  execute( 
    %remove_duprecs (intbl=#Prov05_Identifiers_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_identifier_eff_date,
                     dtvar_end=prov_identifier_end_date,
                     ordvar=prov_identifier_type,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV05_Final);

  title3 "QC[05]: Summary Provider Identifiers Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV05_Latest L
         left join PRV05_Active A on L.submitting_state=A.submitting_state
         left join PRV05_Date D   on L.submitting_state=D.submitting_state
         left join PRV05_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_05_identifiers);

  ** clean-up;
  execute (
    drop table PRV05_Active;
    drop table PRV05_Latest;
    drop table PRV05_Date;
    drop table PRV05_Final;
    drop table #Prov05_Identifiers_Copy;
    drop table #Prov05_Identifiers_Latest1;
    drop table #Prov05_Identifiers_Latest2;
  ) by tmsis_passthrough;

%mend process_05_identifiers;


** 000-06 taxonomy segment;
%macro process_06_taxonomy (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_06_TAXONOMY Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_runid (intbl=Prov_Taxonomy_Classification, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#Prov06_Taxonomy_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov06_Taxonomy_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV06_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols06;
  %let cols06 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
                case
                  when (prov_classification_type='2' or prov_classification_type='3') and 
					length(trim(prov_classification_code))<2 and length(trim(prov_classification_code))>0 and 
					nullif(trim(upper(prov_classification_code)),'') is not null then lpad(trim(upper(prov_classification_code)),2,'0')
                  when prov_classification_type='4' and 
					length(trim(prov_classification_code))<3 and length(trim(prov_classification_code))>0 and 
					nullif(trim(upper(prov_classification_code)),'') is not null then lpad(trim(upper(prov_classification_code)),3,'0')
                  else nullif(trim(upper(prov_classification_code)),'')
				end as prov_classification_code,
                prov_classification_type,
                prov_taxonomy_classification_eff_date,
                prov_taxonomy_classification_end_date;

  %local whr06;
  %let whr06 = prov_classification_type is not null and %upper_case(prov_classification_code) is not null;
  execute( 
    %copy_activerows(intbl=#Prov06_Taxonomy_Latest1,
                     collist=cols06,
                     whr=&whr06,
                     outtbl=#Prov06_Taxonomy_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov06_Taxonomy_Copy, 
               cntvar=cnt_active, 
               outds=PRV06_Active);

  ** screen for Taxonomy during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_classification_type;
  execute( 
    %screen_dates (intbl=#Prov06_Taxonomy_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_taxonomy_classification_eff_date, 
                   dtvar_end=prov_taxonomy_classification_end_date, 
                   outtbl=#Prov06_Taxonomy_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov06_Taxonomy_Latest2, 
               cntvar=cnt_date, 
               outds=PRV06_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 prov_classification_type,
                 prov_classification_code;
  execute( 
    %remove_duprecs (intbl=#Prov06_Taxonomy_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_taxonomy_classification_eff_date,
                     dtvar_end=prov_taxonomy_classification_end_date,
                     ordvar=prov_classification_type,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV06_Final);

  title3 "QC[06]: Summary Provider Taxonomy Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV06_Latest L
         left join PRV06_Active A on L.submitting_state=A.submitting_state
         left join PRV06_Date D   on L.submitting_state=D.submitting_state
         left join PRV06_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_06_taxonomy);

  ** clean-up;
  execute (
    drop table PRV06_Active;
    drop table PRV06_Latest;
    drop table PRV06_Date;
    drop table PRV06_Final;
    drop table #Prov06_Taxonomy_Copy;
    drop table #Prov06_Taxonomy_Latest1;
    drop table #Prov06_Taxonomy_Latest2;
  ) by tmsis_passthrough;


%mend process_06_taxonomy;


** 000-07 Medicaid enrollment segment;
%macro process_07_medicaid (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_07_MEDICAID Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_runid (intbl=Prov_Medicaid_Enrollment, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#Prov07_Medicaid_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov07_Medicaid_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV07_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols07;
  %let cols07 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
                %zero_pad(prov_medicaid_enrollment_status_code, 2),
				state_plan_enrollment,
				prov_enrollment_method,
				%fix_old_dates(appl_date),
                %fix_old_dates(prov_medicaid_eff_date),
                %set_end_dt(prov_medicaid_end_date) as prov_medicaid_end_date;

  %local whr07;
  %let whr07 = prov_medicaid_enrollment_status_code is not null;
  execute( 
    %copy_activerows(intbl=#Prov07_Medicaid_Latest1,
                     collist=cols07,
                     whr=&whr07,
                     outtbl=#Prov07_Medicaid_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov07_Medicaid_Copy, 
               cntvar=cnt_active, 
               outds=PRV07_Active);

  ** screen for licensing during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_medicaid_enrollment_status_code;
  execute( 
    %screen_dates (intbl=#Prov07_Medicaid_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_medicaid_eff_date, 
                   dtvar_end=prov_medicaid_end_date, 
                   outtbl=#Prov07_Medicaid_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov07_Medicaid_Latest2, 
               cntvar=cnt_date, 
               outds=PRV07_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 prov_medicaid_enrollment_status_code;
  execute( 
    %remove_duprecs (intbl=#Prov07_Medicaid_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_medicaid_eff_date,
                     dtvar_end=prov_medicaid_end_date,
                     ordvar=prov_medicaid_enrollment_status_code,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV07_Final);

  title3 "QC[07]: Summary Provider Medicaid Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV07_Latest L
         left join PRV07_Active A on L.submitting_state=A.submitting_state
         left join PRV07_Date D   on L.submitting_state=D.submitting_state
         left join PRV07_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;
  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_07_medicaid);

  ** clean-up;
  execute (
    drop table PRV07_Active;
    drop table PRV07_Latest;
    drop table PRV07_Date;
    drop table PRV07_Final;
    drop table #Prov07_Medicaid_Copy;
    drop table #Prov07_Medicaid_Latest1;
    drop table #Prov07_Medicaid_Latest2;
  ) by tmsis_passthrough;

%mend process_07_medicaid;

** 000-08 affiliated groups segment;
%macro process_08_groups (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_08_GROUPS Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_runid (intbl=Prov_Affiliated_Groups, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#Prov08_AffGrps_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov08_AffGrps_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV08_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols08;
  %let cols08 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
                %upper_case(submitting_state_prov_id_of_affiliated_entity) as submitting_state_prov_id_of_affiliated_entity,
                prov_affiliated_group_eff_date,
                prov_affiliated_group_end_date;

  %local whr08;
  %let whr08 = %upper_case(submitting_state_prov_id_of_affiliated_entity) is not null;
  execute( 
    %copy_activerows(intbl=#Prov08_AffGrps_Latest1,
                     collist=cols08,
                     whr=&whr08,
                     outtbl=#Prov08_AffGrps_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov08_AffGrps_Copy, 
               cntvar=cnt_active, 
               outds=PRV08_Active);

  ** screen for licensing during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_dates (intbl=#Prov08_AffGrps_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_affiliated_group_eff_date, 
                   dtvar_end=prov_affiliated_group_end_date, 
                   outtbl=#Prov08_AffGrps_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov08_AffGrps_Latest2, 
               cntvar=cnt_date, 
               outds=PRV08_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 submitting_state_prov_id_of_affiliated_entity;
  execute( 
    %remove_duprecs (intbl=#Prov08_AffGrps_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_affiliated_group_eff_date,
                     dtvar_end=prov_affiliated_group_end_date,
                     ordvar=submitting_state_prov_id_of_affiliated_entity,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV08_Final);

  title3 "QC[08]: Summary Provider Affiliated Groups Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_active, cnt_latest, cnt_date, cnt_final
    from PRV08_Latest L
         left join PRV08_Active A on L.submitting_state=A.submitting_state
         left join PRV08_Date D   on L.submitting_state=D.submitting_state
         left join PRV08_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_08_groups);

  ** clean-up;
  execute (
    drop table PRV08_Active;
    drop table PRV08_Latest;
    drop table PRV08_Date;
    drop table PRV08_Final;
    drop table #Prov08_AffGrps_Copy;
    drop table #Prov08_AffGrps_Latest1;
    drop table #Prov08_AffGrps_Latest2;
  ) by tmsis_passthrough;
%mend process_08_groups;


** 000-09 affiliated programs segment;
%macro process_09_affpgms (maintbl=, outtbl=);
  %put NOTE: ****** PROCESS_09_AFFPGMS Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id;
  execute( 
    %screen_runid (intbl=Prov_Affiliated_Programs, 
                   runtbl=&maintbl, 
                   runvars=runlist, 
                   outtbl=#Prov09_AffPgms_Latest1);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov09_AffPgms_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV09_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols09;
  %let cols09 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
                %upper_case(affiliated_program_id) as affiliated_program_id,
                affiliated_program_type,
                prov_affiliated_program_eff_date,
                prov_affiliated_program_end_date;

  %local whr09;
  %let whr09 = %upper_case(affiliated_program_id) is not null;
  execute( 
    %copy_activerows(intbl=#Prov09_AffPgms_Latest1,
                     collist=cols09,
                     whr=&whr09,
                     outtbl=#Prov09_AffPgms_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov09_AffPgms_Copy, 
               cntvar=cnt_active, 
               outds=PRV09_Active);

  ** screen for licensing during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 affiliated_program_type;
  execute( 
    %screen_dates (intbl=#Prov09_AffPgms_Copy, 
                   keyvars=keylist,
                   dtvar_beg=prov_affiliated_program_eff_date, 
                   dtvar_end=prov_affiliated_program_end_date, 
                   outtbl=#Prov09_AffPgms_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov09_AffPgms_Latest2, 
               cntvar=cnt_date, 
               outds=PRV09_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 affiliated_program_type,
                 affiliated_program_id;
  execute( 
    %remove_duprecs (intbl=#Prov09_AffPgms_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=prov_affiliated_program_eff_date,
                     dtvar_end=prov_affiliated_program_end_date,
                     ordvar=affiliated_program_id,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV09_Final);

  title3 "QC[09]: Summary Provider Affiliated Programs Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV09_Latest L
         left join PRV09_Active A on L.submitting_state=A.submitting_state
         left join PRV09_Date D   on L.submitting_state=D.submitting_state
         left join PRV09_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_09_affpgms);

  ** clean-up;
  execute (
    drop table PRV09_Active;
    drop table PRV09_Latest;
    drop table PRV09_Date;
    drop table PRV09_Final;
    drop table #Prov09_AffPgms_Copy;
    drop table #Prov09_AffPgms_Latest1;
    drop table #Prov09_AffPgms_Latest2;
  ) by tmsis_passthrough;

%mend process_09_affpgms;


** 000-10 bed type segment;
%macro process_10_beds (loctbl=, outtbl=);
  %put NOTE: ****** PROCESS_10_BEDS Start ******;

  ** screen out all but the latest (largest) T-MSIS run id - provider id - location id;
  %local runlist;
  %let runlist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_location_id;
  execute( 
    %screen_runid (intbl=Prov_Bed_Type_Info, 
                   runtbl=&loctbl, 
                   runvars=runlist, 
                   outtbl=#Prov10_BedType_Latest1, 
                   runtyp=L);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov10_BedType_Latest1, 
               cntvar=cnt_latest, 
               outds=PRV10_Latest);

  ** select active records which meet segment specific criteria ;
  %local cols10;
  %let cols10 = tms_run_id,
                tms_reporting_period,
                record_number,
                submitting_state,
				submitting_state as submtg_state_cd,
                %upper_case(submitting_state_prov_id) as submitting_state_prov_id,
				%upper_case(prov_location_id) as prov_location_id,
                bed_count,
                case 
                	when trim(bed_type_code) in ('1','2','3','4') then trim(bed_type_code)
                	else null
				end as bed_type_code,
                bed_type_eff_date,
                bed_type_end_date;

  %local whr10;
  %let whr10 = (trim(bed_type_code) in ('1','2','3','4')) or (bed_count is not null and bed_count<>0);
  execute( 
    %copy_activerows(intbl=#Prov10_BedType_Latest1,
                     collist=cols10,
                     whr=&whr10,
                     outtbl=#Prov10_BedType_Copy);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov10_BedType_Copy, 
               cntvar=cnt_active, 
               outds=PRV10_Active);

  ** screen for licensing during the month;
  %local keylist;
  %let keylist = tms_run_id,
                 submitting_state,
                 submitting_state_prov_id,
                 prov_location_id,
                 bed_type_code;
  execute( 
    %screen_dates (intbl=#Prov10_BedType_Copy, 
                   keyvars=keylist,
                   dtvar_beg=bed_type_eff_date, 
                   dtvar_end=bed_type_end_date, 
                   outtbl=#Prov10_BedType_Latest2);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=#Prov10_BedType_Latest2, 
               cntvar=cnt_date, 
               outds=PRV10_Date);

  ** remove duplicate records;
  %local grplist;
  %let grplist = tms_run_id, 
                 submitting_state, 
                 submitting_state_prov_id,
                 prov_location_id,
                 bed_type_code;
  execute( 
    %remove_duprecs (intbl=#Prov10_BedType_Latest2, 
                     grpvars=grplist, 
                     dtvar_beg=bed_type_eff_date,
                     dtvar_end=bed_type_end_date,
                     ordvar=bed_type_code,
                     outtbl=&outtbl);
  ) by tmsis_passthrough;
  ** row count;
  %count_rows (intbl=&outtbl, 
               cntvar=cnt_final, 
               outds=PRV10_Final);

  title3 "QC[10]: Summary Provider Bed Type Extract"; 
  select * from connection to tmsis_passthrough
   (select L.submitting_state, cnt_latest, cnt_active, cnt_date, cnt_final
    from PRV10_Latest L
         left join PRV10_Active A on L.submitting_state=A.submitting_state
         left join PRV10_Date D   on L.submitting_state=D.submitting_state
         left join PRV10_Final F  on L.submitting_state=F.submitting_state;)
    order by submitting_state;

  %Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_prvdr_macros.sas, process_10_beds);

  ** clean-up;
  execute (
    drop table PRV10_Active;
    drop table PRV10_Latest;
    drop table PRV10_Date;
    drop table PRV10_Final;
    drop table #Prov10_BedType_Copy;
    drop table #Prov10_BedType_Latest1;
    drop table #Prov10_BedType_Latest2;
  ) by tmsis_passthrough;

%mend process_10_beds;

%* ========================================================================== 
** program documentation 
** program     : 002_prvdr_macros.sas
** description : collection of macros used by the PRV TAF build
** ==========================================================================;


%macro upper_case (textst);
nullif(trim(upper(&textst)),'')
%mend upper_case;

%macro set_end_dt (enddt);
	case 
	  when &enddt is null then '9999-12-31'::date 
	  when date_cmp(&enddt,'1600-01-01')=-1 then '1599-12-31'::date
	  else &enddt
	end 
%mend set_end_dt;

%macro zero_pad (var_cd, var_len);
	case
		when length(trim(&var_cd))<&var_len and length(trim(&var_cd))>0 and &var_cd is not null 
		then lpad(trim(upper(&var_cd)),&var_len,'0')
		else nullif(trim(upper(&var_cd)),'')
	end as &var_cd
%mend zero_pad;

%macro screen_runid (intbl=, runtbl=, runvars=, outtbl=, runtyp=C);
  create table &outtbl 
         diststyle key distkey(submitting_state_prov_id) 
         compound sortkey (&&&runvars) as
    select T.*
    from &intbl T
         inner join &runtbl R
		%if (&runtyp=M) %then %do;
           on %write_equalkeys(keyvars=&runvars, t1=T, t2=R)
		%end;
		%else %do;
			%if (&runtyp=L) %then %do;
           		on T.tms_run_id=R.tms_run_id and T.submitting_state=R.submitting_state and %upper_case(T.submitting_state_prov_id)=R.submitting_state_prov_id and %upper_case(T.prov_location_id)=R.prov_location_id
			%end;
			%else %do;
        		on T.tms_run_id=R.tms_run_id and T.submitting_state=R.submitting_state and %upper_case(T.submitting_state_prov_id)=R.submitting_state_prov_id
			%end;
		%end;
    order by %write_keyprefix(keyvars=&runvars, prefix=T);
%mend screen_runid;

%macro copy_activerows (intbl=, collist=, whr=, outtbl=);
    create table &outtbl
           diststyle key distkey(submitting_state_prov_id)
           compound sortkey(tms_run_id, submitting_state, submitting_state_prov_id) as
      select &&&collist
      from  &intbl
      where tms_is_active=1
      %if ("&whr" ne "") %then %do;
        and (&whr)
      %end;
      order by tms_run_id, submitting_state, submitting_state_prov_id;
%mend copy_activerows;

%macro copy_activerows_nts (intbl=, collist=, whr=, outtbl=);
    create table &outtbl
           diststyle even
           compound sortkey(tms_run_id, submitting_state) as
      select &&&collist
      from  (select *, submitting_state as submtg_state_cd from &intbl
		      where tms_is_active=1
				and tms_reporting_period is not null
				and tot_rec_cnt > 0
				and trim(submitting_state) not in ('94','96'))
	    %if %sysfunc(FIND(&ST_FILTER,%str(ALL))) = 0 %then %do;
        where &ST_FILTER
	    %end;
      order by tms_run_id, submitting_state;
%mend copy_activerows_nts;

%macro screen_dates (intbl=, keyvars=, dtvar_beg=, dtvar_end=, outtbl=);
  create table &outtbl 
         diststyle key
         distkey(submitting_state_prov_id) 
         compound sortkey (&&&keyvars) as
      select T.*
      from &intbl T
		 left join &DA_SCHEMA..state_submsn_type s
		 on T.submitting_state = s.submtg_state_cd
		 and upper(s.fil_type) = 'PRV'	  
  	  where (date_cmp(T.&dtvar_beg,&RPT_PRD) in(-1,0) and (date_cmp(T.&dtvar_end,&st_dt) in(0,1) or T.&dtvar_end is NULL))
		and ((upper(coalesce(s.submsn_type,'X')) <> 'CSO' and date_cmp(T.tms_reporting_period,&st_dt) in (1,0))
			or (upper(coalesce(s.submsn_type,'X')) = 'CSO'))
      order by %write_keyprefix(keyvars=&keyvars, prefix=T);
%mend screen_dates;

%macro remove_duprecs (intbl=, grpvars=, dtvar_beg=, dtvar_end=, ordvar=, outtbl=);
  /* limit data to the latest available reporting periods */
  create table #TblCopyGrouped
         distkey(submitting_state_prov_id) as
    select *,
           row_number() over (
             partition by &&&grpvars
             order by tms_reporting_period desc,
                 &dtvar_beg desc,
                 &dtvar_end desc,
                 record_number desc,
                 &ordvar asc 
           ) as _wanted
    from &intbl
    order by &&&grpvars;

  /* final upduplication step */
  create table &outtbl
         distkey(submitting_state_prov_id) 
         compound sortkey (&&&grpvars) as
    select *
    from #TblCopyGrouped
    where _wanted=1
    order by &&&grpvars;

  /* clean-up */
  drop table #TblCopyGrouped;
%mend remove_duprecs;

%macro count_rows (intbl=, cntvar=, outds=);
execute(
create temp table &outds as
select submitting_state, count(*) as &cntvar
         from &intbl 
         group by submitting_state
         order by submitting_state;
) by tmsis_passthrough;
%mend count_rows;

%macro recode_lookup (intbl=, srtvars=, fmttbl=, fmtnm=, srcvar=, newvar=, outtbl=, fldtyp=, fldlen=);
  create table &outtbl 
         diststyle key distkey(submitting_state_prov_id)
         compound sortkey (&&&srtvars) as
%if (&fldtyp=C) %then %do;
    select T.*, cast(F.label as varchar(&fldlen)) as &newvar
%end;
%else %do;
    select T.*, cast(F.label as smallint) as &newvar
%end;
    from &intbl T
         left join &fmttbl F
         on F.fmtname=&fmtnm and (Trim(T.&srcvar)>=F.start and Trim(T.&srcvar)<=F.end)
    order by %write_keyprefix(keyvars=&srtvars, prefix=T);
%mend recode_lookup;

%macro recode_notnull (intbl=, srtvars=, fmttbl=, fmtnm=, srcvar=, newvar=, outtbl=, fldtyp=, fldlen=);
  create table &outtbl 
         diststyle key distkey(submitting_state_prov_id)
         compound sortkey (&&&srtvars) as
    select T.*, 
%if (&fldtyp=C) %then %do;
		 case when F.label is null then T.&srcvar else F.label end :: varchar(&fldlen) as &newvar
%end;
%else %do;
		 case when F.label is null then T.&srcvar else F.label end :: smallint as &newvar
%end;
    from &intbl T
         left join &fmttbl F
         on F.fmtname=&fmtnm and (Trim(T.&srcvar)>=F.start and Trim(T.&srcvar)<=F.end)
    order by %write_keyprefix(keyvars=&srtvars, prefix=T);
%mend recode_notnull;

%macro write_equalkeys (keyvars=, t1=, t2=);
  %local k klist kvar;
  %let keylist = %sysfunc(translate(%bquote(&&&keyvars), ' ', ','));
  %let k = 1;
  %let kvar = %scan(&keylist, &k);
  %do %while (&kvar ne );
    %if %upcase(&kvar) ne DESC %then %do;
      %if &k gt 1 %then and;
      &t1..&kvar=&t2..&kvar
    %end;
    %let k = %eval(&k + 1);
    %let kvar = %scan(&keylist, &k);
  %end;
%mend write_equalkeys;

%macro write_keyprefix (keyvars=, prefix=);
  %local k klist kvar;
  %let keylist = %sysfunc(translate(%bquote(&&&keyvars), ' ', ','));
  %let k = 1;
  %let kvar = %scan(&keylist, &k);
  %do %while (&kvar ne );
    %if &k gt 1 %then ,;
    %if %upcase(&kvar) ne DESC %then &prefix..&kvar;
    %else desc;
    %let k = %eval(&k + 1);
    %let kvar = %scan(&keylist, &k);
  %end;
%mend write_keyprefix;

%macro nppes_tax(TMSIS_SCHEMA, id_intbl, tax_intbl, tax_outtbl);
  
** get NPPES taxonomy codes using NPI from PRV identifier segment;

  execute(
    create table #nppes_id1
	  distkey(prvdr_id) sortkey(prvdr_id) as
      select submtg_state_cd, submtg_state_cd as submitting_state, tmsis_run_id as tms_run_id, submtg_state_prvdr_id as submitting_state_prov_id, prvdr_id 
	  from &id_intbl. where prvdr_id_type_cd='2'
	  group by submtg_state_cd, tmsis_run_id, submtg_state_prvdr_id, prvdr_id
      order by prvdr_id;
  ) by tmsis_passthrough;
  
  %*drop_temp_tables(&id_intbl.);

** create a table with fewer columns for the initial record pull from NPPES table;

  execute(
    create table #nppes_id2
	  distkey(prvdr_id) as
      select prvdr_id
	  from #nppes_id1
	  group by prvdr_id
      order by prvdr_id;
  ) by tmsis_passthrough;


** link on NPI in NPPES set flags to identify primary taxonomy codes that should be included in the TAF classification segment;

	execute (
	create table #nppes_tax_flags as
	select  nppes.*, t2.prvdr_id
			,%do i=1 %to 14;
				nvl(nppes.hc_prvdr_prmry_txnmy_sw_&i.,' ')||
			%end;
				nvl(nppes.hc_prvdr_prmry_txnmy_sw_15,' ')
				as sw_positions
			,regexp_count(sw_positions,'Y') as taxo_switches
			,%do i=1 %to 14;
				nvl(substring(nppes.hc_prvdr_txnmy_cd_&i.,10,1),' ')||
			%end;
				nvl(substring(nppes.hc_prvdr_txnmy_cd_15,10,1),' ')
				as cd_positions
			,regexp_count(cd_positions,'X') as taxo_cnt
	from #nppes_id2 t2 left join &tmsis_schema..data_anltcs_prvdr_npi_data_vw nppes on t2.prvdr_id=cast(nppes.prvdr_npi as varchar)
	) by tmsis_passthrough;

  %drop_temp_tables(#nppes_id2);

** create #NPPES_tax0 and #NPPES_tax0b to save array contents as separate records;

  execute (
	create table #nppes_tax0 (
	    prvdr_npi integer,
		prvdr_id_chr varchar(12),
		prvdr_clsfctn_cd varchar(12)
	);
  ) by tmsis_passthrough;

  execute (
	create table #nppes_tax0b (
	    prvdr_npi integer,
		prvdr_id_chr varchar(12),
		prvdr_clsfctn_cd varchar(12)
	);
  ) by tmsis_passthrough;

** insert one NPPES taxonomy array element into #NPPES_tax0 - create separate records so that distinct values can to be inserted as rows into TAF taxonomy segment;

	execute (
	insert into #nppes_tax0
		   (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) 
	select distinct prvdr_npi, prvdr_id
		   ,case
			%do i=1 %to 15;
				 when taxo_switches = 1 and position('Y' in sw_positions)=&i. then nvl(hc_prvdr_txnmy_cd_&i.,' ')
			%end;
			%do i=1 %to 15;
				 when taxo_switches = 0 and taxo_cnt = 1 and position('X' in cd_positions)=&i. then nvl(hc_prvdr_txnmy_cd_&i.,' ')
			%end;
				 else null
			end as selected_txnmy_cd
	from #nppes_tax_flags
	where taxo_switches = 1 or (taxo_switches = 0 and taxo_cnt = 1)
	) by tmsis_passthrough;
	
** insert all primary NPPES taxonomy codes into #NPPES_tax0b if more than one - note the current NPPES table has no records with more than one primary taxonomy code;

  execute (

	%do a=1 %to 15;
		insert into #nppes_tax0b
		   (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) 
		   select prvdr_npi, prvdr_id, hc_prvdr_txnmy_cd_&a.
				from #nppes_tax_flags
				where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_&a.,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_&a.='Y';
	%end;

  ) by tmsis_passthrough;
 
  %drop_temp_tables(#nppes_tax_flags);
  
** add multiple NPPES primary taxonomy codes for any NPI to single NPPES primary taxonomy codes for NPIs;

  execute (

    create table #nppes_tax1
	  distkey(prvdr_id_chr) sortkey(prvdr_id_chr) as
      (select prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd
	  from #nppes_tax0)
	  union
	  (select prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd
	  from #nppes_tax0b)
      order by prvdr_npi;

  ) by tmsis_passthrough;

  %drop_temp_tables(#nppes_tax0);
  %drop_temp_tables(#nppes_tax0b);

** link primary taxonomy codes (identified as prvdr_clsfctn_cd and prvdr_clsfctn_type_cd='N') to #nppes_id1 for additional provider and state identifiers required in the TAF;

	execute (
	create table #nppes_tax_final as
	select distinct i.tms_run_id, i.submtg_state_cd, i.submitting_state, i.submitting_state_prov_id, 'N' as prvdr_clsfctn_type_cd, n.prvdr_clsfctn_cd
            from #nppes_id1 i left join #NPPES_tax1 n 
			on i.prvdr_id=n.prvdr_id_chr
			where n.prvdr_npi is not null
			order by 5) by tmsis_passthrough;

  %drop_temp_tables(#nppes_id1);
  %drop_temp_tables(#NPPES_tax1);
  
** add NPPES primary taxonomy codes to taxonomy codes submitted by the state for T-MSIS;

  execute(
    create table &tax_outtbl. as
	  (select tms_run_id, submtg_state_cd, submitting_state, submitting_state_prov_id, prvdr_clsfctn_type_cd, prvdr_clsfctn_cd
	  from &tax_intbl.)
	  union
      (select tms_run_id, submtg_state_cd, submitting_state, submitting_state_prov_id, prvdr_clsfctn_type_cd, prvdr_clsfctn_cd
	  from #nppes_tax_final)
      order by &srtlist;
  ) by tmsis_passthrough;

  %drop_temp_tables(#nppes_tax_final);
  
%mend nppes_tax;

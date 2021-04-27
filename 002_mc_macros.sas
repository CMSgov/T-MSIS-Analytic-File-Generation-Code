%* ========================================================================== 
** program documentation 
** program     : 002_mc_macros.sas
** description : collection of macros used by the MCP TAF build
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
		then lpad(trim(&var_cd),&var_len,'0')
		else nullif(trim(&var_cd),'')
	end as &var_cd
%mend zero_pad;

%macro screen_runid (intbl=, runtbl=, runvars=, outtbl=, runtyp=C);
  create table &outtbl 
         diststyle key distkey(state_plan_id_num) 
         compound sortkey (&&&runvars) as
    select T.*
    from &intbl T
         inner join &runtbl R
		%if (&runtyp=M) %then %do;
           on %write_equalkeys(keyvars=&runvars, t1=T, t2=R)
		%end;
		%else %do;
           on T.tms_run_id=R.tms_run_id and T.submitting_state=R.submitting_state and %upper_case(T.state_plan_id_num)=R.state_plan_id_num
		%end;
    order by %write_keyprefix(keyvars=&runvars, prefix=T);
%mend screen_runid;

%macro copy_activerows (intbl=, collist=, whr=, outtbl=);
    create table &outtbl
           diststyle key distkey(state_plan_id_num)
           compound sortkey(tms_run_id, submitting_state, state_plan_id_num) as
      select &&&collist
      from  &intbl
      where tms_is_active=1
      %if ("&whr" ne "") %then %do;
        and (&whr)
      %end;
      order by tms_run_id, submitting_state, state_plan_id_num;
%mend copy_activerows;

%macro copy_activerows_nts (intbl=, collist=, whr=, outtbl=);
    create table &outtbl
           diststyle even
           compound sortkey(tms_run_id, submitting_state) as
      select &&&collist
      from  &intbl
      where tms_is_active=1
		and tms_reporting_period is not null
		and tot_rec_cnt > 0
		and trim(submitting_state) <> '96'
	    %if %sysfunc(FIND(&ST_FILTER,%str(ALL))) = 0 %then %do;
        and &ST_FILTER
	    %end;
      order by tms_run_id, submitting_state;
%mend copy_activerows_nts;

%macro screen_dates (intbl=, keyvars=, dtvar_beg=, dtvar_end=, outtbl=);
  create table &outtbl 
         diststyle key
         distkey(state_plan_id_num) 
         compound sortkey (&&&keyvars) as
      select T.*
      from &intbl T
		 left join &DA_SCHEMA..state_submsn_type s
		 on T.submitting_state = s.submtg_state_cd
		 and upper(s.fil_type) = 'MCP'	  
      where (date_cmp(T.&dtvar_beg,&RPT_PRD) in(-1,0) and (date_cmp(T.&dtvar_end,&st_dt) in(0,1) or T.&dtvar_end is NULL))
             and ((upper(coalesce(s.submsn_type,'X')) <> 'CSO' and date_cmp(T.tms_reporting_period,&st_dt) in(1,0))
                   or (upper(coalesce(s.submsn_type,'X')) = 'CSO'))
      order by %write_keyprefix(keyvars=&keyvars, prefix=T);
%mend screen_dates;

%macro remove_duprecs (intbl=, grpvars=, dtvar_beg=, dtvar_end=, ordvar=, outtbl=);
  /* limit data to the latest available reporting periods */
  create table #TblCopyGrouped
         distkey(state_plan_id_num) as
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
         distkey(state_plan_id_num) 
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

%macro map_arrayvars (varnm=, N=, fldtyp=);
  %local I_ I;
  %do I_=1 %to &N;
    %let I = %sysfunc(putn(&I_,z2.));
		%if (&fldtyp=C) %then %do;
			, max(case when (_ndx=&I_) then nullif(trim(upper(&varnm)),'') else null end) as &varnm._&I
		%end;
		%else %do;
			, max(case when (_ndx=&I_) then &varnm else null end) as &varnm._&I
		%end;
  %end;
%mend map_arrayvars;

%macro recode_lookup (intbl=, srtvars=, fmttbl=, fmtnm=, srcvar=, newvar=, outtbl=, fldtyp=, fldlen=);
  create table &outtbl 
         diststyle key distkey(state_plan_id_num) 
         compound sortkey (&&&srtvars) as
%if (&fldtyp=C) %then %do;
    select T.*, cast(F._MClabel as varchar(&fldlen)) as &newvar
%end;
%else %do;
    select T.*, cast(F._MClabel as smallint) as &newvar
%end;
    from &intbl T
         left join &fmttbl F
         on F.fmtname=&fmtnm and (Trim(T.&srcvar)>=F._MCstart and Trim(T.&srcvar)<=F._MCend)
    order by %write_keyprefix(keyvars=&srtvars, prefix=T);
%mend recode_lookup;

%macro recode_notnull (intbl=, srtvars=, fmttbl=, fmtnm=, srcvar=, newvar=, outtbl=, fldtyp=, fldlen=);
  create table &outtbl 
         diststyle key distkey(state_plan_id_num) 
         compound sortkey (&&&srtvars) as
    select T.*, 
%if (&fldtyp=C) %then %do;
		 case when F._MClabel is null then T.&srcvar else F._MClabel end :: varchar(&fldlen) as &newvar
%end;
%else %do;
		 case when F._MClabel is null then T.&srcvar else F._MClabel end :: smallint as &newvar
%end;
    from &intbl T
         left join &fmttbl F
         on F.fmtname=&fmtnm and (Trim(T.&srcvar)>=F._MCstart and Trim(T.&srcvar)<=F._MCend)
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

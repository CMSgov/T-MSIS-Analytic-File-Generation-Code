** ==========================================================================
** program documentation 
** program     : annual_macros_pr.sas
** description : Macros to generate the PR TAF using the monthly PRV TAF tables 
** date        : 10/2019
** ==========================================================================;

/* Macro max_run_id to get the highest da_run_id for the given state for input monthly TAF. This
   table will then be merged back to the monthly TAF to pull all records for that state, month, and da_run_id.
   It is also inserted into the metadata table to keep a record of the state/month DA_RUN_IDs that make up 
   each annual run.
   To get the max run ID, must go to the job control table and get the latest national run, and then also
   get the latest state-specific run. Determine the later by state and month and then pull those IDs.
   Macro parms:
   	 inyear=input year, set to the current year */
   	
%macro max_run_id(file=, tbl=, inyear=&year.);

  %let filel=lower(&file.);
  %if &tbl. =  %then %let tbl=taf_&filel.;

	** For NON state-specific runs (where job_parms_text does not include submtg_state_cd in),
	   pull highest da_run_id by time ;

	execute (
		create temp table max_run_id_&file._&inyear._nat as
		select &file._fil_dt
               ,max(da_run_id) as da_run_id

		from (select substring(job_parms_txt,1,4)||substring(job_parms_txt,6,2) as &file._fil_dt
		             ,da_run_id

			from &DA_SCHEMA..job_cntl_parms
			where upper(fil_type) = %nrbquote('&file.') and 
	              sucsfl_ind=1 and
				  substring(job_parms_txt,1,4) = %nrbquote('&inyear.') and
				  charindex('submtg_state_cd in',regexp_replace(job_parms_txt, '\\s+', ' ')) = 0 )

		group by &file._fil_dt

	) by tmsis_passthrough; 

	** For state-specific runs (where job_parms_text includes submtg_state_cd in),
	   pull highest da_run_id by time and state;

	execute (
		create temp table max_run_id_&file._&inyear._ss as
		select &file._fil_dt
		       ,submtg_state_cd
               ,max(da_run_id) as da_run_id

		from (select substring(job_parms_txt,1,4)||substring(job_parms_txt,6,2) as &file._fil_dt
		             ,regexp_substr(substring(job_parms_txt,10), '([0-9]{2})') as submtg_state_cd
		             ,da_run_id

			from &DA_SCHEMA..job_cntl_parms
			where upper(fil_type) = %nrbquote('&file.') and 
	              sucsfl_ind=1 and
				  substring(job_parms_txt,1,4) = %nrbquote('&inyear.') and
				  charindex('submtg_state_cd in',regexp_replace(job_parms_txt, '\\s+', ' ')) > 0 ) 

		group by &file._fil_dt
		         ,submtg_state_cd

	) by tmsis_passthrough; 

	** Now join the national and state lists by month - take the national run ID if higher than
	   the state-specific, otherwise take the state-specific.
	   Must ALSO stack with the national IDs so they are not lost.
	   In outer query, get a list of unique IDs to pull;

	execute (
		create temp table job_cntl_parms_both_&file._&inyear. as

		select distinct &file._fil_dt
                        ,da_run_id

		from (
			select coalesce(a.&file._fil_dt,b.&file._fil_dt) as &file._fil_dt
				   ,case when a.da_run_id > b.da_run_id or b.da_run_id is null then a.da_run_id
				         else b.da_run_id
						 end as da_run_id

			from max_run_id_&file._&inyear._nat a
			     full join
				 max_run_id_&file._&inyear._ss b

			on a.&file._fil_dt = b.&file._fil_dt 

            union all 

            select &file._fil_dt
                   ,da_run_id
            from max_run_id_&file._&inyear._nat )

	) by tmsis_passthrough;

	** Now join to EFTS data to get table of month/state/run IDs to use for data pull.
	   Note must then take the highest da_run_id by state/month (if any state-specific runs 
	   were identified as being later than a national run);

	execute (
		create temp table max_run_id_&file._&inyear. as 
		select a.&file._fil_dt
		       ,b.submtg_state_cd
			   ,max(b.da_run_id) as da_run_id

		from job_cntl_parms_both_&file._&inyear. a
		     inner join
			 (select da_run_id, incldd_state_cd as submtg_state_cd 
              from &DA_SCHEMA..efts_fil_meta where incldd_state_cd != 'Missing' ) b

		on a.da_run_id = b.da_run_id

		%if %sysfunc(find(&ST_FILTER.,%str(ALL))) = 0 %then %do;
         	where &ST_FILTER.
	    %end;

		group by a.&file._fil_dt
		        ,b.submtg_state_cd

	) by tmsis_passthrough;

	/* Insert into metadata table so we keep track of all monthly DA_RUN_IDs     
	   that go into each annual &fil_typ. file */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_&fil_typ._INP_SRC
		select
			&DA_RUN_ID. as ANN_DA_RUN_ID,
			SUBMTG_STATE_CD,
			&file._FIL_DT,
			DA_RUN_ID as SRC_DA_RUN_ID

		from max_run_id_&file._&inyear.
	) by tmsis_passthrough; 

%mend max_run_id;


/* Macro join_monthly to join the max da_run_ids for the given state/month back to the monthly TAF and
   then join each month by submtg_state_cd and main_id (plan or provider). Note this table will be pulled into the subquery in the
   creation of each base and supplemental segment. */
   
%macro join_monthly;

%let file=PRV;

  /* Create the backbone of unique state/msis_id to then left join each month back to */
  
	(select a.submtg_state_cd,
           b.&main_id.,
           count(a.submtg_state_cd) as nmonths
         
		from max_run_id_&file._&inyear. a
        inner join
		&DA_SCHEMA..taf_&fileseg. b
	 
		on a.submtg_state_cd = b.submtg_state_cd and
	    a.&file._fil_dt = b.&file._fil_dt and
	    a.da_run_id = b.da_run_id     
	    
		group by a.submtg_state_cd,
              b.&main_id.
	) as fseg
          
   /* Now loop through each month to left join back to the backbone */
   
   	%do m=1 %to 12;
	
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
			
			left join 
			
		(select b.*
		  from
			max_run_id_&file._&inyear. a
			inner join
			&DA_SCHEMA..taf_&fileseg. b
		 
		  on a.submtg_state_cd = b.submtg_state_cd and
		    a.&file._fil_dt = b.&file._fil_dt and
		    a.da_run_id = b.da_run_id
		    
		  where substring(a.&file._fil_dt,5,2)=%nrbquote('&m.')
		) as m&m.
		 
		 on fseg.submtg_state_cd=m&m..submtg_state_cd and
		    fseg.&main_id.=m&m..&main_id.

		%end; /* end month loop */

%mend join_monthly;

/* Macro all_monthly_segment(intbl=, filet=) to join the records with max da_run_ids for the given state/month back to the monthly TAF and
   select all records for the target year (plan or provider). Note: this table will be the source for creation of annual supplemental segments. */

%macro all_monthly_segments(filet=);

	/* Create file that includes state/id and other data elements for all records in the year for this segment */
	select	b.*
	from
		 max_run_id_&filet._&year. a
		 inner join
	   %if &filet.=PRV %then %do;
		 &DA_SCHEMA..taf_&filet._&fileseg. b
	   %end;
	   %else %do;
		 &DA_SCHEMA..taf_&fileseg. b
	   %end;
		 on a.submtg_state_cd = b.submtg_state_cd and
		    a.&filet._fil_dt = b.&filet._fil_dt and
		    a.da_run_id = b.da_run_id

%mend all_monthly_segments;

/* Macro create_temp_table to create each main table. For each table, there are columns we must get from the raw data in
   the subquery, and then columns we must get from the outer query that pulls from the subquery.
   Macro parms:
	fileseg: PRV options PRV/PRV_LOC/PRV_GRP/PRV_PGM/PRV_TAX/PRV_ENR/PRV_LIC/PRV_IDT/PRV_BED
   	tblname=table name
   	subcols=creation statements for all columns that must be pulled from the raw data in the subquery
   	outercols=creation statements for all columns that must be pulled from the subquery
    subcols2 - subcols8=additional subcols when needing to loop over MC and waiver slots, because cannot
                        loop over all slots within one macro var or will exceed text limit of 65534 chars */

%macro create_temp_table(fileseg=, tblname=, inyear=&year., subcols=, outercols=, subcols2=, subcols3=, subcols4=, subcols5=,
                         subcols6=,subcols7=,subcols8=);

	execute(
		create temp table &tblname._&year.
		distkey(&main_id.)
		sortkey(submtg_state_cd,&main_id.) as
		
			select * &outercols.
			
				from (
					select fseg.submtg_state_cd
					       ,fseg.&main_id.
					       &subcols.
						   &subcols2.
						   &subcols3.
						   &subcols4.
						   &subcols5.
						   &subcols6.
						   &subcols7.
						   &subcols8.
					       
					from ( %join_monthly ) 			
				
				) sub
				
		order by submtg_state_cd,
		         &main_id.				 
	
	) by tmsis_passthrough;

%mend create_temp_table;

%macro annual_segment (fileseg=, dtfile=, collist=, mnths=, outtbl=);
  /* fileseg - identifies which file segment is being created
  ** dtfile - XXX part of XXX_FIL_DT file date field with YYYYMM values
  ** collist - macro variable with unique file grouping vars (separated by commas)
  ** mnths - base name for the monthly flag fields
  ** outtbl - name of the output table
  */

  execute (
			create table #Temp_Rollup as
             select SUBMTG_STATE_CD, &main_id., &&&collist, 
                    sum(case when (substring(&dtfile._fil_dt,5,2)='01') then 1 else 0 end) as _01,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='02') then 1 else 0 end) as _02,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='03') then 1 else 0 end) as _03,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='04') then 1 else 0 end) as _04,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='05') then 1 else 0 end) as _05,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='06') then 1 else 0 end) as _06,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='07') then 1 else 0 end) as _07,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='08') then 1 else 0 end) as _08,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='09') then 1 else 0 end) as _09,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='10') then 1 else 0 end) as _10,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='11') then 1 else 0 end) as _11,
                    sum(case when (substring(&dtfile._fil_dt,5,2)='12') then 1 else 0 end) as _12
             from ( %all_monthly_segments(filet=&dtfile.) )
             group by SUBMTG_STATE_CD, &main_id., &&&collist
             order by SUBMTG_STATE_CD, &main_id., &&&collist;

			create temp table &outtbl as
             select SUBMTG_STATE_CD, &main_id., &&&collist, 
                    case when (_01>0) then 1 else 0 end :: smallint as &mnths._01,
                    case when (_02>0) then 1 else 0 end :: smallint as &mnths._02,
                    case when (_03>0) then 1 else 0 end :: smallint as &mnths._03,
                    case when (_04>0) then 1 else 0 end :: smallint as &mnths._04,
                    case when (_05>0) then 1 else 0 end :: smallint as &mnths._05,
                    case when (_06>0) then 1 else 0 end :: smallint as &mnths._06,
                    case when (_07>0) then 1 else 0 end :: smallint as &mnths._07,
                    case when (_08>0) then 1 else 0 end :: smallint as &mnths._08,
                    case when (_09>0) then 1 else 0 end :: smallint as &mnths._09,
                    case when (_10>0) then 1 else 0 end :: smallint as &mnths._10,
                    case when (_11>0) then 1 else 0 end :: smallint as &mnths._11,
                    case when (_12>0) then 1 else 0 end :: smallint as &mnths._12
             from #Temp_Rollup
             order by SUBMTG_STATE_CD, &main_id., &&&collist;

           drop table #Temp_Rollup;

          ) by tmsis_passthrough;


%mend annual_segment;

/* Macro create_segment, which will be called for each of the segments to run the respective
   create macro, which includes output to the permanent table. This macro will then get the count
   of records in that table, and output to the metadata table. (Note macro CREATE_META_INFO is in
   AWS_Shared_Macros) 
   Macro parms:
     -tblname=name of the output permanent table name (after TAF_ANN_&fil_typ._)
     -FIL_4TH_NODE=abbreviated name of the table to go into the metadata table */

%macro create_segment(tblname, FIL_4TH_NODE);

	%create_&tblname.;

	sysecho 'in get cnt';
	%get_ann_count(&tblname.);

	sysecho 'create metainfo'; 
	%CREATE_META_INFO(&DA_SCHEMA., TAF_ANN_&fil_typ._&tblname., &DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);

	sysecho 'create EFT file metadata';
	%create_efts_metadata;

%mend create_segment;

/* Macro last_best to take the last best value (go backwards in time from month 12 to month 1, taking the first non-missing/null value).
   Macro parms:
     incol=input monthly column
     outcol=name of column to be output, where default is the same name of the incol */

%macro last_best(incol, outcol=);

	%if &outcol.=  %then %let outcol = &incol.;
	
		,coalesce(m12.&incol., m11.&incol., m10.&incol., m09.&incol., 
			        m08.&incol., m07.&incol., m06.&incol., m05.&incol.,
			        m04.&incol., m03.&incol., m02.&incol., m01.&incol.) 

	as &outcol. 

%mend last_best;

/* Macro monthly_array to take the raw monthly columns and array into columns with _MO suffixes.
   Macro parms:
      incol=input monthly column
      outcol=name of column to be output, where default is the name of the incol with _MO for each month appended as a suffix
      nslots=# of slots (used for columns like MC or waiver where we have multiple slots) - default is 1, and for those with
             slots>1, we will add slot # before _MO suffix */

%macro monthly_array(incol, outcol=, nslots=1);

	%if &outcol.=  %then %let outcol = &incol.;

	%do s=1 %to &nslots.;
		%if &nslots.=1 %then %let snum= ;
		%else %let snum=&s.;
	
		,m01.&incol.&snum. as &outcol.&snum._01
		,m02.&incol.&snum. as &outcol.&snum._02
		,m03.&incol.&snum. as &outcol.&snum._03
		,m04.&incol.&snum. as &outcol.&snum._04
		,m05.&incol.&snum. as &outcol.&snum._05
		,m06.&incol.&snum. as &outcol.&snum._06
		,m07.&incol.&snum. as &outcol.&snum._07
		,m08.&incol.&snum. as &outcol.&snum._08
		,m09.&incol.&snum. as &outcol.&snum._09
		,m10.&incol.&snum. as &outcol.&snum._10
		,m11.&incol.&snum. as &outcol.&snum._11
		,m12.&incol.&snum. as &outcol.&snum._12

	%end;
	
%mend monthly_array;

/* Macro ever_year to look across all monthly columns and create an indicator for whether ANY of the monthly
   columns meet the given condition. The default condition is = 1.
   Macro parms:
      incol=input monthly column
      condition=monthly condition to be evaulated, where default is = 1
      raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
          in an earlier subquery and will therefore have the _MO suffixes, where default = 1
      outcol=name of column to be output, where default is the name of the incol
      usenulls=indicator to determine whether to use the nullif function to compare both nulls AND another value,
               where default is = 0 
      nullcond=additional value to look for when usenulls=1 */
      
%macro ever_year(incol, condition=%nrstr(=1), raw=1, outcol=, usenulls=0, nullcond= );

	%if &outcol.=  %then %let outcol = &incol.;
	
	%if &usenulls.=0 %then %do;
	
		%if &raw.=1 %then %do;

			,case when m01.&incol. &condition. or 
			           m02.&incol. &condition. or 
			           m03.&incol. &condition. or 
			           m04.&incol. &condition. or 
			           m05.&incol. &condition. or 
			           m06.&incol. &condition. or 
			           m07.&incol. &condition. or 
			           m08.&incol. &condition. or 
			           m09.&incol. &condition. or 
			           m10.&incol. &condition. or 
			           m11.&incol. &condition. or 
			           m12.&incol. &condition.
			          
		%end;
		
		%if &raw.=0 %then %do;
		
			,case when &incol._01 &condition. or 
			           &incol._02 &condition. or 
			           &incol._03 &condition. or 
			           &incol._04 &condition. or 
			           &incol._05 &condition. or 
			           &incol._06 &condition. or 
			           &incol._07 &condition. or 
			           &incol._08 &condition. or 
			           &incol._09 &condition. or 
			           &incol._10 &condition. or 
			           &incol._11 &condition. or 
			           &incol._12 &condition.
		%end;
		
	%end; /* end usenulls=0 loop */
	
	%if &usenulls.=1 %then %do;
	
		%if &raw.=1 %then %do;

			,case when nullif(m01.&incol.,&nullcond.) &condition. or 
			           nullif(m02.&incol.,&nullcond.) &condition. or 
			           nullif(m03.&incol.,&nullcond.) &condition. or 
			           nullif(m04.&incol.,&nullcond.) &condition. or 
			           nullif(m05.&incol.,&nullcond.) &condition. or 
			           nullif(m06.&incol.,&nullcond.) &condition. or 
			           nullif(m07.&incol.,&nullcond.) &condition. or 
			           nullif(m08.&incol.,&nullcond.) &condition. or 
			           nullif(m09.&incol.,&nullcond.) &condition. or 
			           nullif(m10.&incol.,&nullcond.) &condition. or 
			           nullif(m11.&incol.,&nullcond.) &condition. or 
			           nullif(m12.&incol.,&nullcond.) &condition.
			          
		%end;
		
		%if &raw.=0 %then %do;
		
			,case when nullif(&incol._01,&nullcond.) &condition. or 
			           nullif(&incol._02,&nullcond.) &condition. or 
			           nullif(&incol._03,&nullcond.) &condition. or 
			           nullif(&incol._04,&nullcond.) &condition. or 
			           nullif(&incol._05,&nullcond.) &condition. or 
			           nullif(&incol._06,&nullcond.) &condition. or 
			           nullif(&incol._07,&nullcond.) &condition. or 
			           nullif(&incol._08,&nullcond.) &condition. or 
			           nullif(&incol._09,&nullcond.) &condition. or 
			           nullif(&incol._10,&nullcond.) &condition. or 
			           nullif(&incol._11,&nullcond.) &condition. or 
			           nullif(&incol._12,&nullcond.) &condition.
		%end;	
	
	
	%end; /* end usenulls=1 loop */
	          
	then 1
	else 0
	end as &outcol.

%mend ever_year;

/* Macro any_month to look at multiple columns within a month to evaluate whether ANY
   of the months meets a certain condition, and output monthly indicators.
   Macro parms:
   	incols=list of input columns to be evaluated, separated by spaces
   	outcol=name of the output col with the indicators, with the _MO suffix appended
   	condition=monthly condition to be evaulated, where default is = 1 */
   	
%macro any_month(incols,outcol,condition=%nrstr(=1));

		%do m=1 %to 12;
	
			%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
			
			,case when %do c=1 %to %sysfunc(countw(&incols.));
			              %let col=%scan(&incols.,&c.);
			              
			              m&m..&col. &condition. %if &c. < %sysfunc(countw(&incols.)) %then %do; or %end;
			              
			           %end; 
			           
			      then 1			           
			      else 0
			      end as &outcol._&m.		
			
		%end;

%mend any_month;

/* Macro any_col to look across a list of columns (non-monthly) to determine if ANY meet a given
   condition. The default condition is = 1.
   Macro parms:
      incols=input columns
      outcol=name of column to be output 
      condition=monthly condition to be evaulated, where default is = 1 */
      
%macro any_col(incols, outcol, condition=%nrstr(=1));

	,case when %do c=1 %to %sysfunc(countw(&incols.));
		        	%let col=%scan(&incols.,&c.);
		        	
		        	%if &c.>1 %then %do; or %end; &col. &condition.
		        
		        %end;
		        	
		   then 1 else 0 
		   end as &outcol.


%mend any_col;
   	

/* Macro sum_months to take a SUM over all the input months.
	 Macro parms:
      incol=input monthly column which will be summed (with _MO suffix for each month)
      raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
          in an earlier subquery and will therefore have the _MO suffixes, where default = 0
      outcol=output column with summation, where the default is the incol name with the _MONTHS suffix */
      
%macro sum_months(incol, raw=0, outcol=);

	%if &outcol.=  %then %let outcol = &incol._MOS;
	
	%if &raw.=1 %then %do;

		,coalesce(m01.&incol.,0) + coalesce(m02.&incol.,0) + coalesce(m03.&incol.,0) + 
		 coalesce(m04.&incol.,0) + coalesce(m05.&incol.,0) + coalesce(m06.&incol.,0) + 
		 coalesce(m07.&incol.,0) + coalesce(m08.&incol.,0) + coalesce(m09.&incol.,0) + 
		 coalesce(m10.&incol.,0) + coalesce(m11.&incol.,0) + coalesce(m12.&incol.,0)
		     
	%end;
	
	%if &raw.=0 %then %do;

		,coalesce(&incol._01,0) + coalesce(&incol._02,0) + coalesce(&incol._03,0) + 
		 coalesce(&incol._04,0) + coalesce(&incol._05,0) + coalesce(&incol._06,0) + 
		 coalesce(&incol._07,0) + coalesce(&incol._08,0) + coalesce(&incol._09,0) + 
		 coalesce(&incol._10,0) + coalesce(&incol._11,0) + coalesce(&incol._12,0)
		     
	%end;
	     
	  as &outcol.

%mend sum_months;

/* Macro nonmiss_month to loop through given variable from month 12 to 1 and identify the month with
   the first non-missing value. This will then be used to pull additional columns that should be paired
   with that month. The month = 00 if NO non-missing month.
   Macro parms:
   	incol=input monthly column
   	outcol=output column with month number, where the default is the incol name with the _MN (month number) suffix */
   
%macro nonmiss_month(incol,outcol=);

	%if &outcol.=  %then %let outcol = &incol._MN;
   
	,case %do mb=1 %to 12;
		       %let m=%scan(&monthsb.,&mb.);
		         
		       when m&m..&incol. is not null and m&m..&incol. not in ('',' ') then %nrbquote('&m.') 

		    %end;
		       
	 else '00' 
	end as &outcol.   
   
%mend nonmiss_month;

/* Macro ind_nonmiss_month to loop through individual provider variables from month 12 to 1 and identify the month with
   the first non-missing value for any of those variables. This will then be used to get those variables from that month if needed. The month = 00 if NO non-missing month. */
   
%macro ind_nonmiss_month;

	%let outcol = ind_any_MN;
   
	,case 
		%do mb=1 %to 12;

			%let m=%scan(&monthsb.,&mb.);
		         
			when (m&m..PRVDR_1ST_NAME is not null and m&m..PRVDR_1ST_NAME not in ('',' ')) or 
				(m&m..PRVDR_MDL_INITL_NAME is not null and m&m..PRVDR_MDL_INITL_NAME not in ('',' ')) or 
				(m&m..PRVDR_LAST_NAME is not null and m&m..PRVDR_LAST_NAME not in ('',' ')) or 
				(m&m..GNDR_CD is not null and m&m..GNDR_CD not in ('',' ')) or 
				(m&m..BIRTH_DT is not null) or 
				(m&m..DEATH_DT is not null) or 
				(m&m..AGE_NUM is not null) then %nrbquote('&m.') 

		%end;
		       
		else '00' 
	end as &outcol.   
   
%mend ind_nonmiss_month;

/* Macro assign_nonmiss_month looks at the values for the monthly variables assigned in nonmiss_month (or ind_nonmiss_month),
   and pulls multiple variables for that month based on the assigned month from nonmiss_month. Note
   this can be based on 1 or 2 monthly assignments from nonmiss_month, where the first is evaluated and
   if a month is never assigned to that variable, the second will be evaluated. Note that nonmiss_month (or ind_nonmiss_month)
   must be run in the subquery before assign_nonmiss_month is run in the outer query.
				   
   Macro parms:
   	outcol=column to assign based on the month captured in nonmiss_month
   	monthval1=monthly value to evaluate captured in nonmiss_month
   	incol1=input column to assign if monthval1 is met
   	monthval2=optional monthly value to evaluate captured in nonmiss_month  (or ind_nonmiss_month), IF monthval1 not'01'-'12'
   	incol2=optional input column to assign if monthval2 is met */
   
   
%macro assign_nonmiss_month(outcol, monthval1, incol1, monthval2=, incol2= );

	,case 
		%do mb=1 %to 12;

			%let m=%scan(&monthsb.,&mb.);
			when &monthval1.=%nrbquote('&m.') then &incol1._&m.
			     
		%end;
			  
		%if &monthval2. ne  %then %do;
			  
			%do mb=1 %to 12;

				%let m=%scan(&monthsb.,&mb.);
				when &monthval2.=%nrbquote('&m.') then &incol2._&m.
				     
			%end;
				  
		%end;
			  
		else null
	end as &outcol.

%mend assign_nonmiss_month;


%macro monthly_array_ind_raw(incol, outcol=);

	%if &outcol.=  %then %let outcol = &incol.;
									 
	%do m=1 %to 12;
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));

		,case when m&m..&incol. is not null then 1 else 0
	          end :: smallint as &outcol._&m.
	%end;

%mend monthly_array_ind_raw;


%macro map_arrayvars (varnm=, N=);

  %local I_ I;
  %do I_=1 %to &N;
    %let I = %sysfunc(putn(&I_,z2.));
		, max(case when (_ndx=&I_) then &varnm else null end) as &varnm._&I
  %end;
  
%mend map_arrayvars;

/* Create temp table <segment name>._SPLMTL to join to base */

%macro create_splmlt (segname=, segfile=);
	execute(

		create temp table &segname._SPLMTL_&year.
		distkey(&main_id.)
		sortkey(submtg_state_cd,&main_id.) as

		select submtg_state_cd
	   		   ,&main_id.
			   ,count(submtg_state_cd) as &segname._SPLMTL_CT

		from &segfile.

		group by submtg_state_cd,&main_id.
			   
	) by tmsis_passthrough;

%mend create_splmlt;

/* Macro drop_tables to drop temp tables.
   Macro parms:
   	temptables=list of tables to drop */

%macro drop_tables(temptables);

	%do i=1 %to %sysfunc(countw(&temptables.));

      %let table = %scan(&temptables.,&i.);

	  execute (
        drop table if exists &table. 
	  ) by tmsis_passthrough;

	%end;

%mend drop_tables;

/* Macro table_id_cols to add the 6 cols that are the same across all tables into the final insert select
   statement (DA_RUN_ID, &fil_typ._LINK_KEY, &fil_typ._LOC_LINK_KEY, &fil_typ._FIL_DT, &fil_typ._VRSN, SUBMTG_STATE_CD, &main_id)
   fil_typ - so this can be used for more than one TAF file type */

%macro table_id_cols (loctype=0);

	 &DA_RUN_ID. as DA_RUN_ID
 	 %if &loctype.=0 or &loctype.=1 %then %do;
		,cast ((%nrbquote('&DA_RUN_ID.') || '-' || %nrbquote('&YEAR.') || '-' || %nrbquote('&VERSION.') || '-' ||
				SUBMTG_STATE_CD || '-' || &main_id.) as varchar(56)) as &fil_typ._LINK_KEY
	 	%if &loctype.=1 %then %do;
			,cast ((%nrbquote('&DA_RUN_ID.') || '-' || %nrbquote('&YEAR.') || '-' || %nrbquote('&VERSION.') || '-' ||
					SUBMTG_STATE_CD || '-' || &main_id. || '-' || coalesce(PRVDR_LCTN_ID, '**')) as varchar(74)) as &fil_typ._LOC_LINK_KEY
		%end;
	 %end;
	 %else %if &loctype.=2 %then %do;
		,cast ((%nrbquote('&DA_RUN_ID.') || '-' || %nrbquote('&YEAR.') || '-' || %nrbquote('&VERSION.') || '-' ||
				SUBMTG_STATE_CD || '-' || &main_id. || '-' || coalesce(PRVDR_LCTN_ID, '**')) as varchar(74)) as &fil_typ._LOC_LINK_KEY
	 %end;

	 ,%nrbquote('&YEAR.') as &fil_typ._FIL_DT
	 ,%nrbquote('&VERSION.') as &fil_typ._VRSN
	 ,SUBMTG_STATE_CD
	 ,&main_id

%mend table_id_cols;

/* Macro get_ann_cnt to get the count of the given table and put the count into a macro var 
   Macro parms: tblname=perm table name */

%macro get_ann_count(tblname);

	 create table record_count as
	 select * from connection to tmsis_passthrough
	 ( select count(submtg_state_cd) as row_cnt
		 from &DA_SCHEMA..TAF_ANN_&fil_typ._&tblname.
		 where da_run_id = &da_run_id. 
	  );

	  select row_cnt 
      into :rowcount
	  from record_count;

%mend get_ann_count;

/* Macro create_efts_metadata to get the count of the given table by state and insert into the EFT
   metadata table. Will be called in the get_segment macro. */

%macro create_efts_metadata;

	/* Create state counts and insert into metadata table */

	 execute (
	 	create temp table state_counts as
		select da_run_id,
               submtg_state_cd,
		       count(submtg_state_cd) as rowcount_state

		 from &DA_SCHEMA..TAF_ANN_&fil_typ._&tblname.
		 where da_run_id = &da_run_id. 
		 group by da_run_id,
		          submtg_state_cd

	  ) by tmsis_passthrough;

	  /* Insert the following values into the table:
	     - DA_RUN_ID
	     - 4th node text value
	     - table name
	     - year
	     - version number
	     - overall table count
	     - run date
	     - state code
	     - table count for given state 
	     - fil_dt
	     - version */

	  execute (
	  	insert into &DA_SCHEMA..EFTS_FIL_META
		(da_run_id,fil_4th_node_txt,otpt_name,rptg_prd,itrtn_num,tot_rec_cnt,fil_cret_dt,incldd_state_cd,rec_cnt_by_state_cd,fil_dt,taf_cd_spec_vrsn_name)

		select a.da_run_id
		       ,%nrbquote('&FIL_4TH_NODE.') as fil_4th_node_txt
			   ,%nrbquote('TAF_ANN_&fil_typ._&tblname.') as otpt_name
			   ,%nrbquote('&YEAR.') as rptg_prd
			   ,%nrbquote('&VERSION.') as itrtn_num
			   ,&rowcount. as tot_rec_cnt
			   ,to_char(date(c.rec_add_ts),'MM/DD/YYYY') as fil_cret_dt
			   ,submtg_state_cd as incldd_state_cd
			   ,rowcount_state as rec_cnt_by_state_cd
			   ,%nrbquote('&YEAR.') as fil_dt
			   ,b.taf_cd_spec_vrsn_name 

		from state_counts a
		     inner join
			 &DA_SCHEMA..JOB_CNTL_PARMS b

			on a.da_run_id = b.da_run_id

			inner join
			(select * from &DA_SCHEMA..JOB_OTPT_META where fil_4th_node_txt = %nrbquote('&FIL_4TH_NODE.') ) c

			on a.da_run_id = c.da_run_id

	  ) by tmsis_passthrough;

	  %drop_tables(state_counts)

%mend create_efts_metadata;

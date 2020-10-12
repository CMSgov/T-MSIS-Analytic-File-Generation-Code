/**********************************************************************************************/
/*Program: annual_macros.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Macros to generate the DE TAF using the monthly BSF TAF tables
/*Mod: 
/**********************************************************************************************/
/* Macro max_run_id to get the highest da_run_id for the given state for each input monthly TAF (DE or claims). This
   table will then be merged back to the monthly TAF to pull all records for that state, month, and da_run_id.
   It is also inserted into the metadata table to keep a record of the state/month DA_RUN_IDs that make up 
   each annual run.
   To get the max run ID, must go to the job control table and get the latest national run, and then also
   get the latest state-specific run. Determine the later by state and month and then pull those IDs.
   Macro parms:
   	 inyear=input year, where the default will be set to the current year but it can be changed to all prior years,
   	        for when we need to read in demographic information from all prior years */
   	
%macro max_run_id(file=, tbl=, inyear=);

  %if &tbl. =  %then %let tbl=taf_&file.h;
  %if &inyear. = %then %let inyear=&year.;

  %if &file. ne BSF %then %let node=&file.H;
  %else %let node=BSF;

	** For NON state-specific runs (where job_parms_text does not include submtg_state_cd in),
	   pull highest da_run_id by time and state;

	execute (
		create temp table max_run_id_&file._&inyear._nat as
		select &file._fil_dt
               ,max(da_run_id) as da_run_id

		from (select substring(job_parms_txt,1,4)||substring(job_parms_txt,6,2) as &file._fil_dt
		             ,da_run_id

			from &DA_SCHEMA..job_cntl_parms
			where upper(substring(fil_type,2)) = %nrbquote('&file.') and 
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
			where upper(substring(fil_type,2)) = %nrbquote('&file.') and 
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
            from max_run_id_&file._&inyear._nat ) c

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
              from &DA_SCHEMA..efts_fil_meta where fil_4th_node_txt=%nrbquote('&node.') and incldd_state_cd != 'Missing' ) b

		on a.da_run_id = b.da_run_id

		%if %sysfunc(find(&ST_FILTER.,%str(ALL))) = 0 %then %do;
         	where &ST_FILTER.
	    %end;

		group by a.&file._fil_dt
		        ,b.submtg_state_cd

	) by tmsis_passthrough;


	/* Insert into metadata table so we keep track of all monthly DA_RUN_IDs (both BSF and claims)     
	   that go into each annual DE file */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_INP_SRC
		select
			&DA_RUN_ID. as ANN_DA_RUN_ID,
			SUBMTG_STATE_CD,
			&file._FIL_DT,
			DA_RUN_ID as SRC_DA_RUN_ID

		from max_run_id_&file._&inyear.
	) by tmsis_passthrough; 


%mend max_run_id;

/* Macro create_pyears to create a list of all prior years (from current year minus 1 to 2014).
   Note for 2014 the list will be empty. */

%macro create_pyears;

	%let pyears=;

	%do PY=2014 %to %eval(&YEAR.-1);

		%let pyears=&PY. &pyears.;

	%end;

%mend create_pyears;

/* Macro any_pyear to union all max run ID tables for all prior years, and if rec count is > 0, set
   get prior=1 (will get prior year records) */

%macro any_pyear;

  %if &pyears. ne  %then %do;

	   select anyprior into :getprior from 
		(select * from connection to tmsis_passthrough 
			(select case when count(submtg_state_cd) > 0 then 1
		            else 0 end as anyprior

			 from (%do p=1 %to %sysfunc(countw(&pyears.));
	 	             	%let pyear=%scan(&pyears.,&p.);
						%if &P. > 1 %then %do; union all %end; 
                         select * from max_run_id_bsf_&pyear. 

				  %end; )

		      ) 
		);

	%end;

	%else %let getprior=0;

%mend any_pyear;


/* Macro join_monthly to join the max da_run_ids for the given state/month back to the monthly TAF and
   then join each month by submtg_state_cd and msis_ident_num. Note this table will be pulled into for the subquery in the
   creation of each base and supplemental segment. */
   
   
%macro join_monthly;

  /* Create the backbone of unique state/msis_id to then left join each month back to */
  
   (select a.submtg_state_cd,
           b.msis_ident_num,
           count(a.submtg_state_cd) as nmonths
         
   from max_run_id_bsf_&inyear. a
        inner join
   	    &DA_SCHEMA..taf_mon_bsf b
	 
	 on a.submtg_state_cd = b.submtg_state_cd and
	    a.bsf_fil_dt = b.bsf_fil_dt and
	    a.da_run_id = b.da_run_id     
	    
	 group by a.submtg_state_cd,
              b.msis_ident_num) as enrl
          
   /* Now loop through each month to left join back to the backbone */
   
   	%do m=1 %to 12;
	
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
			
			left join 
			
		(select b.* from

		 max_run_id_bsf_&inyear. a
		 inner join
		 &DA_SCHEMA..taf_mon_bsf b
		 
		 on a.submtg_state_cd = b.submtg_state_cd and
		    a.bsf_fil_dt = b.bsf_fil_dt and
		    a.da_run_id = b.da_run_id
		    
		 where substring(a.bsf_fil_dt,5,2)=%nrbquote('&m.')) as m&m.
		 
		 on enrl.submtg_state_cd=m&m..submtg_state_cd and
		    enrl.msis_ident_num=m&m..msis_ident_num

		%end; /* end month loop */
	  

%mend join_monthly;

/* Macro create_temp_table to create each main table. For each table, there are columns we must get from the raw data in
   the subquery, and then columns we must get from the outer query that pulls from the subquery.
   Macro parms:
   	tblname=table name
   	inyear=input year, where the default will be set to the current year but it can be changed to the prior year,
   	        for when we need to read in demographic information from the prior year
   	subcols=creation statements for all columns that must be pulled from the raw data in the subquery
   	outercols=creation statements for all columns that must be pulled from the subquery
    subcols2 - subcols8=additional subcols when needing to loop over MC and waiver slots, because cannot
                        loop over all slots within one macro var or will exceed text limit of 65534 chars*/

%macro create_temp_table(tblname, inyear=, subcols=, outercols=, subcols2=, subcols3=, subcols4=, subcols5=,
                         subcols6=,subcols7=,subcols8=);

  %if &inyear.=  %then %let inyear=&year.;

	execute(
		create temp table &tblname._&inyear.
		distkey(msis_ident_num)
		sortkey(submtg_state_cd,msis_ident_num) as
		
			select * &outercols.
			
				from (
					select enrl.submtg_state_cd,
					       enrl.msis_ident_num
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
		         msis_ident_num       
	
	
	) by tmsis_passthrough;


%mend create_temp_table;

/* Macro create_segment, which will be called for each of the segments to run the respective
   create macro, which includes output to the permanent table. This macro will then get the count
   of records in that table, and output to the metadata table. (Note macro CREATE_META_INFO is in
   AWS_Shared_Macros) 
   Macro parms:
     -tblname=name of the output permanent table name (after TAF_ANN_DE_) 
     -FIL_4TH_NODE=abbreviated name of the table to go into the metadata table */

%macro create_segment(tblname, FIL_4TH_NODE);

	%create_&tblname.;

	sysecho 'in get cnt';
	%get_ann_count(&tblname.);

	sysecho 'create metainfo'; 
	%CREATE_META_INFO(&DA_SCHEMA., TAF_ANN_DE_&tblname., &DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);

	sysecho 'create EFT file metadata';
	%create_efts_metadata;

%mend create_segment;

/* Macro unique_claims_ids to join the max da_run_ids for the given claims file back to the monthly TAF and
   create a table of list of unique state/msis IDs with any claim.
   These lists will be unioned outside the macro */

%macro unique_claims_ids(cltype);

	select distinct b.submtg_state_cd
	                ,msis_ident_num

	from max_run_id_&cltype._&year. a
        inner join
   	    &DA_SCHEMA..taf_&cltype.h b

	on a.submtg_state_cd = b.submtg_state_cd and
	   a.&cltype._fil_dt = b.&cltype._fil_dt and
	   a.da_run_id = b.da_run_id

	where msis_ident_num is not null and
	      substring(msis_ident_num,1,1) != '&'

%mend unique_claims_ids;

/* Macro last_best to take the last best value (go backwards in time from month 12 to month 1, taking the first non-missing/null value).
   Macro parms:
     incol=input monthly column
     outcol=name of column to be output, where default is the same name of the incol 
     prior=indicator to compare current year against prior years (for demographics) to take prior if current year is missing, where default=0 */

%macro last_best(incol, outcol=, prior=0);

	%if &outcol.=  %then %let outcol = &incol.;
	
	%if &prior.=0 %then %do;
	
		,coalesce(m12.&incol., m11.&incol., m10.&incol., m09.&incol., 
			        m08.&incol., m07.&incol., m06.&incol., m05.&incol.,
			        m04.&incol., m03.&incol., m02.&incol., m01.&incol.) 
			     
	%end; 
	
	%if &prior.=1 %then %do;
	
		,coalesce(c.&incol. %do p=1 %to %sysfunc(countw(&pyears.));
	 		                     %let pyear=%scan(&pyears.,&p.);
                                  ,p&p..&incol. 
                            %end; )
	
	%end;

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

/* Macro monthly_array_eldts to take the raw monthly columns and array into columns with _MO suffixes.
   Effective dates will be truncated to the first of the month if prior to the first, and end dates
   will be truncated to the end of the month if after.
   Macro parms:
      incol=input monthly column
      outcol=name of column to be output, where default is the name of the incol with _MO for each month appended as a suffix
      nslots=# of slots, default = 16 (# of slots of effective/end dates on the BSF) 
      truncfirst=indicator for whether date should be truncated to the first of the month (i.e. date being read in is an effective
                 date), where default = 1. Set to 0 for end dates (truncated to last day of the month) */
      
%macro monthly_array_eldts(incol, outcol=, nslots=16, truncfirst=1);

	%if &outcol.=  %then %let outcol = &incol.;

	%do s=1 %to &nslots.;
		%if &nslots.=1 %then %let snum= ;
		%else %let snum=&s.;

		%do m=1 %to 12;
			%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 

			/* Identify the last day of the month */

			%if &m. in (01 03 05 07 08 10 12)               %then %let lday=31;
			%if &m. in (09 04 06 11)                        %then %let lday=30;
			%if &m. = 02 and &year. in (2016 2020 2024)     %then %let lday=29;
			%else %if &m. = 02 %then %let lday=28;

			/* Truncate effective dates to the 1st of the month if prior to the first. Otherwise pull in the raw date. */

			%if &truncfirst.=1 %then %do;

				,case when m&m..&incol.&snum. is not null and 
                      date_cmp(m&m..&incol.&snum.,to_date(%nrbquote('01 &m. &year.'),'dd mm yyyy')) = -1
				      then to_date(%nrbquote('01 &m. &year.'),'dd mm yyyy')
					  else m&m..&incol.&snum.
					  end as &outcol.&snum._&m.

			%end;

			/* Truncate end dates to the last day of the month if after the last day of the month. Otherwise pull in the 
			   raw date */

			%if &truncfirst.=0 %then %do;

				,case when m&m..&incol.&snum. is not null and 
                           date_cmp(m&m..&incol.&snum.,to_date(%nrbquote('&lday. &m. &year.'),'dd mm yyyy')) = 1
				      then to_date(%nrbquote('&lday. &m. &year.'),'dd mm yyyy')
					  else m&m..&incol.&snum.
					  end as &outcol.&snum._&m.

			%end;

		%end;

	%end;
	
%mend monthly_array_eldts;

/* Macro ever_year to look across all monthly columns and create an indicator for whether ANY of the monthly
   columns meet the given condition. The default condition is = 1.
   Macro parms:
      incol=input monthly column
      condition=monthly condition to be evaulated, where default is = 1
      raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
          in an earlier subquery and will therefore have the _MO suffixes, where default = 1
      outcol=name of column to be output, where default is the name of the incol with _EVER appended as a suffix
      usenulls=indicator to determine whether to use the nullif function to compare both nulls AND another value,
               where default is = 0 
      nullcond=additional value to look for when usenulls=1 */
      
%macro ever_year(incol, condition=%nrstr(=1), raw=1, outcol=, usenulls=0, nullcond= );

	%if &outcol.=  %then %let outcol = &incol._EVR;
	
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

/* Macro address_flag looks at the values for HOME_month and MAIL_month and assigns a 1
   if MAIL_month ne 00, otherwise 0 if HOME_month ne 00, otherwise null */
   
%macro address_flag;
	
	,case when ELGBL_LINE_1_ADR_MAIL_MN != '00' and ELGBL_LINE_1_ADR_HOME_MN = '00' then 1
	     when ELGBL_LINE_1_ADR_HOME_MN != '00' then 0
	else null
	end as ELGBL_ADR_MAIL_FLAG
	     
	     
%mend address_flag;

/* Macro assign_nonmiss_month looks at the values for the monthly variables assigned in nonmiss_month,
   and pulls multiple variables for that month based on the assigned month from nonmiss_month. Note
   this can be based on 1 or 2 monthly assignments from nonmiss_month, where the first is evaluated and
   if a month is never assigned to that variable, the second will be evaluated. This happens for HOME and
   MAIL address. Note that nonmiss_month must be run in the subquery before assign_nonmiss_month is run in
   the outer query.
   Macro parms:
   	outcol=column to assign based on the month captured in nonmiss_month
   	monthval1=monthly value to evaluate captured in nonmiss_month
   	incol1=input column to assign if monthval1 is met
   	monthval2=optional monthly value to evaluate captured in nonmiss_month, IF monthval1=00 
   	incol2=optional input column to assign if monthval2 is met */
   
   
%macro assign_nonmiss_month(outcol, monthval1, incol1, monthval2=, incol2= );

	,case %do mb=1 %to 12;
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

/* Macro address_same_year to use yearpull to pull in the address information
   from the same year in which ELGBL_LINE_1_ADR was pulled 
   Macro parms:
	  incol = input col to pull */

%macro address_same_year(incol);

	,case when yearpull = &year. then c.&incol.
		  %do p=1 %to %sysfunc(countw(&pyears.));
	 		   %let pyear=%scan(&pyears.,&p.);
			   when yearpull = &pyear. then p&p..&incol.
		  %end;
		  else null
		  end as &incol.

%mend address_same_year;

/* Macro mc_type_rank to look across all MC types for each month and assign one type for
   the month based on the priority ranking. For each month, must loop through each value
   in priority order and within each value, must loop through each slot.
   Macro parms:
   	smonth=the month to begin looping over, where default=1.
    emonth=the month to end looping over, where default=12.*/
   
%macro mc_type_rank(smonth=,emonth=);

	%let priorities=01 04 05 06 15 07 14 17 08 09 10 11 12 13 18 16 02 03 60 70 80 99;
	
	%do m=&smonth. %to &emonth.;
	
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));
		
		,case %do p=1 %to %sysfunc(countw(&priorities.));
		      	%let value=%scan(&priorities.,&p.); 
		      	  when (
		      	    %do s=1 %to &nmcslots.;
		      	  	    m&m..MC_PLAN_TYPE_CD&s.=%nrbquote('&value.') %if &s. < &nmcslots. %then %do; or %end; 
		      	  	%end; 
		      	  ) then %nrbquote('&value.')
		       %end;
		       
		  else null
		  end as MC_PLAN_TYPE_CD_&m.
		
	%end;


%mend mc_type_rank;

/* Macro mc_waiv_slots to look across all MC or waiver slots for the month and create an indicator for months with specific
   values of type.
   Macro parms:
   	incol=input type column to evaluate
    values=list of values (waiver or MC types) to look for
    outcol=output column with indicator for specific type 
    smonth=the month to begin looping over, where default=1
    emonth=the month to end looping over, where default=12 */

%macro mc_waiv_slots(incol,values,outcol,smonth=1,emonth=12);

	%if &incol.=MC_PLAN_TYPE_CD %then %let nslots=&nmcslots.;
	%if &incol.=WVR_TYPE_CD %then %let nslots=&nwaivslots.;

	%do m=&smonth. %to &emonth.;
	
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));

		,case when %do s=1 %to &nslots.;

						%if &s. > 1 %then %do; or %end; m&m..&incol.&s. in (&values.)

					%end;

		then 1 
		else 0
		end as &outcol._&m.

	%end;

%mend mc_waiv_slots;

/* Macro run_mc_slots to run the above mc_waiv_slots macro for all the MC types.
   Macro parms:
    smonth=the month to begin looping over, where default=1
    emonth=the month to end looping over, where default=12 */

%macro run_mc_slots(smonth,emonth);

	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('01'), CMPRHNSV_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('02'), TRDTNL_PCCM_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('03'), ENHNCD_PCCM_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('04'), HIO_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('05'), PIHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('06'), PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('07'), LTC_PIHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('08'), MH_PIHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)

	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('09'), MH_PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('10'), SUD_PIHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('11'), SUD_PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('12'), MH_SUD_PIHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('13'), MH_SUD_PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('14'), DNTL_PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('15'), TRANSPRTN_PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('16'), DEASE_MGMT_MC_PLAN,smonth=&smonth.,emonth=&emonth.)

	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('17'), PACE_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('18'), PHRMCY_PAHP_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('60'), ACNTBL_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('70'), HM_HOME_MC_PLAN,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(MC_PLAN_TYPE_CD, %nrstr('80'), IC_DUALS_MC_PLAN,smonth=&smonth.,emonth=&emonth.)


%mend run_mc_slots;

/* Macro run_waiv_slots to run the above mc_waiv_slots macro for all the Waiver types.
   Macro parms:
    smonth=the month to begin looping over, where default=1
    emonth=the month to end looping over, where default=12 */

%macro run_waiv_slots(smonth,emonth);

	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('22'), _1115_PHRMCY_PLUS_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('23'), _1115_DSTR_REL_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('24'), _1115_FP_ONLY_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, 
        %nrstr('06','07','08','09','10','11','12','13','14','15','16','17','18','19','33'), _1915C_WVR,smonth=&smonth.,emonth=&emonth.)

	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('20'), _1915BC_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('02','03','04','05','32'), _1915B_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('01'), _1115_OTHR_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('21'), _1115_HIFA_WVR,smonth=&smonth.,emonth=&emonth.)
	%mc_waiv_slots(WVR_TYPE_CD, %nrstr('25','26','27','28','29','30','31'), _OTHR_WVR,smonth=&smonth.,emonth=&emonth.)

%mend run_waiv_slots;

/* Macro mc_nonnull_zero to look across all MC IDs AND types slots and create an indicator
   if there is any non-null/00 value for type OR any non-null/0-, 8- or 9-only value for ID (for the _SPLMTL flags) 
   Macro parms:
   	 outcol=name of outcol (supp flag, will have suffix of smonth_emonth and then all must be combined to get
            yearly value)
     smonth=start month to loop over
     endmonth=end month to loop over */

%macro mc_nonnull_zero(outcol,smonth,emonth);

	,case when %do m=&smonth. %to &emonth.;
					%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));
						%do s=1 %to &nmcslots.;

						/* Non-null/00 plan type */

			    		(nullif(nullif(trim(m&m..MC_PLAN_TYPE_CD&s.),''),'00') is not null or 

						/* OR non-0, 8, 9 only or non-null ID */

						(nullif(trim(m&m..MC_PLAN_ID&s.),'') is not null and 

                        trim(m&m..MC_PLAN_ID&s.) not in ('0','00','000','0000','00000','000000','0000000',
						                                 '00000000','000000000','0000000000','00000000000','000000000000',
														 '8','88','888','8888','88888','888888','8888888',
						                                 '88888888','888888888','8888888888','88888888888','888888888888',
														 '9','99','999','9999','99999','999999','9999999',
						                                 '99999999','999999999','9999999999','99999999999','999999999999')) )

                           %if &m.<&emonth. or &s.<&nmcslots. %then %do; or %end;

					 %end;
				%end;
		then 1
		else 0
		end as &outcol._&smonth._&emonth.

%mend mc_nonnull_zero;

/* Macro waiv_nonnull to look across all waiver IDs AND types slots and create an indicator
   if there is any non-null value (for the _SPLMTL flags) 
   Macro parms:
   	 outcol=name of outcol (supp flag) */

%macro waiv_nonnull(outcol);

	,case when %do m=1 %to 12;
					%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));
						%do s=1 %to &nwaivslots.;

			    		m&m..WVR_ID&s. is not null or 
                        m&m..WVR_TYPE_CD&s. is not null %if &m.<12 or &s.<&nwaivslots. %then %do; or %end;

					 %end;
				%end;
		then 1
		else 0
		end as &outcol.


%mend waiv_nonnull;

/* Macro misg_enrlmt_type to create indicators for ENRL_TYPE_FLAG = NULL. 
   Set to 1 if ENRL_TYPE_FLAG = NULL AND the person is in the month. 
   Set to 0 if ENRL_TYPE_FLAG != NULL AND person in the month. 
   Set to NULL if the person is not in the month. */

%macro misg_enrlmt_type;
									 
	%do m=1 %to 12;
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));

		,case when m&m..ENRL_TYPE_FLAG is null and m&m..msis_ident_num is not null
	          then 1
	          when m&m..msis_ident_num is not null
	          then 0
	          else null
	          end as MISG_ENRLMT_TYPE_IND_&m.
									 	    
	%end;

%mend misg_enrlmt_type;

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
   statement (DA_RUN_ID, DE_LINK_KEY, DE_FIL_DT, ANN_DE_VRSN, SUBMTG_STATE_CD, MSIS_IDENT_NUM)
   Macro parms:
	suffix= optional param which will be set to _COMB to pull in the combined state and MSIS_ID cols for the Base segment*/

%macro table_id_cols(suffix=);

	 &DA_RUN_ID. as DA_RUN_ID
	 ,cast ((%nrbquote('&DA_RUN_ID.') || '-' || %nrbquote('&YEAR.') || '-' || %nrbquote('&VERSION.') || '-' ||
	         SUBMTG_STATE_CD&suffix. || '-' || MSIS_IDENT_NUM&suffix.) as varchar(40)) as DE_LINK_KEY
	 ,%nrbquote('&YEAR.') as DE_FIL_DT
	 ,%nrbquote('&VERSION.') as ANN_DE_VRSN
	 ,SUBMTG_STATE_CD&suffix.
	 ,MSIS_IDENT_NUM&suffix.

%mend table_id_cols;

/* Macro get_ann_cnt to get the count of the given table and put the count into a macro var 
   Macro parms:
	tblname=perm table name */

%macro get_ann_count(tblname);

	 create table record_count as
	 select * from connection to tmsis_passthrough
	 ( select count(submtg_state_cd) as row_cnt
		 from &DA_SCHEMA..TAF_ANN_DE_&tblname.
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

		 from &DA_SCHEMA..TAF_ANN_DE_&tblname.
		 where da_run_id = &da_run_id. 
		 group by da_run_id,
		          submtg_state_cd

	  ) by tmsis_passthrough;

	  /* Insert the following values into the table:
	     - DA_RUN_ID
	     - 4th node text value
	     - table name
	     - year
	     - version number (01)
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
			   ,%nrbquote('TAF_ANN_DE_&tblname.') as otpt_name
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

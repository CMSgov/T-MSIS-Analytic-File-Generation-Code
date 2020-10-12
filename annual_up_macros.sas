/**********************************************************************************************/
/*Program: annual_up_macros.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Macros to generate the UP TAF using the DE and monthly claims
/*Mod: 
/**********************************************************************************************/

/* Macro max_run_id to get the highest da_run_id for the given state for each input monthly TAF (DE or claims). This
   table will then be merged back to the monthly TAF to pull all records for that state, month, and da_run_id.
   It is also inserted into the metadata table to keep a record of the state/month DA_RUN_IDs that make up 
   each annual run.
   To get the max run ID, must go to the job control table and get the latest national run, and then also
   get the latest state-specific run. Determine the later by state and month and then pull those IDs.
   Macro parms:
   	 inyear=input year, where the default will be set to the current year but it can be changed to prior/following year
   	        for when we need to read in IP prior/following year records - NOTE for these, we will only pull in prior 3 and
            following 3 months */

%macro max_run_id(file=, tbl=, inyear=&year.);

  %if &tbl. =  %then %let tbl=taf_&file.h;

  %if &file. ne DE %then %let node=&file.H;
  %else %let node=BSE;

	** For NON state-specific runs (where job_parms_text does not include submtg_state_cd in),
	   pull highest da_run_id by time ;

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
				  %if &inyear. = &pyear. %then %do;
					 substring(job_parms_txt,6,2) in ('10','11','12') and
				  %end;

				  %if &inyear. = &fyear. %then %do;
					 substring(job_parms_txt,6,2) in ('01','02','03') and
				  %end;
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
				  %if &inyear. = &pyear. %then %do;
					 substring(job_parms_txt,6,2) in ('10','11','12') and
				  %end;

				  %if &inyear. = &fyear. %then %do;
					 substring(job_parms_txt,6,2) in ('01','02','03') and
				  %end;
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
	   were identified as being later than a national run).
	   Note for DE only, strip off month from fil_dt;

	execute (
		create temp table max_run_id_&file._&inyear. as 
		select %if &file. = DE	%then %do; substring(a.&file._fil_dt,1,4) as &file._fil_dt %end;
			   %else %do; a.&file._fil_dt %end;
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


	/* Insert into metadata table so we keep track of all monthly DA_RUN_IDs (both DE and claims)     
	   that go into each annual UP file */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_UP_INP_SRC
		select
			&DA_RUN_ID. as ANN_DA_RUN_ID,
			SUBMTG_STATE_CD,
			&file._FIL_DT,
			DA_RUN_ID as SRC_DA_RUN_ID

		from max_run_id_&file._&inyear.
	) by tmsis_passthrough; 


%mend max_run_id;
  

/* Macro to determine if year is leap year - must be divisible by 4 and NOT divisible by 100, unless also divisible by 400.
   It will create macro vars &leap. ( = 1 if leap year, 0 if not) and &totdays ( = 366 if leap year, 365 if not) */

%macro leapyear(inyear);

	%if %sysfunc(mod(&inyear.,4)) = 0 and (%sysfunc(mod(&inyear.,100)) > 0 or %sysfunc(mod(&inyear.,400)) = 0)
	    %then %let leap=1;

	%else %let leap=0;

	%if &leap.=1 %then %let totdays=366;
	%else %let totdays=365;

%mend leapyear;

/* Macro pullclaims to pull in all the monthly claims files, subsetting to only desired claim types, non-null MSIS ID, and
   MSIS ID not beginning with '&'. Keep only needed cols.
   Also create MDCD, SCHIP, NON_XOVR and XOVR indicators to more easily pull in these four combinations of records when
   counting/summing.
   For NON_XOVR/XOVR, if the input xovr_ind is null, look to tot_mdcr_ddctbl_amt and tot_mdcr_coinsrnc_amt - if EITHER is > 0,
   set OXVR=1.
	Macro parms:
	     file=file to pull from (IP/LT/OT/RX)
         hcols=cols to keep from header (which are not universal in all four claims files)
         lcols=cols to keep from line (which are not universal in all four claims files)
         inyear=year to pull (default is current year) */

%macro pullclaims(file, hcols=, lcols=, inyear=&year.);

	execute (
		create temp table &file.h_&inyear.
        distkey(&file._link_key)
        sortkey(&file._link_key) as 
		select a.da_run_id
               ,a.submtg_state_cd
		       ,msis_ident_num
			   ,&file._link_key
			   ,clm_type_cd
			   ,tot_mdcd_pd_amt
			   ,sect_1115a_demo_ind
			   ,xovr_ind
			   ,pgm_type_cd

			   ,case when clm_type_cd in ('1','2','3','5')
			        then 1 else 0
					end as MDCD

			   ,case when MDCD=0
			        then 1 else 0
					end as SCHIP

			   ,case when xovr_ind='1' or (xovr_ind is null and (tot_mdcr_ddctbl_amt>0 or tot_mdcr_coinsrnc_amt>0))
				     then 1 else 0
					 end as XOVR

				,case when XOVR=0
				     then 1 else 0
					 end as NON_XOVR

				,case when clm_type_cd in ('1','A')
				      then 1 else 0
					  end as FFS


			   %if &hcols. ne  %then %do i=1 %to %sysfunc(countw(&hcols.));
			   	  %let col=%scan(&hcols.,&i.);
				   ,&col.
				%end;

		from max_run_id_&file._&inyear. a
		     inner join
			 &DA_SCHEMA..taf_&file.h b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.&file._fil_dt = b.&file._fil_dt and
		   a.da_run_id = b.da_run_id

		where clm_type_cd in ('1','2','3','5','A','B','C','E') and
		      msis_ident_num is not null and
			  substring(msis_ident_num,1,1) != '&'


	) by tmsis_passthrough;


	** Join to line-level file to get needed cols - drop denied lines;

	execute (
		create temp table &file.l0_&inyear.
        distkey(&file._link_key)
        sortkey(&file._link_key) as 
		select a.da_run_id
               ,a.submtg_state_cd
			   ,b.&file._link_key
		       ,b.tos_cd
			   ,b.mdcd_pd_amt
			   ,b.mdcd_ffs_equiv_amt
			   %if "&file." = "IP" or "&file." = "LT" %then %do;
			   	  ,b.srvc_bgnng_dt as srvc_bgnng_dt_ln
				  ,b.srvc_endg_dt as srvc_endg_dt_ln
			   %end;
			   %if &lcols. ne  %then %do i=1 %to %sysfunc(countw(&lcols.));
			   	  %let col=%scan(&lcols.,&i.);
				   ,&col.
				%end;

		from max_run_id_&file._&inyear. a
		     inner join
			 &DA_SCHEMA..taf_&file.l b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.&file._fil_dt = b.&file._fil_dt and
		   a.da_run_id = b.da_run_id

		where cll_stus_cd not in ('026','26','087','87','542','585','654') or cll_stus_cd is null 


	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(max_run_id_&file._&inyear.)

	execute (
		create temp table &file.l_&inyear. 
		distkey(&file._link_key)
        sortkey(&file._link_key) as 
		select a.*
		       ,b.tos_cd
			   ,b.mdcd_pd_amt
			   ,b.mdcd_ffs_equiv_amt
			   %if "&file." = "IP" or "&file." = "LT" %then %do;
			   	  ,b.srvc_bgnng_dt_ln
				  ,b.srvc_endg_dt_ln
			   %end;
			   %if &lcols. ne  %then %do i=1 %to %sysfunc(countw(&lcols.));
			   	  %let col=%scan(&lcols.,&i.);
				   ,&col.
				%end;


		from &file.h_&inyear. a
		     left join
			 &file.l0_&inyear. b

		on a.&file._link_key = b.&file._link_key and
           a.da_run_id = b.da_run_id

	) by tmsis_passthrough;

	%drop_tables(&file.l0_&inyear.);

	** Drop IPH if not current year (only line-level files are used);

	%if &inyear. ne &year. %then %do;
		
		%drop_tables(&file.h_&inyear.);

	%end;

%mend pullclaims;

/* Macro getmax to get the max of the incol (which will be a 0/1 indicator)
   Macro parms:
	  incol=input col
      outcol=output col (optional) - if not specified default will be incol */

%macro getmax(incol=, outcol=);

	%if &outcol.=  %then %let outcol=&incol.;

	,max(&incol.) as &outcol.

%mend getmax;

/* Macro sumrecs to get the sum of the incol 
   Macro parms:
	  incol=input col
      outcol=output col (optional) - if not specified default will be incol */

%macro sumrecs(incol=, outcol=);

	%if &outcol.=  %then %let outcol=&incol.;

	,sum(&incol.) as &outcol.

%mend sumrecs;

/* Macro count_recs to count the number of recs where the given column equals the given condition
   Macro parms:
	  condcol1=input col to condition on
      cond1=condition to subset condcol on (optional) - if not specified default will be = 1 
      condcol2=second input col to condition on (optional)
      cond2=condition to subset second condcol on (optional) - if not specified default will be = 1 
      condcol3=third input col to condition on (optional)
      cond3=condition to subset third condcol on (optional) - if not specified default will be = 1 
      condcol4=fourth input col to condition on (optional)
      cond4=condition to subset fourth condcol on (optional) - if not specified default will be = 1 
	  outcol=output col which will contain the count of recs */

%macro count_rec(condcol1=, cond1=%nrstr(=1), 
                  condcol2=, cond2=%nrstr(=1), 
                  condcol3=, cond3=%nrstr(=1), 
				  condcol4=, cond4=%nrstr(=1), 
                  outcol= );

	,sum(case when &condcol1. &cond1.
					%if &condcol2. ne  %then %do;
					   	and &condcol2. &cond2.
						%if &condcol3. ne  %then %do;
						     and &condcol3. &cond3.
						%end;
						%if &condcol4. ne  %then %do;
						     and &condcol4. &cond4.
						 %end;
				    %end;

         then 1 
         else 0 end) as &outcol.

%mend count_rec;

/* Macro any_rec to create an indicator for ANY rec where the given column equals the given condition
   Macro parms:
	  condcol1=input col to condition on
      cond1=condition to subset condcol on (optional) - if not specified default will be = 1 
      condcol2=second input col to condition on (optional)
      cond2=condition to subset second condcol on (optional) - if not specified default will be = 1 
      condcol3=third input col to condition on (optional)
      cond3=condition to subset third condcol on (optional) - if not specified default will be = 1 
      condcol4=fourth input col to condition on (optional)
      cond4=condition to subset fourth condcol on (optional) - if not specified default will be = 1 

	  outcol=output col which will contain the count of recs */

%macro any_rec(condcol1=, cond1=%nrstr(=1), 
                 condcol2=, cond2=%nrstr(=1), 
                 condcol3=, cond3=%nrstr(=1),
				 condcol4=, cond4=%nrstr(=1), 
                 outcol= );

	,max(case when &condcol1. &cond1.
					%if &condcol2. ne  %then %do;
					   	and &condcol2. &cond2.
						%if &condcol3. ne  %then %do;
						     and &condcol3. &cond3.
						%end;
						%if &condcol4. ne  %then %do;
						     and &condcol4. &cond4.
						 %end;
				    %end;

         then 1 
         else 0 end) as &outcol.

%mend any_rec;

/* Macro sum_paid to sum tot_mdcd_pd_amt on the headers OR mdcd_pd_amt on the lines where the given column equals the given condition
   Macro parms:
	  condcol1=input col to condition on
      cond1=condition to subset condcol on (optional) - if not specified default will be = 1 
      condcol2=second input col to condition on (optional)
      cond2=condition to subset second condcol on (optional) - if not specified default will be = 1 
      condcol3=third input col to condition on (optional)
      cond3=condition to subset third condcol on (optional) - if not specified default will be = 1 
      condcol4=fourth input col to condition on (optional)
      cond4=condition to subset fourth condcol on (optional) - if not specified default will be = 1 

      paidcol=payment col to sum (optional) - if not specified default will be tot_mdcd_pd_amt (header), otherwise
              can specify mdcd_pd_amt to sum line-level amounts

      outcol=output col which will contain the sum of paid amount (optional) - default is incol */

%macro sum_paid(condcol1=, cond1=%nrstr(=1), 
                condcol2=, cond2=%nrstr(=1), 
                condcol3=, cond3=%nrstr(=1), 
				condcol4=, cond4=%nrstr(=1), 
				paidcol=tot_mdcd_pd_amt,
                outcol= );

	%if &outcol.=  %then %let outcol=&incol.;

	,sum(case when &condcol1. &cond1. 
					%if &condcol2. ne  %then %do;
					   	and &condcol2. &cond2.
						%if &condcol3. ne  %then %do;
						     and &condcol3. &cond3.
						%end;
						%if &condcol4. ne  %then %do;
						     and &condcol4. &cond4.
						 %end;
				    %end;

				then &paidcol. 
                else null end) as &outcol.

%mend sum_paid;


%macro assign_toc;

	%if &ind1.=MDCD %then %do;
	  	 %let ffsval=1;
		 %let capval=2;
		 %let mcval=3;
		 %let suppval=5;
  	%end;

	%if &ind1.=SCHIP %then %do;
	 	 %let ffsval=A;
		 %let capval=B;
		 %let mcval=C;
		 %let suppval=E;
	%end;

%mend assign_toc;

/* Macro union_base_hdr: select statement to union the four file types, called once per file type,
   from the header-level rollup (created in 001_up_base_hdr) 
	Macro parms:
	     file=file to pull from (IP/LT/OT/RX) */

%macro union_base_hdr(file);

	select 

		submtg_state_cd
	    ,msis_ident_num
	    ,sect_1115a_demo_ind_any
		,ANY_FFS
		,ANY_MC

		%do i=1 %to 2;
		  %let ind1=%scan(&INDS1.,&i.);
		  %do j=1 %to 2;
		   	 %let ind2=%scan(&INDS2.,&j.);

			,&ind1._rcpnt_&ind2._FFS_FLAG
		    ,&ind1._rcpnt_&ind2._MC_FLAG
		    ,TOT_&ind1._&ind2._PD

			%if &ind2.=NON_XOVR %then %do;

				,&ind1._&ind2._SPLMTL_CLM
				,TOT_&ind1._&ind2._SPLMTL_PD

			%end;

		  %end;

		%end;

		/* Loop over the four file types - if the file type is NOT the given type,
		   set element to 0/null, otherwise pull in actual column */

		%do f=1 %to 4;
			 %let file2=%scan(&FLTYPES.,&f.);

			 %if &file2. ne RX %then %do;

				 %if "&file." ne "&file2." %then %do;

				 	,0 as &file2._mh_dx_ind_any
					,0 as &file2._sud_dx_ind_any
					,0 as &file2._mh_txnmy_ind_any
					,0 as &file2._sud_txnmy_ind_any

					,0 as &file2._ffs_mh_clm
					,0 as &file2._mc_mh_clm
					,0 as &file2._ffs_sud_clm
					,0 as &file2._mc_sud_clm

					,null as &file2._ffs_mh_pd
					,null as &file2._ffs_sud_pd

				 %end; 

				 %else %if "&file." = "&file2." %then %do;

				  	,&file2._mh_dx_ind_any
					,&file2._sud_dx_ind_any
					,&file2._mh_txnmy_ind_any
					,&file2._sud_txnmy_ind_any

					,&file2._ffs_mh_clm
					,&file2._mc_mh_clm
					,&file2._ffs_sud_clm
					,&file2._mc_sud_clm

					,&file2._ffs_mh_pd
					,&file2._ffs_sud_pd

				 %end; 

			%end; /* end file2 ne RX loop */

			/* Now must loop over MDCD/SCHIP NON_XOVR/XOVR */

			%do i=1 %to 2;
				%let ind1=%scan(&INDS1.,&i.);
				%do j=1 %to 2;
				  	 %let ind2=%scan(&INDS2.,&j.);

					 %if "&file." ne "&file2." %then %do;

					 	 ,null as TOT_&ind1._&ind2._FFS_&file2._PD

					 %end; 

					 %if "&file." = "&file2." %then %do;

					 	 ,TOT_&ind1._&ind2._FFS_&file2._PD

					 %end;

					 /* Only count claims for OT and RX - IP and LT will be counted when rolling up to
				        visits/days */

					  %if &file2. = OT or &file2. = RX %then %do;

					  	  %if "&file." ne "&file2." %then %do;

							 ,0 as &ind1._&ind2._FFS_&file2._CLM
							 ,0 as &ind1._&ind2._MC_&file2._CLM

						   %end;

						   %else %if "&file." = "&file2." %then %do;

							 ,&ind1._&ind2._FFS_&file2._CLM
							 ,&ind1._&ind2._MC_&file2._CLM

						   %end; 

					  %end; /* end file2 = OT or RX loop */

				%end; 

			%end; 

		%end; /* end file2=1 to 4 loop */

	from &file.h_bene_base_&year.

%mend union_base_hdr;

/* Macro commoncols_base_line: list of columns to be read in from all four file types from line-level
   rollup to header (created in 003_up_base_line) when unioning all four file types */

%macro commoncols_base_line;

	submtg_state_cd
    ,msis_ident_num
	%do h=1 %to 7;
		,hcbs_&h._clm_flag
	%end;

	/* For four combinations of claims (MDCD non-xover, SCHIP non-xover, MDCD xovr and SCHIP xovr,
       get the same counts and totals. Loop over INDS1 (MDCD SCHIP) and INDS2 (NON_XOVR XOVR) to assign
	   the four pairs of records */

	%do i=1 %to 2;
	  %let ind1=%scan(&INDS1.,&i.);
	  %do j=1 %to 2;
	   	 %let ind2=%scan(&INDS2.,&j.);

		 ,&ind1._&ind2._ANY_MC_CMPRHNSV
		 ,&ind1._&ind2._ANY_MC_PCCM
		 ,&ind1._&ind2._ANY_MC_PVT_INS
		 ,&ind1._&ind2._ANY_MC_PHP
		 ,&ind1._&ind2._PD
		 ,&ind1._&ind2._FFS_EQUIV_AMT
		 ,&ind1._&ind2._MC_CMPRHNSV_PD
		 ,&ind1._&ind2._MC_PCCM_PD
		 ,&ind1._&ind2._MC_PVT_INS_PD
		 ,&ind1._&ind2._MC_PHP_PD

	  %end;
	%end;

%mend commoncols_base_line;

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
   statement (DA_RUN_ID, DE_LINK_KEY, DE_FIL_DT, ANN_DE_VRSN, SUBMTG_STATE_CD, MSIS_IDENT_NUM) */

%macro table_id_cols;

	 &DA_RUN_ID. as DA_RUN_ID
	 ,cast ((%nrbquote('&DA_RUN_ID.') || '-' || %nrbquote('&YEAR.') || '-' || %nrbquote('&VERSION.') || '-' ||
	         SUBMTG_STATE_CD || '-' || MSIS_IDENT_NUM) as varchar(40)) as UP_LINK_KEY
	 ,%nrbquote('&YEAR.') as UP_FIL_DT
	 ,%nrbquote('&VERSION.') as ANN_UP_VRSN
	 ,SUBMTG_STATE_CD
	 ,MSIS_IDENT_NUM

%mend table_id_cols;

/* Macro create_efts_metadata to get the count of the given table overall and by state and insert 
   into the EFT metadata table.
   Also call macro CREATE_META_INFO (in AWS_Shared_Macro.sas) to insert just total counts into separate
   metadata table.
   Macro parms:
	tblname=perm table name (following TAF_ANN_UP_) 
    FIL_4TH_NODE=abbreviated name of the table to go into the metadata table */

%macro create_efts_metadata(tblname,FIL_4TH_NODE);

	/* Get total count */

	 create table record_count as
	 select * from connection to tmsis_passthrough
	 ( select count(submtg_state_cd) as row_cnt

		 from &DA_SCHEMA..TAF_ANN_UP_&tblname.
		 where da_run_id = &da_run_id. 
	  );

	  select row_cnt 
      into :rowcount
	  from record_count;


	/* Create state counts and insert into metadata table */

	 execute (
	 	create temp table state_counts as
		select da_run_id,
               submtg_state_cd,
		       count(submtg_state_cd) as rowcount_state

		 from &DA_SCHEMA..TAF_ANN_UP_&tblname.
		 where da_run_id = &da_run_id. 
		 group by da_run_id,
                  submtg_state_cd

	  ) by tmsis_passthrough;

	  %CREATE_META_INFO(&DA_SCHEMA., TAF_ANN_UP_&tblname., &DA_RUN_ID., &ROWCOUNT., &FIL_4TH_NODE.);

	  /* Insert the following values into the table:
	     - DA_RUN_ID
	     - 4th node text value
	     - table name
	     - year
	     - version number
	     - overall table count
	     - run date
	     - state code
	     - table count for given state */

	  execute (
	  	insert into &DA_SCHEMA..EFTS_FIL_META
		(da_run_id,fil_4th_node_txt,otpt_name,rptg_prd,itrtn_num,tot_rec_cnt,fil_cret_dt,incldd_state_cd,rec_cnt_by_state_cd,fil_dt,taf_cd_spec_vrsn_name)

		select a.da_run_id
		       ,%nrbquote('&FIL_4TH_NODE.') as fil_4th_node_txt
			   ,%nrbquote('TAF_ANN_UP_&tblname.') as otpt_name
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

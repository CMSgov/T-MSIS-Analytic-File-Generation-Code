/**********************************************************************************************/
/*Program: 006_up_base_ip.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Read in the IP line-level file, aggregate to header, and rollup to the stay-level
/*         to count number of stays and total unique  days
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro base_ip_stays;

	** Stack all three years (prior year with three months, current year, and following year with
       three months). Create claim_provider to be able to roll up to stay-level.
       Keep FFS and encounters only;

	execute (
		create temp table ipl_3yr 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select *
		       ,coalesce(blg_prvdr_num,blg_prvdr_npi_num) as claim_provider

		from (
			select * from ipl_&pyear.

			union
			select * from ipl_&year.

			union
			select * from ipl_&fyear.
			)

		where clm_type_cd in ('1','A','3','C')
	) by tmsis_passthrough;

	** Drop line-level tables (no longer needed);

	%drop_tables(ipl_&pyear. ipl_&fyear.);

	** Roll up to the header-level, creating claim admission and discharge date (using
	   line-level values if header is missing), and identifying claims to keep based on TOS_CD -
	   Identify claims where there is at least one claim with specific TOS_CD values we want included in the stay counts,
	   OR all TOS_CD values are null - we will keep all claims with either of these scenarios.
	   Drop claims with null claim provider;

	execute (
		create temp table ipl_3yr_hdr 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select *

		from ( 
		   select *
			    ,case when admsn_dt is not null then admsn_dt
				     when srvc_bgnng_dt_ln_min is not null then srvc_bgnng_dt_ln_min
					 else null
					 end as claim_admsn_dt

				,case when dschrg_dt is not null then dschrg_dt
			          when srvc_endg_dt_ln_max is not null then srvc_endg_dt_ln_max
					  else null
					  end as claim_dschrg_dt

				/* Create claim_tos_keep (keep claim based on TOS_CD values) and invalid_dates (will
				   drop based on dates if any of the following:
					   - admission OR discharge null
					   - discharge before admission
					   - > 1.5 years between admission and discharge ) */

			    ,case when any_tos_keep=1 or tos_cd_null=1
				        then 1 else 0
						end as claim_tos_keep

				,case when claim_admsn_dt is null or 
                           claim_dschrg_dt is null or
						   claim_dschrg_dt < claim_admsn_dt or
						   (claim_dschrg_dt - claim_admsn_dt + 1) > (365.25 * 1.5)
						then 1 else 0
						end as invalid_dates

		 from (
		 	select submtg_state_cd
			       ,msis_ident_num
				   ,ip_link_key
				   ,claim_provider
				   ,ptnt_stus_cd
				   ,FFS
				   ,MDCD
				   ,XOVR
				   ,admsn_dt
				   ,dschrg_dt

				   ,min(srvc_bgnng_dt_ln) as srvc_bgnng_dt_ln_min
				   ,max(srvc_endg_dt_ln) as srvc_endg_dt_ln_max

				   ,max(case when TOS_CD in ('001','060','084','086','090','091','092','093')
							 then 1 else 0 end) 
	                         as any_tos_keep

				   ,case when max(TOS_CD) is null
				         then 1 else 0
						 end as tos_cd_null

			from ipl_3yr
			where claim_provider is not null
			group by submtg_state_cd
			         ,msis_ident_num
				     ,ip_link_key
				     ,claim_provider
				     ,ptnt_stus_cd
				     ,FFS
				     ,MDCD
				     ,XOVR
				     ,admsn_dt
				     ,dschrg_dt

			) 
		)

		where claim_tos_keep=1 and invalid_dates=0

	) by tmsis_passthrough;

	** Now need to identify, for a given set of claims by discharge date only, whether there is ANY claim with that discharge date
	   that has an associated patient status code != 30 and != null. This is because of the rules for rolling up based on patient status code:
	   If there are two claims that fall into the category where we have a claim with a discharge date equal to or one day before the admission
	   date of the next stay, but one has a patient status code = 30 or null and one has a code indicating discharge, the code indicating discharge  will take precedence;

	execute (
		create temp table ip_for_stays_unq_dschrg 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
			   ,msis_ident_num
			   ,claim_provider
			   ,FFS
			   ,MDCD
			   ,XOVR
			   ,claim_dschrg_dt
			   ,max(case when ptnt_stus_cd != 30 and ptnt_stus_cd is not null
			        then 1 else 0
					end) as stus_cd_dschrg

		from ipl_3yr_hdr
		group by submtg_state_cd
			     ,msis_ident_num
			     ,claim_provider
			     ,FFS
			     ,MDCD
			     ,XOVR
			     ,claim_dschrg_dt

	) by tmsis_passthrough;

	** Create a table of distinct claim admission/discharge dates for all records,
	   joining back to the above table to add stus_cd_dschrg;

	execute (
		create temp table ip_for_stays_unq 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select distinct a.submtg_state_cd
				        ,a.msis_ident_num
					    ,a.claim_provider
					    ,a.FFS
					    ,a.MDCD
					    ,a.XOVR
						,a.claim_admsn_dt
						,a.claim_dschrg_dt
						,b.stus_cd_dschrg

		from ipl_3yr_hdr a

			  left join
			  ip_for_stays_unq_dschrg b

			on a.submtg_state_cd = b.submtg_state_cd and
		       a.msis_ident_num = b.msis_ident_num and
			   a.claim_provider = b.claim_provider and
			   a.FFS = b.FFS and
			   a.MDCD = b.MDCD and
			   a.XOVR = b.XOVR and
			   a.claim_dschrg_dt = b.claim_dschrg_dt

	) by tmsis_passthrough;

	** Create a unique date ID to filter on later when identifying stays completely contained within another;

	execute (
	  	 create temp table ip_for_stays_unq_ids 
		 distkey(msis_ident_num)
         sortkey(submtg_state_cd,msis_ident_num) as

		 select *
		 		,trim(submtg_state_cd||'-'||msis_ident_num||'-'||claim_provider||'-'||FFS||'-'||MDCD||'-'||XOVR||'-'||
                      cast(row_number() over 

		              (partition by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR 
		               order by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR, claim_admsn_dt, claim_dschrg_dt) 
			           as char(3))) as dateId

		 from ip_for_stays_unq

	) by tmsis_passthrough;


	** Create a table of claims by joining every stay to every other stay within the same group, and
	    identifying overlaps where one stay is completely contained within another ;

	execute (
	    create temp table ip_for_stays_overlaps 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		 select a.*
		 from ip_for_stays_unq_ids a 
			  inner join 
		      ip_for_stays_unq_ids b

		 /* Join records to each other, but omit matches where same record */

		 on a.submtg_state_cd = b.submtg_state_cd and
		    a.msis_ident_num = b.msis_ident_num and
		    a.claim_provider = b.claim_provider and
			a.FFS = b.FFS and
			a.MDCD = b.MDCD and
			a.XOVR = b.XOVR and
			a.dateId <> b.dateId

		 /* Get every dateID where admission date is greater than or equal to another record's admission date
			AND discharge date is less than or equal to that other record's discharge date. These are records contained
			within another stay, which will be excluded from rollup */

		 where date_cmp(a.claim_admsn_dt,b.claim_admsn_dt) in (0,1) and
		       date_cmp(a.claim_dschrg_dt,b.claim_dschrg_dt) in (0,-1)

	) by tmsis_passthrough;

	** Now create a table of non-overlap records which will be used for rollup ;

	execute (
		create temp table ip_for_stays_nonoverlap 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

	     select a.*

		 from ip_for_stays_unq_ids a

			   left join 
		       ip_for_stays_overlaps b

		 on a.dateid = b.dateid

		 where b.dateid is null     

	) by tmsis_passthrough;

	execute (
	     create temp table ip_stays_out 
		 distkey(msis_ident_num)
         sortkey(submtg_state_cd,msis_ident_num) as

		 select submtg_state_cd
	           ,msis_ident_num
			   ,claim_provider
			   ,FFS
			   ,MDCD
			   ,XOVR
			   ,g
		       ,min(claim_admsn_dt) as stay_admsn_dt
			   ,max(claim_dschrg_dt) as stay_dschrg_dt

		 from
		    (
		   	    select submtg_state_cd
			           ,msis_ident_num
					   ,claim_provider
					   ,FFS
					   ,MDCD
					   ,XOVR
					   ,claim_admsn_dt
					   ,claim_dschrg_dt
					   ,sum(C) over (partition by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR
							         order by claim_admsn_dt, claim_dschrg_dt
				                     rows UNBOUNDED PRECEDING) as G
			 from
				 (
				 select submtg_state_cd
				       ,msis_ident_num
					   ,claim_provider
					   ,FFS
					   ,MDCD
					   ,XOVR
					   ,claim_admsn_dt
					   ,claim_dschrg_dt
					   ,claim_admsn_dt_lag
					   ,claim_dschrg_dt_lag

					   /* Determine when to create a second stay - if the difference between current admission date and prior
					      discharge date is more than 1 day, OR the difference is 0 or 1 day (either same or contiguous days)
					      AND the prior patient status code indicates discharge, then create a second stay */
					      

					   ,case when claim_admsn_dt - coalesce(claim_dschrg_dt_lag,claim_admsn_dt) > 1 or
					              (claim_admsn_dt - coalesce(claim_dschrg_dt_lag,claim_admsn_dt) in (0,1) and
								   (stus_cd_dschrg_lag=1))
					         then 1 else 0
							 end as C
				 from

				     (select submtg_state_cd
					       ,msis_ident_num
						   ,claim_provider
						   ,FFS
						   ,MDCD
						   ,XOVR
						   ,claim_admsn_dt
						   ,claim_dschrg_dt
						   ,lag(claim_admsn_dt) over (partition by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR
						                                     order by claim_admsn_dt, claim_dschrg_dt) 
				                                             as claim_admsn_dt_lag
						   ,lag(claim_dschrg_dt) over (partition by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR
						                                     order by claim_admsn_dt, claim_dschrg_dt)     
				                                             as claim_dschrg_dt_lag
						   ,lag(stus_cd_dschrg) over (partition by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR
						                                     order by claim_admsn_dt, claim_dschrg_dt)     
				                                             as stus_cd_dschrg_lag

					 from ip_for_stays_nonoverlap
					      order by claim_admsn_dt, claim_dschrg_dt) s1 ) s2 ) s3 

			  group by submtg_state_cd, msis_ident_num, claim_provider, FFS, MDCD, XOVR, g
	     
		 ) by tmsis_passthrough;

	** Now take the above stays and by MDCD/SCHIP, XOVR/NON_XOVR, and FFS/MC, subset to stay records to keep for the 
	   current year:  
	      - stays with a discharge date in the current year
	      - stays with no more than 1.5 years difference between stay admission and stay discharge date

	    Create day-level indicators for every day from 2 years prior to 01/01 of the calendar year to 12/31 of the calendar year
	    (because the earliest stay-level date allowable would be 1.5 years prior to 01/01, but  for simplification,
	     just count beginning with 6/1 of two years prior)

	    Also recreate the SCHIP and NON_XOVR indicators (for simplicity had only rolled up by MDCD/XOVR);

	 execute (
	 	create temp table ip_stays_days 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select *
			  ,case when MDCD=0 then 1 else 0
			            end as SCHIP
		      ,case when XOVR=0 then 1 else 0
			             end as NON_XOVR

		     /* Loop over all three years, and then all days of the year, and based on service beginning and ending date (when both are
				not null), create a daily indicator.
			    Call macro leapyear for each year to determine whether year is a leap year so know number of days for February.
		        Create macro var alldays to be the total number of days (max of NUM macro var) to be able to count total days on roll-up */

				%let num=0;

				%do ipyear=&pyear2. %to &year.;
					%if &ipyear.=&pyear2. %then %let smonth=6;
					%else %let smonth=1;

					%leapyear(inyear=&ipyear.)

					%do month=&smonth. %to 12;
						%if %sysfunc(length(&month.))=1 %then %let month=0&month.;

						%if &month. in (01 03 05 07 08 10 12) %then %let lday=31;
						%if &month. in (04 06 09 11) %then %let lday=30;
						%if &month. = 02 and &leap.=0 %then %let lday=28;
						%if &month. = 02 and &leap.=1 %then %let lday=29;

						%do day=1 %to &lday.;
							%if %sysfunc(length(&day.))=1 %then %let day=0&day.;

							%let num=%eval(&num.+1);

							,case when date_cmp(stay_admsn_dt,%nrbquote('&ipyear.-&month.-&day.')) in (-1,0) and
						               date_cmp(stay_dschrg_dt,%nrbquote('&ipyear.-&month.-&day.')) in (0,1) 
		                          then 1 else 0 
				                  end as day&num.

							%let alldays=&num.;

						%end;
					%end; 

				%end;

		from ip_stays_out
		where (stay_dschrg_dt - stay_admsn_dt + 1) <= (365.25 * 1.5) and
		      date_part('year',stay_dschrg_dt)=&year.

	) by tmsis_passthrough;

	** Now by clm_type_cd, MDCD/SCHIP, and NON_XOVR/XOVR, get a count of unique days and stays. 
	   Must create two separate tables (one for MDCD and one for SHCIP) because of column limits;

	%do i=1 %to 2;
	   %let ind1=%scan(&INDS1.,&i.);

		execute (
			create temp table ip_stays_days_&ind1. 
			distkey(msis_ident_num)
        	sortkey(submtg_state_cd,msis_ident_num) as

			select submtg_state_cd
			       ,msis_ident_num

				  %do j=1 %to 2;
				  	 %let ind2=%scan(&INDS2.,&j.);

					  /* For FFS and MC, sum daily indicators to create day counts, and then just count once for
					     each matching record to get stay counts */

					   ,%do num=1 %to &alldays.;
							%if &num. > 1 %then %do; + %end;
								max(case when &ind1.=1 and &ind2.=1 and FFS=1 then day&num. else 0 end)
						%end;
						as &ind1._&ind2._FFS_IP_DAYS

						,sum(case when &ind1.=1 and &ind2.=1 and FFS=1
						          then 1 else 0 end)
							 as &ind1._&ind2._FFS_IP_STAYS

						,%do num=1 %to &alldays.;
							%if &num. > 1 %then %do; + %end;
								max(case when &ind1.=1 and &ind2.=1 and FFS=0 then day&num. else 0 end)
						%end;
						as &ind1._&ind2._MC_IP_DAYS

						,sum(case when &ind1.=1 and &ind2.=1 and FFS=0
						          then 1 else 0 end)
							 as &ind1._&ind2._MC_IP_STAYS

			 	 %end;


			from ip_stays_days
			where &ind1.=1
			group by submtg_state_cd
			         ,msis_ident_num

		) by tmsis_passthrough;

	%end;

	** Drop tables no longer needed;

	%drop_tables(ipl_3yr ipl_3yr_hdr ip_for_stays_unq_dschrg ip_for_stays_unq ip_for_stays_unq_ids ip_for_stays_overlaps ip_for_stays_nonoverlap ip_stays_out ip_stays_days );

%mend base_ip_stays;

/**********************************************************************************************/
/*Program: 005_up_base_lt.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Read in the LT line-level file, aggregate to header, and count unique LT days
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro base_lt_days;

	** Take the header-level file with needed cols, and join to a rolled-up line level file to
       get min srvc_bgnng_dt and TOS_CD from the line. Only keep FFS/MC claims, and only keep
       claims with at least one line with specific TOS_CD values (009, 043-050, 059), OR with 
       all TOS_CD values = null.

       Then need to get daily indicators for each stay, and then get the MAX across all days for each bene
       to create a count of unique LT days ;
       

	execute (
		create temp table lt_hdr_days_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
		       ,msis_ident_num
			   ,a.lt_link_key
			   ,clm_type_cd
			   ,MDCD
			   ,SCHIP
			   ,NON_XOVR
			   ,XOVR
			   ,srvc_endg_dt
			   ,srvc_bgnng_dt
			   ,admsn_dt

			   ,case when srvc_bgnng_dt is not null then srvc_bgnng_dt
                     when srvc_bgnng_dt_ln_min is not null then srvc_bgnng_dt_ln_min
					 when admsn_dt is not null then admsn_dt
					 else null
					 end as bgnng_dt

			   ,case when ANY_TOS_KEEP=1 or TOS_CD_NULL=1
				        then 1 else 0
						end as CLAIM_TOS_KEEP

				/* Loop over all days of the year, and based on service beginning and ending date (when both are
				   not null), create a daily indicator. Name in sequential order from 1-365/366 (to more easily loop over).
			       Call macro leapyear to determine whether year is a leap year so know number of days for February */

				%leapyear(inyear=&year.)

				%let num=0;

				%do month=1 %to 12;

					%if %sysfunc(length(&month.))=1 %then %let month=0&month.;
					%if &month. in (01 03 05 07 08 10 12) %then %let lday=31;
					%if &month. in (04 06 09 11) %then %let lday=30;
					%if &month. = 02 and &leap.=0 %then %let lday=28;
					%if &month. = 02 and &leap.=1 %then %let lday=29;

					%do day=1 %to &lday.;
						%if %sysfunc(length(&day.))=1 %then %let day=0&day.;

						%let num=%eval(&num.+1);

						,case when bgnng_dt is not null and
                                   srvc_endg_dt is not null and
                                   date_cmp(bgnng_dt,%nrbquote('&year.-&month.-&day.')) in (-1,0) and
					               date_cmp(srvc_endg_dt,%nrbquote('&year.-&month.-&day.')) in (0,1) 
	                          then 1 else 0 
			                  end as day&num.

					%end;

				%end; 

		from lth_&year. a
		     left join
			 (select lt_link_key
					 ,min(srvc_bgnng_dt_ln) as srvc_bgnng_dt_ln_min
					 ,max(case when TOS_CD in ('009','043','044','045','046','047','048','049','050','059')
						  then 1 else 0 end) as ANY_TOS_KEEP
					 ,case when max(TOS_CD) is null
						       then 1 else 0
							   end as TOS_CD_NULL

			  from ltl_&year.
			  group by lt_link_key ) b

		on a.lt_link_key = b.lt_link_key

	   where a.clm_type_cd in ('1','A','3','C')

	) by tmsis_passthrough;

	** Drop LTL;

	%drop_tables(ltl_&year.);

	** Now by clm_type_cd, MDCD/SCHIP, and NON_XOVR/XOVR, get a count of unique days. Subset to claims based on TOS_CD values.;

	execute (
		create temp table lt_hdr_days2_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
		       ,msis_ident_num
			   %do i=1 %to 2;
			   	  %let ind1=%scan(&INDS1.,&i.);
				  %do j=1 %to 2;
				  	 %let ind2=%scan(&INDS2.,&j.);

					 /* Create macro vars to assign claim types for MDCD or SCHIP */

				   	  %assign_toc

					   ,%do num=1 %to &totdays.;
							%if &num. > 1 %then %do; + %end;
								max(case when &ind1.=1 and &ind2.=1 and clm_type_cd=%nrbquote('&ffsval.') then day&num. else 0 end)
						%end;
						as &ind1._&ind2._FFS_LT_DAYS

						,%do num=1 %to &totdays.;
							%if &num. > 1 %then %do; + %end;
								max(case when &ind1.=1 and &ind2.=1 and clm_type_cd=%nrbquote('&mcval.') then day&num. else 0 end)
						%end;
						as &ind1._&ind2._MC_LT_DAYS

			 	 %end;
			  %end;


		from lt_hdr_days_&year.
		where CLAIM_TOS_KEEP=1
		group by submtg_state_cd
		         ,msis_ident_num

	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(lt_hdr_days_&year.)

%mend base_lt_days;

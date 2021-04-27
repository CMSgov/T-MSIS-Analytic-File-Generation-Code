/**********************************************************************************************/
/*Program: 021_bsf_ELG00021.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_ENRLMT_TIME_SGMT_DATA and create unique output for BSF.
/*Mod: 4/4/2018 - Complete rewrite for CCB to incorporate multiple spells. 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/

%macro process_enrlmt(type,enrl_type_cd);
/** Step 0: A) Set effective and end dates to death date if death date occurs before. **/
/**         B) Set null end dates to December 31, 9999 **/
execute
   (
     create temp table &tab_no._&type._step0 distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) 
	 as
	 select *
	 from &tab_no._v
     where enrlmt_type_cd = %nrbquote('&enrl_type_cd')
	 ) by tmsis_passthrough;

/** Steps 1-2: A) Remove records where beneficiary died before start of month    
	           B) Identify records completely within another record's date range **/
execute
   (
     create temp table &tab_no._&type._step1 distkey(dateId) sortkey(submtg_state_cd,msis_ident_num) 
	 as
	 select *
	        /** Create a unique date ID to filter on later **/
	 		,trim(submtg_state_cd ||'-'||msis_ident_num || '-' ||
                     cast(rank() over (partition by submtg_state_cd ,msis_ident_num
					 order by submtg_state_cd, msis_ident_num, &eff_date, &end_date) as char(3))) as dateId
	 from &tab_no._&type._step0
	 ) by tmsis_passthrough;

execute
   (
     create temp table &tab_no._&type._step1_overlaps distkey(dateid) sortkey(dateid) 
	 as
	 select t1.*
	 from &tab_no._&type._step1 t1 
	 inner join &tab_no._&type._step1 t2
	 /** Join records for beneficiary to each other, but omit matches where it's the same record **/
         on t1.submtg_state_cd = t2.submtg_state_cd
		 and t1.msis_ident_num = t2.msis_ident_num
		 and t1.dateId <> t2.dateId
	/** Get every dateID where their effective date is greater than or equal to another record's effective date
		AND their end date is less than or equal to that other record's end date. **/
	 where date_cmp(t1.&eff_date,t2.&eff_date) in(0,1)
	    and date_cmp(t1.&end_date,t2.&end_date) in (-1,0)
	 ) by tmsis_passthrough;

execute
   (
     create temp table &tab_no._&type._step2 distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) 
	 as
	 select t1.*

	 from &tab_no._&type._step1 t1
	 /** Join initial date to overlapping dateIDs and remove **/
	 left join &tab_no._&type._step1_overlaps t2
       on t1.dateid = t2.dateid
	where t2.dateid is null     
	 ) by tmsis_passthrough;

execute
   (
     create temp table &tab_no._&type._step3 distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) 
	 as

	 select submtg_state_cd, msis_ident_num
	       ,min(&eff_date) as &eff_date
		   ,max(&end_date) as &end_date

	 from
	 (
	 select submtg_state_cd ,msis_ident_num ,&eff_date ,&end_date
	        ,sum(C) over (partition by submtg_state_cd, msis_ident_num
			             order by &eff_date, &end_date
                         rows UNBOUNDED PRECEDING) as G
	 from
	 (
	 select submtg_state_cd
	       ,msis_ident_num
		   ,&eff_date
		   ,&end_date
		   ,m_eff_dt
		   ,m_end_dt
		   ,decode(sign(&eff_date-nvl(m_end_dt+1,&eff_date)),1,1,0) as C
	 from
     (select submtg_state_cd
	       ,msis_ident_num
		   ,&eff_date
		   ,&end_date
		   ,lag(&eff_date) over (partition by submtg_state_cd, msis_ident_num
		                         order by &eff_date, &end_date) as m_eff_dt
		   ,lag(&end_date) over (partition by submtg_state_cd, msis_ident_num
		                         order by &eff_date, &end_date) as m_end_dt
	 from &tab_no._&type._step2
     order by &eff_date, &end_date) s1 ) s2 ) s3 
	 group by submtg_state_cd, msis_ident_num, g
     
	 ) by tmsis_passthrough;


/** Step 4: Prep long table for transposition **/
execute
   (
     create temp table &tab_no._&type._step4 distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) 
	 as
	 select *
	 		,row_number() over (partition by submtg_state_cd ,msis_ident_num
					 order by submtg_state_cd, msis_ident_num, &eff_date, &end_date) as keeper
            /* If data indicates they were deceased well before their original effective dates, set to 0 */
			,greatest(datediff(day,greatest(&st_dt,&eff_date),
                      least(&rpt_prd, &end_date))+1,0) as NUM_DAYS
		    ,case when date_cmp(&end_date,&rpt_prd) in(0,1) then 1 else 0 end as ELIG_LAST_DAY
	from  &tab_no._&type._step3        
	 ) by tmsis_passthrough;

/** Determine Max number of keeper  **/
select max_keep into :max_keep
from (select * from connection to tmsis_passthrough
      (select max(keeper) as max_keep from &tab_no._&type._step4));

/** Step 5: Transpose data from long to wide and create &type specific eligibility columns **/
execute
   (
     create temp table &type._spells distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) 
	 as
	 select m.*
		   ,1 as &type._ENR

		/** Allow for up to 16 columns in array. If there are <16 total spells, use nulls **/
		 %do I=1 %to 16;
		  %if %eval(&I <= &max_keep) %then
		   %do;
	          ,t&I..&eff_date as &TYPE._ENRLMT_EFF_DT_&I.
		      ,t&I..&end_date as &TYPE._ENRLMT_END_DT_&I.
		   %end; %else
		   %do;
	          ,cast(null as date) as &TYPE._ENRLMT_EFF_DT_&I.
		      ,cast(null as date) as &TYPE._ENRLMT_END_DT_&I.
		   %end;
        %end;

	 from (select submtg_state_cd 
                  ,msis_ident_num 
	              ,sum(NUM_DAYS) as DAYS_ELIG_IN_MO_CNT
				  ,max(ELIG_LAST_DAY) as ELIG_LAST_DAY
	       from &tab_no._&type._step4 group by submtg_state_cd, msis_ident_num) m
          %do I=1 %to 16;
		   %if %eval(&I <= &max_keep) %then
 
            %do;
	          %tbl_joiner(&I)
			%end; 
        %end;   
	 ) by tmsis_passthrough;

%mend process_enrlmt;

%macro create_ELG00021(tab_no, _2x_segment, eff_date, end_date);
	%macro tbl_joiner(tblnum);
			left join (select * from &tab_no._&type._step4 where keeper=&tblnum) t&tblnum. 
	          on m.submtg_state_cd=t&tblnum..submtg_state_cd
			 and m.msis_ident_num =t&tblnum..msis_ident_num
	%mend tbl_joiner;


/* For bene/state combinations, remove records that overlap across enrollment types. */

/* First use sort order to identify which records are priority across enrollment types */
execute ( create temp table &tab_no._step1 distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) as		  
		  select *
		  ,row_number() over (partition by submtg_state_cd 
                                      ,msis_ident_num
					     order by submtg_state_cd, 
                                  msis_ident_num,
								  TMSIS_RPTG_PRD desc,
								  &eff_date desc,
								  &end_date desc, 
								  REC_NUM desc,
                                  coalesce(enrlmt_type_cd,'999')) as ORDER_FLAG
		 from &tab_no		 ) by tmsis_passthrough;

/* Next match records to themselves and limit to where enrollment types are different. */

execute (create temp table &tab_no._step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
		 select t1.*
		 /* Create a flag that indicates if the record overlaps with another in any time frame. */
		 /* Set the flag to 1 only if it overlaps and is for a lower prioty record. Ie - If */
		 /* there is an overlap, it will be true for both records, so only flag the higher order */
		 /* (lesser priority) for removal. */

       ,case when ((t1.&eff_date between t2.&eff_date and t2.&end_date) or 
                  (t2.&eff_date between t1.&eff_date and t1.&end_date))
	        and t1.ORDER_FLAG > t2.ORDER_FLAG then 1 else 0 end as OVR_LAP_RMV

		 from &tab_no._step1 t1
		 left join &tab_no._step1 t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		 and t1.msis_ident_num = t2.msis_ident_num
		 and coalesce(t1.enrlmt_type_cd,'X') <> coalesce(t2.enrlmt_type_cd,'X')

        where OVR_LAP_RMV = 1 /* Only keep records to be removed in this table */) by tmsis_passthrough;

		/* Next match the records to be removed back to the ordered records and drop the record */
		/* if it appears in the removal table. */
execute ( create temp table &tab_no._step3 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
		  select t1.*
		  from &tab_no._step1 t1
		  left join &tab_no._step2 t2
		  on t1.submtg_state_cd = t2.submtg_state_cd
		  and t1.msis_ident_num = t2.msis_ident_num
		  and t1.order_flag = t2.order_flag
		  where t2.msis_ident_num is null ) by tmsis_passthrough;

		  /* After records have been sanitized of overlaps across enrollment types, begin the processing */
		  /* for within enrollment type collapsing. */
execute
   (
     create temp table &tab_no._v distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) 
	 as
	 select distinct 
         e21.submtg_state_cd
        ,e21.msis_ident_num
		,e21.tmsis_run_id
		,e21.enrlmt_type_cd
		,greatest(case when date_cmp(e02.death_date,&eff_date) in(-1,0) 
                or (e02.death_date is not null and &eff_date is null) then e02.death_date
              else &eff_date end, &st_dt::date) as &eff_date		
        ,least(case when date_cmp(e02.death_date,&end_date) in(-1,0) 
                or (e02.death_date is not null and &end_date is null) then e02.death_date
              when &end_date is null then '31DEC9999' else &end_date end, &rpt_prd::date) as &end_date
	 from &tab_no._step3 e21
	 left join ELG00002_&BSF_FILE_DATE._uniq e02
	 on e21.submtg_state_cd = e02.submtg_state_cd
	 and e21.msis_ident_num = e02.msis_ident_num
     where e21.msis_ident_num is not null
		/* Filter out records where the death_date is before the start of the month **/
		/* This is separate from process enrollment because it includes UNK enrollment types **/
		  and date_cmp(least(&st_dt,nvl(death_date,&end_date,'31DEC9999')),&st_dt) in(0,1)
	    /* Also remove any records where the effective date is after their death date */
		  and date_cmp(&eff_date,least(death_date,'31DEC9999')) <> 1
	 ) by tmsis_passthrough;

%process_enrlmt(MDCD,1);
%process_enrlmt(CHIP,2);

/** Get master list of eligible beneficiaries and check for unknown enrollment info **/
execute (
     create temp table &tab_no._buckets
	 distkey(msis_ident_num) 
	 sortkey(submtg_state_cd,msis_ident_num) as
	 select 
        b.submtg_state_cd 
        ,b.msis_ident_num        
		,max(b.tmsis_run_id) as tmsis_run_id

		%do I=1 %to &DAYS_IN_MONTH;
		 %let yr_mn = %substr(&rpt_out,3,7);
		 %if %eval(&I<10 ) %then
		  %let dt_to_ck = %nrbquote('0&I.&yr_mn');
		     %else %let dt_to_ck = %nrbquote('&I.&yr_mn');

	    ,max(case when cast(ENRLMT_TYPE_CD as integer) in(1,2)
		          and date_cmp(&eff_date,&dt_to_ck) in(-1,0)
				  and date_cmp(&end_date,&dt_to_ck) in(1,0) then 1 else 0 end) as DT_CHK_&I
		%end;
	
		,sum(case when cast(ENRLMT_TYPE_CD as integer) not in(1,2) or ENRLMT_TYPE_CD is null then 1 else 0 end) as bucket_c
        from &tab_no._v b
		group by b.submtg_state_cd, b.msis_ident_num

) by tmsis_passthrough;

create table states as
select * from connection to tmsis_passthrough
( select distinct submtg_state_cd from &tab_no);

/** Create state abbrev case statement using SAS function and macro variables **/
title 'Generated case statements for FIPS codes';
select cat("when b.submtg_state_cd='",submtg_state_cd,"' then ","'",fipstate(input(submtg_state_cd,2.)),"'") 
into :state_format
separated by ' '
from states;

/** Combine master list to Medicaid and CHIP specific tables **/
execute (
     create temp table &tab_no._combined
	 distkey(msis_ident_num) sortkey(msis_ident_num,submtg_state_cd) as
	 select b.*,
	        coalesce(m.MDCD_ENR,0) as MDCD_ENR,
		    coalesce(c.CHIP_ENR,0) as CHIP_ENR,
           %do I=1 %to 16;
	          m.MDCD_ENRLMT_EFF_DT_&I.,
		      m.MDCD_ENRLMT_END_DT_&I.,
		   %end;			
            
           %do I=1 %to 16;
	         c.CHIP_ENRLMT_EFF_DT_&I.,
		     c.CHIP_ENRLMT_END_DT_&I.,
		   %end;
		
		    /** Number of days in month equal to last day value of month **/
			&DAYS_IN_MONTH as DAYS_IN_MONTH,

			/** Sum the number of days in the month that they had enrollment in Medicad or CHIP**/
			DT_CHK_1
			%do I=2 %to &DAYS_IN_MONTH;
			      + DT_CHK_&I
			%end; as DAYS_ELIG_IN_MO_CNT,

			/** Eligible entire month if the number of days in the month is <= sum of days in Medicaid or CHIP **/
			case when DT_CHK_1 = 1
			%do I=2 %to &DAYS_IN_MONTH;
			      and DT_CHK_&I = 1
			%end; then 1 else 0 end as ELIGIBLE_ENTIRE_MONTH_IND,

			/** Eligible Last day if they are eligible on the last day for any of their Medicaid or CHIP records **/
            greatest(m.ELIG_LAST_DAY,c.ELIG_LAST_DAY,0) as ELIGIBLE_LAST_DAY_OF_MONTH_IND,

			case when m.MDCD_ENR = 1 then 1
			     when c.CHIP_ENR  = 1 then 2
				 else null end as ENROLLMENT_TYPE_FLAG,

			/** Single Enroll if only one spell across Medicaid and CHIP and no unknown **/
			case when b.bucket_c=1 or
			          (MDCD_ENR=1 and CHIP_ENR=1) or
					  (m.MDCD_ENRLMT_EFF_DT_1 is not null and m.MDCD_ENRLMT_EFF_DT_2 is not null) or
					  (c.CHIP_ENRLMT_EFF_DT_1 is not null and c.CHIP_ENRLMT_EFF_DT_2 is not null)
			     then 0 else 1 end as SINGLE_ENR_FLAG, 

            case &state_format end as ST_ABBREV

		from &tab_no._buckets b

		left outer join MDCD_SPELLS m 
         on b.submtg_state_cd=m.submtg_state_cd 
        and b.msis_ident_num=m.msis_ident_num

		left outer join CHIP_SPELLS c 
         on b.submtg_state_cd=c.submtg_state_cd 
        and b.msis_ident_num=c.msis_ident_num
		
		) by tmsis_passthrough;

execute (
     /** Output table with SSN_IND and region column to provide master table for final program **/
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num) sortkey(msis_ident_num,submtg_state_cd) as
	 select 
        c.*
		,s.ssn_ind
   ,case when ST_ABBREV in('CT','MA','ME','NH','RI','VT') 			then '01'
         when ST_ABBREV in('NJ','NY','PR','VI')						then '02'
         when ST_ABBREV in('DE','DC','MD','PA','VA','WV') 			then '03'
         when ST_ABBREV in('AL','FL','GA','KY','MS','NC','SC','TN') then '04'
         when ST_ABBREV in('IL','IN','MI','MN','OH','WI')           then '05'
         when ST_ABBREV in('AR','LA','NM','OK','TX')				then '06'
         when ST_ABBREV in('IA','KS','MO','NE')						then '07'
         when ST_ABBREV in('CO','MT','ND','SD','UT','WY')			then '08'
         when ST_ABBREV in('AZ','CA','HI','NV','AS','GU','MP')		then '09'
         when ST_ABBREV in('AK','ID','OR','WA')						then '10'
		 when c.submtg_state_cd = '97' 								then '03'
		 when c.submtg_state_cd in ('93','94')                      then '08'
         else '11' end as REGION
        from &tab_no._combined c
		/* Add ssn_ind from initial max ID pull */
		left join ssn_ind s
		  on c.submtg_state_cd=s.submtg_state_cd
		) by tmsis_passthrough;

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 021_bsf_ELG00021, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 021_bsf_ELG00021, 21.1 create_ELG00021);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 021_bsf_ELG00021, 21.2 process_enrlmt);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 021_bsf_ELG00021, 21.3 join);

%drop_table_multi(&tab_no._mdcd_step0 &tab_no._mdcd_step1 &tab_no._mdcd_step2
                  &tab_no._mdcd_step3 &tab_no._mdcd_step4 mdcd_spells
                  &tab_no._chip_step0 &tab_no._chip_step1 &tab_no._chip_step2
                  &tab_no._chip_step3 &tab_no._chip_step4 chip_spells
                  &tab_no._buckets &tab_no._combined);

%mend create_ELG00021;

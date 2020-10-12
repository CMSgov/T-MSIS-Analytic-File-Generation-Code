/**********************************************************************************************/
/*Program: 002_bsf_ELG00002.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_PRMRY_DMGRPHC_ELGBLTY and create unique output for BSF.
/*Mod: 
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
 
%macro create_ELG00002(tab_no, _2x_segment, eff_date, end_date);
%let deceased_flag= case when DEATH_DT is not null and date_cmp(DEATH_DT,&RPT_PRD) in(-1,0) then 1 else 0 end;
%let created_vars = upper(GNDR_CD) as GNDR_CODE;
%let AGE = case when BIRTH_DT is null then null
             when coalesce(d.DECEASED_FLG,0)=1
             then floor((d.DEATH_DATE-comb.BIRTH_DT)/365.25)
		     else floor((&RPT_PRD-comb.BIRTH_DT)/365.25) end;
 execute(

     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._recCt 
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num,recCt) as 
	 select 
        submtg_state_cd, 
        msis_ident_num,
		count(TMSIS_RUN_ID) as recCt
		from &tab_no
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select recCt,count(msis_ident_num) as beneficiaries from &tab_no._recCt group by recCt ) order by recCt;


 execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no._uniq 
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num)  as 

	 select t1.*,

        &created_vars,
		1 as KEEP_FLAG

		from &tab_no t1
        inner join &tab_no._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1
		) by tmsis_passthrough;

title "Number of beneficiary with unique records in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._uniq );

%MultiIds(sort_key=%str(coalesce(gndr_cd,'xx')||coalesce(cast(birth_dt as char(10)),'xx')
                        ||coalesce(cast(death_dt as char(10)),'xx')));

title "Number of beneficiares who were processed for duplicates in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._multi );

/* Select best record for death date */
execute (
   create temp table &tab_no._death 
   distkey(msis_ident_num) 
   sortkey(submtg_state_cd,msis_ident_num,best_record) as

   select submtg_state_cd,
          msis_ident_num,
		  case when to_char(DEATH_DT,'YYYYMM')::integer > &BSF_FILE_DATE then null else DEATH_DT end as DEATH_DATE,
		  &deceased_flag as DECEASED_FLG,
	  	  row_number() over (partition by submtg_state_cd,
	                              msis_ident_num
                order by submtg_state_cd,
	                     msis_ident_num,
						 TMSIS_RPTG_PRD desc,
						 &eff_date desc,
						 &end_date desc,
						 REC_NUM desc,
                         DEATH_DT desc) as best_record
	from &tab_no.A ) by tmsis_passthrough; 
          

 execute(

     /* Union together tables for a permanent table */
     create temp table &tab_no._&BSF_FILE_DATE._uniq 
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num) as 

	 select comb.*,
	        coalesce(d.DECEASED_FLG,0) as DECEASED_FLG,
            d.DEATH_DATE,
			&AGE as AGE_CALC,
			case when AGE_CALC>125 then 125 
                 when AGE_CALC < -1 then null
                 else AGE_CALC end as AGE,

		case when BIRTH_DT is null or AGE < -1 then null
             when AGE between -1  and 0 then 1
		     when AGE between  1  and 5 then 2
		     when AGE between  6 and 14 then 3
			 when AGE between 15 and 18 then 4
			 when AGE between 19 and 20 then 5
			 when AGE between 21 and 44 then 6
			 when AGE between 45 and 64 then 7
			 when AGE between 65 and 74 then 8
			 when AGE between 75 and 84 then 9
			 when AGE between 85 and 125 then 10
             else null end as AGE_GROUP_FLG
	 from
	 (select * from &tab_no._uniq
	 union all
	 select * from &tab_no._multi) comb
 
	 /* Compute deceased flag and death_dt and join back to unique table data */
	 left join &tab_no._death  d
	  
	    on comb.SUBMTG_STATE_CD=d.SUBMTG_STATE_CD
	   and comb.msis_ident_num=d.msis_ident_num
	   and d.best_record=1

     ) by tmsis_passthrough;

title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));

%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 002_bsf_ELG00002, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 002_bsf_ELG00002, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no.A &tab_no._recCt &tab_no._uniq &tab_no._death &tab_no._multi &tab_no._multi_all &tab_no._multi_step2);

%mend create_ELG00002;


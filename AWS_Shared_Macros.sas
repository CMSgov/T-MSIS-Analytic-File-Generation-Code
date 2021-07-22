/************************************************************************************************************/
/* Program:		AWS_Shared_Macros.sas																		*/
/* Author:		Deo S. Bencio																				*/
/* Date:		12/1/2016																					*/
/* Purpose:		Program contains macros that massage data from various segments in T-MSIS					*/
/*				THIS PROGRAM DOES NOT STAND ALONE.                                                      	*/
/*				IP_build.sas - pull program for IP build													*/
/*				LT_build.sas - pull program for LT build													*/
/*				OT_build.sas - pull program for OT build													*/
/*				RX_build.sas - pull program for RX build													*/
/*																											*/
/* Modified:    12/12/2016 - CA update RX list of variables and remove eligibility macros                   */
/*              2/20/2017  - CA change macro calls for pulling header and claims family segments to include */
/*                           distribution and sort keys & dedupe claims family and header tables before join*/                                                    
/*              3/2/2017   - CA recode PA Chip and IA Chip to state code. Change row number to a macro var  */ 
/*				5/23/2017  - DB modified code to split file-specific macros from shared macros.  			*/
/*							 this program contains macros that are shared by all claims files.				*/
/*              7/25/2017  - CA Modified to fit the Interim Solution                                        */                                                                                                         
/*				12/10/2017 - DB modified code to allow IP claims with no discharge dates to be included in  */
/*							 a monthly run of TAF.  Code is written to minimize change to other file types  */
/*          	4/2/2018   - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q1.xlsx           */
/*				10/4/2018 - DB modified to apply CCB Data-Cleaning Business rules - 2018 Q3					*/
/*							Added macro var_set_taxo to clean up 0-fill in taxonomy codes 					*/
/*							Modified macro var_set_fills to nullify 0.00 in PRCDR_CD fields 				*/
/*              12/17/2018 - Updated column name to get the code spec version for TAF processing            */
/*				3/7/2019 	- DB modified to apply CCB Data-Cleaning Business Rules - 2019 Q1.xlsx			*/
/*							Added column LINE_NUM to identify line numbers in LINE table					*/
/*				3/11/2019 - DB modified to add two new columns fil_dt and taf_sd_spec_vrsn_name 			*/
/*							to efts_file_meta.Added code to populate the newly added fields					*/
/*              9/18/2019 - DB/AL modified to apply CCB Data-Cleaning Business Rules - 2019 Q3.xlsx         */
/*						    modified macro var_set_fills to remove translate of dots to missing             */
/*				6/9/2020  - DB modified to apply TAF CCB 2020 Q2 Change Request                             */
/*              12/15/2020- DB modified to apply TAF CCB 2020 Q4 Change Request                             */
/*							-MACTAF-1581: Mod macro var_set_tos to include new value 145                    */
/*							-MACTAF-1613: Exclude IA CHIP T-MSIS files from TAF Production					*/
/*							-MACTAF-1564: Add TAF selection date to the IP and OT headers					*/
/*				3/4/2021  - DB modified to apply TAF CCB 2021 Q1 V5.1 Change Request						*/
/*							-MACTAF-1680: Add codes '46-'46B' to xix_srvc_ctgry_cd							*/
/*							-MACTAF-1682: Mod macro var_set_tos to include new value 146					*/
/*				6/7/2021  - DB modified to apply TAF CCB 2021 Q2 V6.0 Change Request						*/
/*							-MACTAF-1706: Add codes '2C,18A5'  to xix_srvc_ctgry_cd							*/
/*							-MACTAF-1708: Mod macro var_set_poa to include code '1'							*/
/*							-MACTAF-1719: Hard code exclusion of MT TPA (state code 94) from TAF            */
/************************************************************************************************************/

options SASTRACE=',,,ds' SASTRACELOC=Saslog nostsuffix dbidirectexec sqlgeneration=dbms msglevel=I sql_ip_trace=source;
options spool errorabend minoperator;

/* MACRO TO READ IN PARAMETERS FROM LOOKUP TABLE */
%MACRO JOB_CONTROL_RD(FILE_TYPE,DA_SCHEMA);
   CREATE TABLE JOB_CD_LOOKUP AS
   select * from connection to tmsis_passthrough(
      SELECT 
       DA_RUN_ID,
	   SCHLD_ORDR_NUM, 
	   JOB_PARMS_TXT,
	   TAF_CD_SPEC_VRSN_NAME
	FROM &DA_SCHEMA..JOB_CNTL_PARMS
	WHERE SCHLD_ORDR_NUM = (SELECT MIN(SCHLD_ORDR_NUM) 
	FROM &DA_SCHEMA..JOB_CNTL_PARMS
	WHERE SUCSFL_IND IS  NULL
	AND FIL_TYPE = %nrbquote('&FILE_TYPE.'))
	AND FIL_TYPE = %nrbquote('&FILE_TYPE.')
	;
     ) ;
     
   SELECT DA_RUN_ID 
          ,SUBSTR(JOB_PARMS_TXT,1,10) AS RPTPD FORMAT=$10.
		  ,SUBSTR(TAF_CD_SPEC_VRSN_NAME,2,2) FORMAT=$2. 
          ,CASE WHEN FIND(JOB_PARMS_TXT,",")>0
                THEN SUBSTR(JOB_PARMS_TXT,FIND(JOB_PARMS_TXT,",")+1,LENGTH(JOB_PARMS_TXT)) 
                ELSE 'ALL' END AS ST_FILTER
	  INTO :DA_RUN_ID,:REPORTING_PERIOD, :VERSION , :ST_FILTER
   FROM JOB_CD_LOOKUP;

%MEND JOB_CONTROL_RD;

/* MACRO TO UPDATE START TIME IN LOOKUP TABLE */
%MACRO JOB_CONTROL_UPDT(DA_RUN_ID,DA_SCHEMA);
   
   EXECUTE (
      UPDATE &DA_SCHEMA..JOB_CNTL_PARMS
      SET JOB_STRT_TS = CONVERT_TIMEZONE('EDT', GETDATE())
      WHERE DA_RUN_ID = &DA_RUN_ID.
	;
     ) BY TMSIS_PASSTHROUGH;
     
%MEND JOB_CONTROL_UPDT;

/* MACRO TO UPDATE END TIME IN LOOKUP TABLE */
%MACRO JOB_CONTROL_UPDT2(DA_RUN_ID,DA_SCHEMA);
   
   EXECUTE (
      UPDATE &DA_SCHEMA..JOB_CNTL_PARMS
      SET JOB_END_TS = CONVERT_TIMEZONE('EDT', GETDATE()),
      SUCSFL_IND = 1
      WHERE DA_RUN_ID = &DA_RUN_ID.
      ;
     ) BY TMSIS_PASSTHROUGH;
     
%MEND JOB_CONTROL_UPDT2;

%MACRO GET_CNT(TABLE_NAME, DA_SCHEMA);
  CREATE TABLE RECORD_COUNT AS
   select * from connection to tmsis_passthrough(   
      
      SELECT COUNT(TMSIS_RUN_ID) AS ROW_CNT 
      FROM &DA_SCHEMA..&TABLE_NAME.
	  WHERE DA_RUN_ID = &DA_RUN_ID.
      ;
    ) ;    
   
      SELECT
      ROW_CNT INTO :ROWCOUNT
      FROM RECORD_COUNT
      ;

 
%MEND GET_CNT;   

%MACRO CREATE_META_INFO(DA_SCHEMA, TABLE_NAME, DA_RUN_ID, ROWCOUNT,FIL_4TH_NODE);

   EXECUTE (
      INSERT INTO &DA_SCHEMA..JOB_OTPT_META
      (DA_RUN_ID ,   
      OTPT_TYPE ,  
      OTPT_NAME,  
      OTPT_LCTN_TXT ,  
      REC_CNT,
      FIL_4TH_NODE_TXT)
      VALUES
      (&DA_RUN_ID.,
      'TABLE',
       %nrbquote('&TABLE_NAME.') ,
       %nrbquote('&DA_SCHEMA.') ,
       &ROWCOUNT.,
	   %NRBQUOTE('&FIL_4TH_NODE.')
       ) ;
	   )BY TMSIS_PASSTHROUGH;
   

%MEND CREATE_META_INFO;

/* Macro to create the EFT file meta data */
%MACRO CREATE_EFTSMETA_INFO(DA_SCHEMA, DA_RUN_ID, TABLE_NAME, pgm_name, step_name, object_name,audt_count);

   EXECUTE (
      INSERT INTO &DA_SCHEMA..EFTS_FIL_META
	  (da_run_id,fil_4th_node_txt,otpt_name,rptg_prd,itrtn_num,tot_rec_cnt,fil_cret_dt,incldd_state_cd,rec_cnt_by_state_cd,fil_dt,taf_cd_spec_vrsn_name)
	  SELECT t1.da_run_id,
             t2.fil_4th_node_txt,
			 t2.otpt_name,
             to_char(cast(substring(t1.job_parms_txt, 1, 10) as date),'Month,YYYY')  as rptg_prd,	         
			 substring(t1.taf_cd_spec_vrsn_name,2,2) as itrtn_num,
			 t2.rec_cnt as tot_rec_cnt,
			 to_char(date(t2.rec_add_ts),'MM/DD/YYYY') as fil_cret_dt,
			 coalesce(t3.submtg_state_cd, 'Missing')  as incldd_state_cd,
			 t3.audt_cnt_val  as rec_cnt_by_state_cd,
			 %nrbquote('&TAF_FILE_DATE.') as fil_dt,
			 t1.taf_cd_spec_vrsn_name
			 from &da_schema..job_cntl_parms t1,
			       &da_schema..job_otpt_meta  t2,
				   &da_schema..pgm_audt_cnts  t3,
				   &da_schema..pgm_audt_cnt_lkp t4
				  where t1.da_run_id = &DA_RUN_ID.
                  and   t1.da_run_id = t2.da_run_id
				  and   t2.da_run_id = t3.da_run_id
                  and   t2.otpt_name = %nrbquote('&TABLE_NAME.')
				  and   t3.pgm_audt_cnt_id = t4.pgm_audt_cnt_id
				  and   t1.sucsfl_ind = true
				  and   t4.pgm_name  = %nrbquote('&pgm_name.')
				  and   t4.step_name = %nrbquote('&step_name.')
                  and   t4.obj_name   = %nrbquote('&object_name.')
                  and   t4.audt_cnt_of = %nrbquote('&audt_count.')	
       
	   )BY TMSIS_PASSTHROUGH;
   

%MEND CREATE_EFTSMETA_INFO;

/* MACRO TO READ IN PARAMETERS FROM LOOKUP TABLE */
%MACRO FINAL_CONTROL_INFO(DA_RUN_ID, DA_SCHEMA);
   CREATE TABLE FINAL_CONTROL_INFO AS
   SELECT * FROM CONNECTION TO TMSIS_PASSTHROUGH(
       SELECT
	     A.DA_RUN_ID,
		 JOB_STRT_TS,
		 JOB_END_TS,
		 CAST(OTPT_NAME AS CHAR(100)) AS OTPTNAME,
		 CAST(OTPT_LCTN_TXT AS CHAR(100)) AS OTPTLCTNTXT,
		 REC_CNT
	   FROM &DA_SCHEMA..JOB_CNTL_PARMS A,
	        &DA_SCHEMA..JOB_OTPT_META  B
	   WHERE A.DA_RUN_ID = B.DA_RUN_ID
	   AND   A.DA_RUN_ID = &DA_RUN_ID.
	   ;
	);
%MEND FINAL_CONTROL_INFO;

%MACRO TRUNCATE_TABLE(DA_SCHEMA,TABLE_NAME);
   EXECUTE (
   		TRUNCATE TABLE &DA_SCHEMA..&TABLE_NAME.
         	;  
     ) BY TMSIS_PASSTHROUGH;
%MEND TRUNCATE_TABLE;

%MACRO UPDATE_TRUNCATE_METADATA(DA_SCHEMA,FILE_TYPE);
title "The run ids that are being truncated are :";
select * from connection to tmsis_passthrough
 ( SELECT T2.DA_RUN_ID
	  							  FROM &DA_SCHEMA..JOB_CNTL_PARMS T1,
								       &DA_SCHEMA..JOB_OTPT_META T2
									WHERE T1.DA_RUN_ID = T2.DA_RUN_ID
									AND   T1.FIL_TYPE = %nrbquote('&FILE_TYPE.')
									AND   T2.TRNCT_TS IS NULL) ;

   EXECUTE (
      UPDATE &DA_SCHEMA..JOB_OTPT_META  
	  SET  TRNCT_TS = CONVERT_TIMEZONE('EDT', GETDATE()),
	       REC_UPDT_TS = CONVERT_TIMEZONE('EDT', GETDATE())
	  WHERE  DA_RUN_ID IN ( SELECT T2.DA_RUN_ID
	  							  FROM &DA_SCHEMA..JOB_CNTL_PARMS T1,
								       &DA_SCHEMA..JOB_OTPT_META T2
									WHERE T1.DA_RUN_ID = T2.DA_RUN_ID
									AND   T1.FIL_TYPE = %nrbquote('&FILE_TYPE.'))
     AND TRNCT_TS IS NULL 
	;  
     ) BY TMSIS_PASSTHROUGH;
     
%MEND UPDATE_TRUNCATE_METADATA;

/* MACRO TO CREATE A CONTENT LISTING OF TAF FILE */   
%MACRO FILE_CONTENTS(DA_SCHEMA, TABLE_NAME);
   CREATE TABLE FILE_CONTENTS_&TABLE_NAME. AS
   select * from connection to tmsis_passthrough( 

   SELECT * 
   FROM &DA_SCHEMA..&TABLE_NAME.
   WHERE DA_RUN_ID = &DA_RUN_ID.
   LIMIT 1
     
	;
     ) ;

  
 
%MEND FILE_CONTENTS;

%macro DROP_temp_tables(temp_table_name);
 	execute 
	(
	  drop table if exists &temp_table_name.
    )by tmsis_passthrough;
  
%mend DROP_temp_tables;

%macro Get_Audt_counts(DA_SCHEMA,DA_RUN_ID, pgm_name, step_name);
        %let rcnt=0;
		create table cnt_table as 
		SELECT * FROM CONNECTION TO tmsis_passthrough
		(   

		SELECT * 
		FROM &DA_SCHEMA..PGM_AUDT_CNT_LKP 
		WHERE PGM_NAME = %nrbquote('&pgm_name.')
		and step_name =  %nrbquote('&step_name.')	 
		;

		);
        title;
		select left(put(count(*),4.)) into :rcnt  
		from cnt_table;
		select pgm_audt_cnt_id into :pgm_audt_cnt_id1 - :pgm_audt_cnt_id&rcnt. from cnt_table;
		select obj_name into :obj_name1-:obj_name&rcnt. from cnt_table;
		select audt_cnt_of into :audt_cnt_of1 - :audt_cnt_of&rcnt. from cnt_table;
		select grp_by into :grp_by1 - :grp_by&rcnt. from cnt_table;

		%getcount(&DA_SCHEMA.,&DA_RUN_ID., &rcnt.);
%mend Get_Audt_counts;

%macro getcount(DA_SCHEMA,DA_RUN_ID, rcnt);

 %do j=1 %to &rcnt.;
	execute 
	(	
		create temp table row_count&j. as
		select da_run_id, pgm_audt_cnt_id, submtg_state_cd, audt_cnt_val
		from 
		(
		select &da_run_id. as da_run_id,
                &&pgm_audt_cnt_id&j. as pgm_audt_cnt_id,
                t1.state as submtg_state_cd,
				t1.cnt as audt_cnt_val
		from 
	    (
		select &&grp_by&j. as state, count(&&audt_cnt_of&j.) as cnt from 
				&&obj_name&j.	
				group by &&grp_by&j.		
		) as t1
		) as t2
		union 
		select &da_run_id. as da_run_id,
                &&pgm_audt_cnt_id&j. as pgm_audt_cnt_id, 
				'xx' as submtg_state_cd, 
				0 as audt_cnt_val
		
     )by tmsis_passthrough;

 	 %let rtcnt=0;
	 	create table temp_rcnt_table as 
		SELECT * FROM CONNECTION TO tmsis_passthrough
		(   
        select  case when t1.cnt = 1 then 1
	             when t1.cnt > 1 then t1.cnt -1
			   end as rcnt
	 	from 
	    (
		SELECT count(*) as cnt 
		FROM row_count&j.
		) as t1			 
		;

		);

		select rcnt into :rtcnt  
		from temp_rcnt_table;
	 

	 execute 
	(	
		insert into &DA_SCHEMA..PGM_AUDT_CNTS
		select da_run_id, pgm_audt_cnt_id, nullif(submtg_state_cd, 'xx'),audt_cnt_val from row_count&j.
		order by audt_cnt_val desc
		limit &rtcnt.
		 
		
     )by tmsis_passthrough;

	 %DROP_temp_tables(row_count&j.);
	 
 %end;

%mend getcount;

%macro Get_Audt_counts_clms(DA_SCHEMA,DA_RUN_ID, pgm_name, step_name);
        %let rcnt=0;
		create table cnt_table as 
		SELECT * FROM CONNECTION TO tmsis_passthrough
		(   

		SELECT * 
		FROM &DA_SCHEMA..PGM_AUDT_CNT_LKP 
		WHERE PGM_NAME = %nrbquote('&pgm_name.')
		and step_name =  %nrbquote('&step_name.')	 
		;

		);
        title;
		select left(put(count(*),4.)) into :rcnt  
		from cnt_table;
		select pgm_audt_cnt_id into :pgm_audt_cnt_id1 - :pgm_audt_cnt_id&rcnt. from cnt_table;
		select obj_name into :obj_name1-:obj_name&rcnt. from cnt_table;
		select audt_cnt_of into :audt_cnt_of1 - :audt_cnt_of&rcnt. from cnt_table;
		select grp_by into :grp_by1 - :grp_by&rcnt. from cnt_table;

		%getcount_clms(&DA_SCHEMA.,&DA_RUN_ID., &rcnt.);
%mend Get_Audt_counts_clms;

%macro getcount_clms(DA_SCHEMA,DA_RUN_ID, rcnt);

 %do j=1 %to &rcnt.;
	execute 
	(	
		create temp table row_count&j. as
		select da_run_id, pgm_audt_cnt_id, submtg_state_cd, audt_cnt_val
		from 
		(
		select &da_run_id. as da_run_id,
                &&pgm_audt_cnt_id&j. as pgm_audt_cnt_id,
                t1.state as submtg_state_cd,
				t1.cnt as audt_cnt_val
		from 
	    (
		select &&grp_by&j. as state, count(&&grp_by&j.) as cnt from
		(
		%if &&audt_cnt_of&j.= submtg_state_cd or &&audt_cnt_of&j.= new_submtg_state_cd_line or &&audt_cnt_of&j.= new_submtg_state_cd %then 
		%do;
		select  &&audt_cnt_of&j.  from 
				&&obj_name&j.	
		%end;
		%else %do;

		select  &&audt_cnt_of&j., &&grp_by&j. from 
				&&obj_name&j.
		%end;	
					
		) as t0		
		 group by &&grp_by&j.
		) as t1	
		) as t2
		union 
		select &da_run_id. as da_run_id,
                &&pgm_audt_cnt_id&j. as pgm_audt_cnt_id, 'xx' as submtg_state_cd, 0 as audt_cnt_val

		
     )by tmsis_passthrough;

 	 %let rtcnt=0;
	 	create table temp_rcnt_table as 
		SELECT * FROM CONNECTION TO tmsis_passthrough
		(   
        select  case when t1.cnt = 1 then 1
	             when t1.cnt > 1 then t1.cnt -1
			   end as rcnt
	 	from 
	    (
		SELECT count(*) as cnt 
		FROM row_count&j.
		) as t1			 
		;

		);

		select rcnt into :rtcnt  
		from temp_rcnt_table;
	 

	 execute 
	(	
		insert into &DA_SCHEMA..PGM_AUDT_CNTS
		select da_run_id, pgm_audt_cnt_id, nullif(submtg_state_cd, 'xx'),audt_cnt_val from row_count&j.
		%if &rtcnt. > 1 %then %do;
			where submtg_state_cd <> 'xx'
		%end;
		order by audt_cnt_val desc
		 
		
     )by tmsis_passthrough;

	 %DROP_temp_tables(row_count&j.);
	 
 %end;

%mend getcount_clms;

%macro AWS_MAXID_pull (TMSIS_SCHEMA, table); 

select cats("'",submtg_state_cd,"'") into :CUTOVER_FILTER
separated by ','
from
(select * from connection to tmsis_passthrough
(
select * from &da_schema..state_tmsis_cutovr_dt
where &TAF_FILE_DATE >= cast(tmsis_cutovr_dt as integer) 
));

%global RUN_IDS STATE_IDS combined_list;
	select tmsis_run_id, submtg_state_cd, cats("('",submtg_state_cd,"',",tmsis_run_id,")")
	     into :run_ids separated by ' ',
		      :state_ids separated by ' ',
			  :combined_list separated by ','
	from connection to tmsis_passthrough
	(select 
		 submtg_state_cd
		,max(tmsis_run_id) as tmsis_run_id
	from &TMSIS_SCHEMA..&table.
	where job_stus = 'success'

	     %if %sysfunc(FIND(&ST_FILTER,%str(ALL))) = 0 %then %do;
         and &ST_FILTER
	   %end;

	   and submtg_state_cd in(&CUTOVER_FILTER)

	   and submtg_state_cd <> '96'

	   and submtg_state_cd <> '94'

	group by submtg_state_cd)
    order by submtg_state_cd;

%put run_ids = &run_ids;
%put state_ids = &state_ids;
%put combined_list = &combined_list;
%mend AWS_MAXID_pull;

/* pull final action claims from claims family tables and join to header records */
%macro AWS_Claims_Family_Table_Link (TMSIS_SCHEMA, tab_no, _2x_segment, fl, analysis_date);

	/* Subset Header File applying inclusion and exclusion rules					    */
	/* For IP use, create a flag indicating presence/absence of discharge date in claim */ 
    /* For OT use, create a flag indicating absence of both beg/end service date in claim */
	execute (

	create temp table HEADER_&fl
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as
    
    select	%&tab_no
		%if &fl=IP %then %do;
		    ,case when &analysis_date is NULL then 1 
		        else 0 
			   end as NO_DISCH_DT
		%end;
		%if &fl=OTHR_TOC %then %do;
		    ,case when &analysis_date is NULL and a.SRVC_BGNNG_DT is null then 1 
		        else 0 
			   end as NO_SRVC_DT
		%end;

	/** Join header to usable file **/
    from 	&TMSIS_SCHEMA..&_2x_segment A
	
	where  	%if &fl=OTHR_TOC %then %do;
			((date_part_year (nvl(&analysis_date,a.SRVC_BGNNG_DT)) = &rep_yr and	/* analysis date year is ref year and */
			date_part (month, nvl(&analysis_date,a.SRVC_BGNNG_DT)) = &rep_mo)		/* analysis date month is ref month */
			 or (&analysis_date is null AND a.SRVC_BGNNG_DT is null)                /* or missing both */
			%end; %else
			%do;
		    ((date_part_year (&analysis_date) = &rep_yr and	                	/* analysis date year is ref year and */
			date_part (month, &analysis_date) = &rep_mo)  						/* analysis date month is ref month */
			%end;
			%if &fl=IP %then %do;
			or 
			&analysis_date is NULL 												/* for IP, include NULL discharge dates */
			%end; 
			)
			and                    	
	         A.TMSIS_ACTV_IND = 1 and										    /* record is active and */
			(upper(A.CLM_STUS_CTGRY_CD) <> 'F2' or A.CLM_STUS_CTGRY_CD is null) and  	/* claim status is not denied or null and */
            (upper(A.CLM_TYPE_CD) <> 'Z' or A.CLM_TYPE_CD is null) and					/* claim type is not Z or null and */
			(A.CLM_DND_IND <> '0' or A.CLM_DND_IND is null) and					/* claim denied indicator is not 0 or null */
			(A.CLM_STUS_CD NOT IN('26','87','026','087','542','585','654') or A.CLM_STUS_CD is null) and /* claim status code is not 26, 87, 542, 585, or 654 */
			(A.ADJSTMT_IND <> '1' or A.ADJSTMT_IND IS NULL) 	

          /* state level filters */
			%if &FL=OTHR_TOC %then %do; /* For OT it's state by state */
			and a.submtg_state_cd = %nrbquote('&STATE_ID')
			and a.tmsis_run_id = &RUN_ID %end;
			%else %do; /* For all other states we do all included states at once */
            and (a.submtg_state_cd,a.tmsis_run_id) in (&combined_list)
			%end;
    
	) by tmsis_passthrough;


	execute (

	create temp table HEADER2_&fl
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as
    
    select	A.TMSIS_RUN_ID
		 	,A.ORGNL_CLM_NUM
		 	,A.ADJSTMT_CLM_NUM
		 	,A.SUBMTG_STATE_CD
		 	,A.ADJDCTN_DT
		 	,A.ADJSTMT_IND 
		%if &fl=IP %then %do;
		    ,A.NO_DISCH_DT
		%end;
		%if &fl=OTHR_TOC %then %do;
		    ,A.NO_SRVC_DT
		%end;
		

    from 	HEADER_&FL A 

	group by A.TMSIS_RUN_ID,A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM,A.ADJSTMT_CLM_NUM,A.ADJDCTN_DT,A.ADJSTMT_IND
			 %if &fl=IP %then %do;
			 ,no_disch_dt 
			 %end;
			 %if &fl=OTHR_TOC %then %do;
			 ,NO_SRVC_DT 
			 %end;
    having count(A.TMSIS_RUN_ID) = 1

    ) by tmsis_passthrough;


/********************************************************************************************************************************************/
/* create subset of records without discharge dates in prep for joining with line file to see which ones have a corresponding svc end date  */
/* this is only done for IP																													*/
/********************************************************************************************************************************************/
	%if &fl in(IP OTHR_TOC) %then %do;
	execute (

 	create temp TABLE NO_DISCHARGE_DATES
    distkey(ORGNL_CLM_NUM) 
    sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND) 
	as 
	with 	MAX_DATES as
			(select H.TMSIS_RUN_ID
		 			,H.ORGNL_CLM_NUM
		 			,H.ADJSTMT_CLM_NUM
		 			,H.SUBMTG_STATE_CD
		 			,H.ADJDCTN_DT
		 			,H.ADJSTMT_IND 
					,MAX(L.SRVC_ENDG_DT) as SRVC_ENDG_DT
					,MAX(L.SRVC_BGNNG_DT) as SRVC_BGNNG_DT 
					
  					from  HEADER2_&fl H inner join &TMSIS_SCHEMA..TMSIS_CLL_REC_&fl L   							

  					on    H.TMSIS_RUN_ID = L.TMSIS_RUN_ID and
	      				  H.SUBMTG_STATE_CD = L.SUBMTG_STATE_CD and 
		  				  H.ORGNL_CLM_NUM = upper(coalesce(L.ORGNL_CLM_NUM,'~')) and 
		  				  H.ADJSTMT_CLM_NUM = upper(coalesce(L.ADJSTMT_CLM_NUM,'~')) and
		  				  H.ADJDCTN_DT = coalesce(L.ADJDCTN_DT,'01JAN1960') and
		  				  H.ADJSTMT_IND = upper(coalesce(L.LINE_ADJSTMT_IND,'X'))

					/* Exclude records with missing end date, and keep TMSIS active and where header is missing disch */
					where L.TMSIS_ACTV_IND = 1 and

					 %if &FL=IP %then %do;
		   				  coalesce(L.SRVC_ENDG_DT, L.SRVC_BGNNG_DT) is not NULL and
		   				  H.NO_DISCH_DT = 1  %end; %else
					 %if &FL=OTHR_TOC %then %do;
                          L.SRVC_ENDG_DT is not null and
						  H.NO_SRVC_DT = 1 						  
					 %end;


		          /* STATE LEVEL FILTERS */
					%IF &FL=OTHR_TOC %THEN %DO; /* FOR OT IT'S STATE BY STATE */
					AND L.SUBMTG_STATE_CD = %NRBQUOTE('&STATE_ID')
					AND L.TMSIS_RUN_ID = &RUN_ID %END;
					%ELSE %DO; /* FOR ALL OTHER STATES WE DO ALL INCLUDED STATES AT ONCE */
		            AND (L.SUBMTG_STATE_CD,L.TMSIS_RUN_ID) IN (&COMBINED_LIST)
					%END;	


					group by H.TMSIS_RUN_ID,H.ORGNL_CLM_NUM,H.ADJSTMT_CLM_NUM,
							 H.SUBMTG_STATE_CD,H.ADJDCTN_DT,H.ADJSTMT_IND  
				)
	select	*
	from	MAX_DATES
	where	
	   %if &FL=IP %then %do;
		   (date_part (year, coalesce(SRVC_ENDG_DT, SRVC_BGNNG_DT)) = &rep_yr and	    /* max svc ending date year is ref year and */
			date_part (month, coalesce(SRVC_ENDG_DT, SRVC_BGNNG_DT)) = &rep_mo)  		/* max svc ending date month is ref month */
	   %end; %else 
	   %if &FL=OTHR_TOC %then %do;
		    ((date_part_year (SRVC_ENDG_DT) = &rep_yr and	                	/* analysis date year is ref year and */
			date_part (month, SRVC_ENDG_DT) = &rep_mo))  						/* analysis date month is ref month */
       %end;
	;
	) by tmsis_passthrough;

	%end;


/********************************************************************************************************************************************/
/* create file containing primary keys to be used in joining with claims family table to pick up value of final action indicator.			*/
/* output only contains records where final action indicator=1.  For IP, source of keys is the file with discharge dates and file with      */
/* discharge dates joined with line file to determine if there is a corresponding svc date													*/
/********************************************************************************************************************************************/
    execute (
    
	create temp TABLE CLM_FMLY_&FL 
	distkey(ORGNL_CLM_NUM)
    sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND) as

	select TMSIS_RUN_ID
		 			,coalesce(upper(ORGNL_CLM_NUM), '~') as ORGNL_CLM_NUM
		 			,coalesce(upper(ADJSTMT_CLM_NUM), '~') as ADJSTMT_CLM_NUM
		 			,SUBMTG_STATE_CD
		 			,coalesce(ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT
					,COALESCE(ADJSTMT_IND,'X') as ADJSTMT_IND
	from &TMSIS_SCHEMA..TMSIS_CLM_FMLY_&fl F
	/** Limit to final action claims in family table **/
    where CLM_FMLY_FINL_ACTN_IND = 1	 
	          /* state level filters */
			%if &FL=OTHR_TOC %then %do; /* For OT it's state by state */
			and F.submtg_state_cd = %nrbquote('&STATE_ID')
			and F.tmsis_run_id = &RUN_ID %end;
			%else %do; /* For all other states we do all included states at once */
            and (F.submtg_state_cd,F.tmsis_run_id) in (&combined_list)
			%end;
    group by 1,2,3,4,5,6
	having count(TMSIS_RUN_ID)=1
	) by tmsis_passthrough;

	%if &fl in(IP OTHR_TOC) %then %do;
	execute (
    create temp TABLE COMBINED_HEADER distkey(ORGNL_CLM_NUM) 
    sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND) as 
     (select A.TMSIS_RUN_ID ,A.ORGNL_CLM_NUM ,A.ADJSTMT_CLM_NUM ,A.SUBMTG_STATE_CD ,A.ADJDCTN_DT 
            ,A.ADJSTMT_IND 
			,null as SRVC_ENDG_DT_DRVD_L
			,null as SRVC_ENDG_DT_CD_L
     from HEADER2_&FL A 
	 where
	 %if &FL=IP %then %do; A.NO_DISCH_DT = 0 %end;
            %else %if &FL=OTHR_TOC %then %do; A.NO_SRVC_DT = 0 %end;
      union all 
      select B.TMSIS_RUN_ID ,B.ORGNL_CLM_NUM ,B.ADJSTMT_CLM_NUM ,B.SUBMTG_STATE_CD ,B.ADJDCTN_DT 
            ,B.ADJSTMT_IND 
			,case when nullif(B.SRVC_ENDG_DT,'01JAN1960') is null or SRVC_ENDG_DT is null then SRVC_BGNNG_DT
				  else SRVC_ENDG_DT end as SRVC_ENDG_DT_DRVD_L
			,case when nullif(B.SRVC_ENDG_DT,'01JAN1960') is not null and SRVC_ENDG_DT is not null then '4'
				  when nullif(B.SRVC_BGNNG_DT,'01JAN1960') is not null and SRVC_BGNNG_DT is not null then '5' 
				  else null
			 end  as SRVC_ENDG_DT_CD_L
     from NO_DISCHARGE_DATES B )

	) by tmsis_passthrough;
	%end;

    execute (

    create temp TABLE ALL_HEADER_&FL.
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as

	select 	H.*
    %if &fl in(IP OTHR_TOC) %then %do;
	from   COMBINED_HEADER H
    %end; %else
	%do;
	from   HEADER2_&fl H
	%end;
	/** Join to limited and de-duped HEADER table to CLAIM FAMILY TABLE **/
	inner join CLM_FMLY_&FL F
	on H.ORGNL_CLM_NUM = F.ORGNL_CLM_NUM
	and H.ADJSTMT_CLM_NUM = F.ADJSTMT_CLM_NUM
	and H.ADJDCTN_DT =F.ADJDCTN_DT
	and H.ADJSTMT_IND = F.ADJSTMT_IND
	and H.SUBMTG_STATE_CD = F.SUBMTG_STATE_CD
	and H.TMSIS_RUN_ID = F.TMSIS_RUN_ID


	) by tmsis_passthrough;

%DROP_temp_tables(HEADER2_&FL);
%DROP_temp_tables(CLM_FMLY_&FL);
    %if &fl in(IP OTHR_TOC) %then %do;
	%DROP_temp_tables(COMBINED_HEADER);
    %end; 
/********************************************************************************************************************************************/
/* merge back the remaining header finder file that has been linked with the claims family table with the main header file to pick up the   */
/* remaining data elements from the header file. Output of this step shd contain header records to be kept in final file					*/
/********************************************************************************************************************************************/
	execute (
    create temp TABLE FA_HDR_&FL. 
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)

	as

    select A.*
			,h.submtg_state_cd as new_submtg_state_cd
		%if &fl in(IP OTHR_TOC) %then %do;
			,coalesce(A.SRVC_ENDG_DT_DRVD_H,H.SRVC_ENDG_DT_DRVD_L) as SRVC_ENDG_DT_DRVD
			,coalesce(A.SRVC_ENDG_DT_CD_H,H.SRVC_ENDG_DT_CD_L) as SRVC_ENDG_DT_CD
		%end;
	from ALL_HEADER_&FL. H 

	/** Join de-duped HEADER finder file to T-MSIS HEADER FILE **/
	inner join HEADER_&FL A
	on 		H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD and
	   		H.TMSIS_RUN_ID = A.TMSIS_RUN_ID and
	      	H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM and
		  	H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM and
		  	H.ADJDCTN_DT = A.ADJDCTN_DT and
	  	  	H.ADJSTMT_IND = A.ADJSTMT_IND

	) by tmsis_passthrough;

%DROP_temp_tables(HEADER_&FL);
%DROP_temp_tables(ALL_HEADER_&FL);

   CREATE TABLE HEADER_COUNT AS
   select * from connection to tmsis_passthrough(   
      
      SELECT COUNT(TMSIS_RUN_ID) AS HDR_ROW_CNT 
      FROM FA_HDR_&FL.       
    ) ; 	


	%Get_Audt_counts_clms(&DA_SCHEMA.,&DA_RUN_ID., AWS_Shared_Macros, 0.3 Final action headers);
%if &FL = IP %then
%do;
    %Get_Audt_counts_clms(&DA_SCHEMA.,&DA_RUN_ID., AWS_Shared_Macros, 0.4 No Discharge Dates);
%end;

    %if &fl in(IP OTHR_TOC) %then %do;
	%DROP_temp_tables(NO_DISCHARGE_DATES);
    %end; 

%MEND AWS_Claims_Family_Table_Link;


%macro var_set_type1 (var,upper=NO,lpad=0,new=NO);

 %if &lpad=0 %then %do;
 trim(
	%if &upper=YES %then %do;
		upper(
	%end;
		&var
	%if &upper=YES %then %do;
		)
	%end;
	) 
 %end; %else
 %do;
 case when &var is NOT NULL then 
 	%if &upper=YES %then %do;
		upper(
	%end;
		lpad(trim(&var),&lpad,'0')
	%if &upper=YES %then %do;
		)
	%end;
	else NULL
 end 
 %end;
	 as %if &new=NO %then %do; 
					  &var 
				  	%end; %else %do; 
				  	  &new
				  	%end;

%mend var_set_type1;

            
%macro var_set_type2 (var, lpad, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, cond7=@, cond8=@, cond9=@, cond10=@);

 case when &var is NOT NULL and
           %if &lpad>0 %then %do;
 			lpad(trim(&var),&lpad,'0')
			%end; %else %do;
			trim(&var) 
			%end;
			in (%nrbquote('&cond1')
 	   %if "&cond2" ne "@" %then %do; , %nrbquote('&cond2') %end; %else %goto skip2;
	   %if "&cond3" ne "@" %then %do; , %nrbquote('&cond3') %end; %else %goto skip2;
	   %if "&cond4" ne "@" %then %do; , %nrbquote('&cond4') %end; %else %goto skip2;
 	   %if "&cond5" ne "@" %then %do; , %nrbquote('&cond5') %end; %else %goto skip2;
	   %if "&cond6" ne "@" %then %do; , %nrbquote('&cond6') %end; %else %goto skip2;
	   %if "&cond7" ne "@" %then %do; , %nrbquote('&cond7') %end; %else %goto skip2;
 	   %if "&cond8" ne "@" %then %do; , %nrbquote('&cond8') %end; %else %goto skip2;
	   %if "&cond9" ne "@" %then %do; , %nrbquote('&cond9') %end; %else %goto skip2;
	   %if "&cond10" ne "@" %then %do; , %nrbquote('&cond10') %end; %else %goto skip2;
	   %skip2:
	   ) then 
		 %if &lpad>0 %then %do;
		 lpad(trim(&var),&lpad,'0')
		 %end; %else %do;
		 trim(&var)
		 %end;
	   else NULL
  end as &var
	   
%mend var_set_type2;


%macro var_set_type3 (var, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, spaces=YES, new=NO);

 case when &var in (%nrbquote('&cond1')
 	   %if "&cond2" ne "@" %then %do; , %nrbquote('&cond2') %end; %else %goto skip3;
	   %if "&cond3" ne "@" %then %do; , %nrbquote('&cond3') %end; %else %goto skip3;
	   %if "&cond4" ne "@" %then %do; , %nrbquote('&cond4') %end; %else %goto skip3;
 	   %if "&cond5" ne "@" %then %do; , %nrbquote('&cond5') %end; %else %goto skip3;
	   %if "&cond6" ne "@" %then %do; , %nrbquote('&cond6') %end; %else %goto skip3;
	   %skip3:
	   )
	   %if &spaces=YES %then %do;
	   	 or nullif(trim(%nrbquote('&cond1')),'') = NULL
 	     %if "&cond2" ne "@" %then %do; or nullif(trim(%nrbquote('&cond2')),'') = NULL %end; %else %goto skip3a;
	     %if "&cond3" ne "@" %then %do; or nullif(trim(%nrbquote('&cond3')),'') = NULL %end; %else %goto skip3a;
	     %if "&cond4" ne "@" %then %do; or nullif(trim(%nrbquote('&cond4')),'') = NULL %end; %else %goto skip3a;
 	     %if "&cond5" ne "@" %then %do; or nullif(trim(%nrbquote('&cond5')),'') = NULL %end; %else %goto skip3a;
	     %if "&cond6" ne "@" %then %do; or nullif(trim(%nrbquote('&cond6')),'') = NULL %end; %else %goto skip3a;
	     %skip3a:
	   %end;
	   then NULL
	   else &var
  end as %if &new=NO %then %do; 
					  &var 
				  %end; %else %do; 
				  	  &new
				  %end;
	   
%mend var_set_type3;


%macro var_set_type4 (var, upper, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, cond7=@, cond8=@, cond9=@, cond10=@);

 case when %if &upper=YES %then %do;
 			trim(upper(&var))
			%end; %else %do;
			trim(&var)
			%end;
			in (%nrbquote('&cond1')
 	   %if "&cond2" ne "@" %then %do; , %nrbquote('&cond2') %end; %else %goto skip4;
	   %if "&cond3" ne "@" %then %do; , %nrbquote('&cond3') %end; %else %goto skip4;
	   %if "&cond4" ne "@" %then %do; , %nrbquote('&cond4') %end; %else %goto skip4;
 	   %if "&cond5" ne "@" %then %do; , %nrbquote('&cond5') %end; %else %goto skip4;
	   %if "&cond6" ne "@" %then %do; , %nrbquote('&cond6') %end; %else %goto skip4;
	   %if "&cond7" ne "@" %then %do; , %nrbquote('&cond7') %end; %else %goto skip4;
 	   %if "&cond8" ne "@" %then %do; , %nrbquote('&cond8') %end; %else %goto skip4;
	   %if "&cond9" ne "@" %then %do; , %nrbquote('&cond9') %end; %else %goto skip4;
	   %if "&cond10" ne "@" %then %do; , %nrbquote('&cond10') %end; %else %goto skip4;
	   %skip4:
	   ) then 
		 %if &upper=YES %then %do;
		 trim(upper(&var))
		 %end; %else %do;
		 trim(&var)
		 %end;
	   else NULL
	end as &var
	   
%mend var_set_type4;

%macro var_set_type5 (var,lpad=2,lowerbound=1,upperbound=10,multiple_condition=NO);
case when &var is not NULL and regexp_count(lpad(&var,&lpad,'0'),%nrbquote('[0-9]{&lpad}')) > 0 then 
	 case when (&var::smallint >= &lowerbound and &var::smallint <= &upperbound) then lpad(&var,&lpad,'0')
	      else NULL
	 end
	 else NULL
end  
	 %if &multiple_condition=NO %then %do;
	 as &var
	 %end; %else
	 %if &multiple_condition=YES %then %do;
	 end as &var
	 %end;
%mend var_set_type5;
%macro var_set_type6 (var, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, new=NO);

 case when &var in (&cond1
 	   %if "&cond2" ne "@" %then %do; , &cond2 %end; %else %goto skip6;
	   %if "&cond3" ne "@" %then %do; , &cond3 %end; %else %goto skip6;
	   %if "&cond4" ne "@" %then %do; , &cond4 %end; %else %goto skip6;
 	   %if "&cond5" ne "@" %then %do; , &cond5 %end; %else %goto skip6;
	   %if "&cond6" ne "@" %then %do; , &cond6 %end; %else %goto skip6;
	   %skip6:
	   ) then NULL
	   else &var
  end as %if &new=NO %then %do; 
					  &var 
				  %end; %else %do; 
				  	  &new
				  %end;
	   
%mend var_set_type6;

%macro var_set_proc (var);

case when lpad(&var,2,'0') in('01','02','06','07','10','11','12','13','14','15','16','17','18','19',
										'20','21','22','23','24','25','26','27','28','29','30','31','32','33',
										'34','35','36','37','38','39','40','41','42','43','44','45','46','47',
										'48','49','50','51','52','53','54','55','56','57','58','59','60','61',
										'62','63','64','65','66','67','68','69','70','71','72','73','74','75',
										'76','77','78','79','80','81','82','83','84','85','86','87') then lpad(&var,2,'0')
     else NULL
 end as &var 

%mend var_set_proc;

%macro var_set_ptstatus (var);

case when lpad(&var,2,'0') in('01','02','03','04','05','06','07','08','09',
							  '20','21','30','40','41','42','43','50','51',
							  '61','62','63','64','65','66','69','70','71',
							  '72','81','82','83','84','85','86','87','88',
							  '89','90','91','92','93','94','95') then lpad(&var,2,'0')
     else NULL
 end as &var 

%mend var_set_ptstatus;


%macro var_set_tos (var);

case when regexp_count(lpad(&var,3,'0'),'[0-9]{3}') > 0 then 
	 case when ((&var::smallint >= 1 and &var::smallint <= 93) or
			    (&var::smallint in (115,119,120,121,122,123,127,131,134,135,136,137,138,139,140,141,142,143,144,145,146))) 
		  then lpad(&var,3,'0')
	 end
	 else NULL
end  as &var

%mend var_set_tos;

%macro var_set_prtype (var);

case when regexp_count(lpad(&var,2,'0'),'[0-9]{2}') > 0 then 
	 case when (&var::smallint >= 1 and &var::smallint <= 57) then lpad(&var,2,'0')
	      else NULL
	 end
	 else NULL
end  as &var
%mend var_set_prtype;


%macro var_set_spclty (var);

case when regexp_count(lpad(&var,2,'0'),'[0-9]{2}') > 0 then 
	 case when (&var::smallint >= 1 and &var::smallint <= 98) then upper(lpad(&var,2,'0'))
	      else NULL
	 end
	 else case when upper(&var) in ('A0','A1','A2','A3','A4','A5','A6','A7','A8','A9','B1','B2','B3','B4','B5') then upper(lpad(&var,2,'0'))
			   else NULL
	 end
end  as &var

%mend var_set_spclty;


%macro var_set_poa (var);

case when (upper(&var) in ('Y','N','U','W','1')) then upper(&var)
	else NULL
 end as &var

%mend var_set_poa;


%macro var_set_fills (var, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, spaces=YES, new=NO);

 case when
	   (regexp_count(trim(&var.),%nrbquote('[^&cond1.]+')) = 0
 	   %if "&cond2" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond2.]+')) = 0 %end; %else %goto skip6;
	   %if "&cond3" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond3.]+')) = 0 %end; %else %goto skip6;
	   %if "&cond4" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond4.]+')) = 0 %end; %else %goto skip6;
 	   %if "&cond5" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond5.]+')) = 0 %end; %else %goto skip6;
	   %if "&cond6" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond6.]+')) = 0 %end; %else %goto skip6;
	   %skip6:
	   ) %if &spaces. = YES %then %do; or nullif(trim(&var.),'')=null %end; then NULL
	   else &var
  end as %if &new=NO %then %do; 
					  &var 
				  %end; %else %do; 
				  	  &new
				  %end;
	   
%mend var_set_fills;


%macro var_set_fillpr (var, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, spaces=YES, new=NO);

 case when &var. = '0.00' or
       (regexp_count(trim(&var.),%nrbquote('[^&cond1.]+')) = 0
 	   %if "&cond2" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond2.]+')) = 0 %end; %else %goto skip6;
	   %if "&cond3" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond3.]+')) = 0 %end; %else %goto skip6;
	   %if "&cond4" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond4.]+')) = 0 %end; %else %goto skip6;
 	   %if "&cond5" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond5.]+')) = 0 %end; %else %goto skip6;
	   %if "&cond6" ne "@" %then %do; or regexp_count(trim(&var.),%nrbquote('[^&cond6.]+')) = 0 %end; %else %goto skip6;
	   %skip6:
	   ) %if &spaces. = YES %then %do; or nullif(trim(&var.),'')=null %end; then NULL
	   else &var
  end as %if &new=NO %then %do; 
					  &var 
				  %end; %else %do; 
				  	  &new
				  %end;
	   
%mend var_set_fillpr;
%macro var_set_rsn (var);

case when regexp_count(lpad(&var,3,'0'),'[0-9]{3}') > 0 then 
	 case when (&var::smallint >= 1 and &var::smallint <= 99) then upper(lpad(&var,3,'0'))
	      else &var
	 end
	 else &var
end  as &var

%mend var_set_rsn;


%macro var_set_taxo (var, cond1=@, cond2=@, cond3=@, cond4=@, cond5=@, cond6=@, cond7=@, spaces=YES, new=NO);

 case when regexp_count(trim(&var.),%nrbquote('[^0]+')) = 0 
       or
       &var in (%nrbquote('&cond1')
 	   %if "&cond2" ne "@" %then %do; , %nrbquote('&cond2') %end; %else %goto skipx;
	   %if "&cond3" ne "@" %then %do; , %nrbquote('&cond3') %end; %else %goto skipx;
	   %if "&cond4" ne "@" %then %do; , %nrbquote('&cond4') %end; %else %goto skipx;
 	   %if "&cond5" ne "@" %then %do; , %nrbquote('&cond5') %end; %else %goto skipx;
	   %if "&cond6" ne "@" %then %do; , %nrbquote('&cond6') %end; %else %goto skipx;
	   %if "&cond7" ne "@" %then %do; , %nrbquote('&cond7') %end; %else %goto skipx;
	   %skipx:
	   )
	   %if &spaces=YES %then %do;
	   	 or nullif(trim(%nrbquote('&cond1')),'') = NULL
 	     %if "&cond2" ne "@" %then %do; or nullif(trim(%nrbquote('&cond2')),'') = NULL %end; %else %goto skipxa;
	     %if "&cond3" ne "@" %then %do; or nullif(trim(%nrbquote('&cond3')),'') = NULL %end; %else %goto skipxa;
		 %if "&cond4" ne "@" %then %do; or nullif(trim(%nrbquote('&cond4')),'') = NULL %end; %else %goto skipxa;
 	     %if "&cond5" ne "@" %then %do; or nullif(trim(%nrbquote('&cond5')),'') = NULL %end; %else %goto skipxa;
	     %if "&cond6" ne "@" %then %do; or nullif(trim(%nrbquote('&cond6')),'') = NULL %end; %else %goto skipxa;
	     %if "&cond7" ne "@" %then %do; or nullif(trim(%nrbquote('&cond7')),'') = NULL %end; %else %goto skipxa;
	     %skipxa:
	   %end;
	   then NULL
	   else &var
  end as %if &new=NO %then %do; 
					  &var 
				  %end; %else %do; 
				  	  &new
				  %end;
	   
%mend var_set_taxo;

%macro fix_old_dates(date_var);

   case when date_cmp(&date_var,'1600-01-01')=-1 then '1599-12-31'::date else &date_var end as &date_var

%mend fix_old_dates;

%macro Prep_Formats (fmtname);

	proc format library=library cntlout=&fmtname (where=(fmtname="&fmtname") keep=fmtname start label);
	run;
	%redshift_insert (&fmtname);
%mend Prep_Formats;

%let xix_srvc_ctgry_cd_values=
	('001A',
	'001B',
	'001C',
	'001D',
	'002A',
	'002B',
	'002C',  /* DB added 6/7/2021 */
	'003A',
	'003B',
	'004A',
	'004B',
	'004C',
	'005A',
	'005B',
	'005C',
	'005D',
	'006A',
	'006B',
	'0007',
	'07A1',
	'07A2',
	'07A3',
	'07A4',
	'07A5',
	'07A6',
	'0008',
	'009A',
	'009B',
	'0010',
	'0011',
	'0012',
	'0013',
	'0014',
	'0015',
	'0016',
	'017A',
	'017B',
	'17C1',
	'017D',
	'018A',
	'18A1',
	'18A2',
	'18A3',
	'18A4',
	'18A5',    /* DB Added 6/7/2021 */
	'18B1',
	'18B2',
	'018C',
	'018D',
	'018E',
	'019A',
	'019B',
	'019C',
	'019D',
	'0022',
	'023A',
	'023B',
	'024A',
	'024B',
	'0025',
	'0026',
	'0027',
	'0028',
	'0029',
	'0030',
	'0031',
	'0032',
	'0033',
	'0034',
	'034A',
	'0035',
	'0036',
	'0037',
	'0038',
	'0039',
	'0040',
	'0041',
	'0042',
	'0043',
	'0044',
	'0045',   /* added to set 6/9/2020 */
	'0046',	  /* added to set 3/4/2021 */ 
	'46A1',   /* added to set 3/4/2021 */   
	'46A2',   /* added to set 3/4/2021 */   
	'46A3',   /* added to set 3/4/2021 */   
	'46A4',   /* added to set 3/4/2021 */   
	'46A5',   /* added to set 3/4/2021 */ 
	'46A6',   /* added to set 3/4/2021 */   
	'046B',   /* added to set 3/4/2021 */   
	'0049',
	'0050');
	
%let xxi_srvc_ctgry_cd_values=
	('01A',
	'01B',
	'01C',
	'01D',
	'002',
	'003',
	'004',
	'005',
	'006',
	'007',
	'008',
	'08A',
	'009',
	'010',
	'011',
	'012',
	'013',
	'014',
	'015',
	'016',
	'017',
	'018',
	'019',
	'020',
	'021',
	'022',
	'023',
	'024',
	'025',
	'031',
	'032',
	'32A',
	'32B',
	'033',
	'034',
	'035',
	'35A',
	'35B',
	'048',
	'049',
	'050');







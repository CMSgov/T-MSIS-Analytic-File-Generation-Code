/************************************************************************************************************/
/* Program:		AWS_Project_Macros.sas																		*/
/* Author:		Deo S. Bencio																				*/
/* Date:		12/1/2016																					*/
/* Purpose:		Program contains macros that massage data from various segments in T-MSIS					*/
/*				THIS PROGRAM DOES NOT STAND ALONE.                                                      	*/
/*				IP_build.sas - pull program for IP build													*/
/*				LT_build.sas - pull program for LT build													*/
/*				OT_build.sas - pull program for OT build													*/
/*				RX_build.sas - pull program for RX build                                                    */
/*              This program contains the code for the 3 macros:                                            */
/*              1. AWS_MAXID_pull                                                                           */
/*              2. AWS_Claims_Family_Table_Link                                                             */
/*              3. ORDER_VARS                                                                               */ 
/*																											*/
/* Copyright (C) Mathematica Policy Research, Inc.                                                          */
/* This code cannot be copied, distributed or used without the express written permission                   */
/* of Mathematica Policy Research, Inc.                                                                     */ 
/************************************************************************************************************/
options SASTRACE=',,,ds' SASTRACELOC=Saslog nostsuffix dbidirectexec sqlgeneration=dbms msglevel=I sql_ip_trace=source;
options spool;

%macro AWS_MAXID_pull (TMSIS_SCHEMA, table, label); 

 	execute (

	create temp table allrecs as
	select 
		 submtg_state_cd
		,tmsis_rptg_prd
		,tmsis_run_id
		,ssn_ind
	   	from &TMSIS_SCHEMA.&table.
		where tmsis_actv_ind = 1
		and tmsis_rptg_prd is not null
		and tot_rec_cnt > 0
      /*and submtg_state_cd = '05'		for testing use AR data only */
		order by submtg_state_cd, tmsis_rptg_prd, tmsis_run_id

	) by tmsis_passthrough;	


	execute (

	create temp table test1 as
	select 
		 submtg_state_cd
		,tmsis_rptg_prd
		,tmsis_run_id as max_run_id
		,ssn_ind
	from allrecs
	group by submtg_state_cd, tmsis_rptg_prd, tmsis_run_id, ssn_ind
	having tmsis_run_id = max(tmsis_run_id)

    ) by tmsis_passthrough;	


	execute (

	create temp table usable_file_&label. as
	select 
		 distinct(submtg_state_cd)
		 ,max(max_run_id) as tmsis_run_id    /* usable_run_id_&label.  */
		 ,ssn_ind
	from test1
	group by submtg_state_cd, ssn_ind
	order by submtg_state_cd

	) by tmsis_passthrough;

%mend AWS_MAXID_pull;

/* pull final action claims from claims family tables and join to header records */
%macro AWS_Claims_Family_Table_Link (TMSIS_SCHEMA, tab_no, _2x_segment, fl, analysis_date);

    /* SUBSET CLAIMS FAMILY TABLE USING THE MAX RUN ID FILE*/
	execute (

	create temp table CLM_FAMILY_REC_&fl
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as
      
    select
         	A.TMSIS_RUN_ID
         	,A.CLM_FMLY_FINL_ACTN_IND
		 	,COALESCE(A.ORGNL_CLM_NUM,'0') AS ORGNL_CLM_NUM
		 	,COALESCE(A.ADJSTMT_CLM_NUM,'0') AS ADJSTMT_CLM_NUM
		 	,A.SUBMTG_STATE_CD
		 	,COALESCE(A.ADJDCTN_DT,'01JAN1960') AS ADJDCTN_DT
		 	,COALESCE(A.ADJSTMT_IND,'X') AS ADJSTMT_IND         

    from 	&TMSIS_SCHEMA.TMSIS_CLM_FMLY_&FL. A INNER JOIN usable_file_&fl. B 

  	on   	A.SUBMTG_STATE_CD=B.SUBMTG_STATE_CD and
            A.tmsis_run_id=B.tmsis_run_id
	        
	where   A.CLM_FMLY_FINL_ACTN_IND = 1 and
			(A.ADJSTMT_IND <> '1' or ADJSTMT_IND IS NULL)

    group by A.TMSIS_RUN_ID,A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM,A.ADJSTMT_CLM_NUM,A.ADJDCTN_DT,A.ADJSTMT_IND,A.CLM_FMLY_FINL_ACTN_IND
    having count(A.TMSIS_RUN_ID) = 1	
    ;
	) by tmsis_passthrough;


    /* CREATE TABLE OF DUPLICATE HEADERS BASED ON PRIMARY KEYS */
	execute (

    create temp TABLE DUPE_HEADER_&FL.
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as
      
    select
	  	 	A.TMSIS_RUN_ID
			,A.SUBMTG_STATE_CD
			,COALESCE(A.ORGNL_CLM_NUM,'0') AS ORGNL_CLM_NUM
			,COALESCE(A.ADJSTMT_CLM_NUM,'0')  AS ADJSTMT_CLM_NUM
			,COALESCE(A.ADJDCTN_DT,'01JAN1960') AS ADJDCTN_DT
			,COALESCE(A.ADJSTMT_IND,'X') AS ADJSTMT_IND
            ,B.ssn_ind 

    from 	&TMSIS_SCHEMA.&_2x_segment A INNER JOIN usable_file_&fl. B

    on		A.TMSIS_RUN_ID = B.TMSIS_RUN_ID and
	        A.SUBMTG_STATE_CD = B.SUBMTG_STATE_CD

  	where 	A.TMSIS_ACTV_IND = 1 and										    /* record is active and */
			(A.CLM_STUS_CTGRY_CD <> 'F2' or A.CLM_STUS_CTGRY_CD is null) and  	/* claim status is not denied and */
			(A.ADJSTMT_IND <> '1' or A.ADJSTMT_IND IS NULL) and					/* adjustment indicator is not void and */
			date_part_year (&analysis_date) = &rep_yr and	                	/* analysis date year is ref year and */
			date_part (month, &analysis_date) = &rep_mo	                    	/* analysis date month is ref month */
    group by A.TMSIS_RUN_ID,A.SUBMTG_STATE_CD,A.ORGNL_CLM_NUM,A.ADJSTMT_CLM_NUM,A.ADJDCTN_DT,A.ADJSTMT_IND,B.SSN_IND
    having COUNT(A.TMSIS_RUN_ID) > 1	 

	) by tmsis_passthrough;


    /* SUBSET HEADER RECORDS USING USABLE FILE (MAX RUNIDS) */ 
	execute (

    create temp TABLE ALL_HEADER_&FL.
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as
      
    select
	  	 	%&tab_no
            ,b.ssn_ind 

    from 	&TMSIS_SCHEMA.&_2x_segment A INNER JOIN usable_file_&fl. B

    on		A.TMSIS_RUN_ID = B.TMSIS_RUN_ID and
	        A.SUBMTG_STATE_CD = B.SUBMTG_STATE_CD

  	where 	A.TMSIS_ACTV_IND = 1 and										    /* active indicator = 1 and */
		    (A.CLM_STUS_CTGRY_CD <> 'F2' or A.CLM_STUS_CTGRY_CD is null) and	/* claim status is not denied and */
		    (A.ADJSTMT_IND <> '1' or A.ADJSTMT_IND IS NULL) AND					/* adjustment indicator is not void and */
		    date_part_year (&analysis_date) = &rep_yr AND	                    /* analysis date year is ref year and */
			date_part (month, &analysis_date) = &rep_mo		                    /* analysis date month is ref month */
    	 
	) by tmsis_passthrough;


	/* REMOVE HEADER RECORDS THAT HAVE DUPLICATES */
    execute (

    create temp table HEADER_&FL. 
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT,ADJSTMT_IND)
    as
      
    select  A.*             

    from 	ALL_HEADER_&FL. A LEFT JOIN DUPE_HEADER_&FL B

    on		A.TMSIS_RUN_ID = B.TMSIS_RUN_ID and
	        A.SUBMTG_STATE_CD = B.SUBMTG_STATE_CD and
	        A.ORGNL_CLM_NUM = B.ORGNL_CLM_NUM and
			A.ADJSTMT_CLM_NUM = B.ADJSTMT_CLM_NUM and
			A.ADJDCTN_DT = B.ADJDCTN_DT and
			A.ADJSTMT_IND = B.ADJSTMT_IND

	where B.ORGNL_CLM_NUM IS NULL and											/* orig claim num is null and */
	      B.ADJSTMT_CLM_NUM IS NULL and											/* adj claim num is null and */
		  B.ADJDCTN_DT IS NULL and 												/* adj date is null and */
          B.ADJSTMT_IND IS NULL													/* adj ind is null -- so essentially, NOT in dupe file */

	) by tmsis_passthrough; 


    /* JOIN HEADER RECORDS TO THE CLAIMS FAMILY TABLE */
    execute (

    create temp table FA_HDR_&FL. 
    distkey(ORGNL_CLM_NUM)
	sortkey(TMSIS_RUN_ID,SUBMTG_STATE_CD,ORGNL_CLM_NUM,ADJSTMT_CLM_NUM,ADJDCTN_DT)
    as
      
    select
	  	 	A.*

    from 	HEADER_&FL. A inner join CLM_FAMILY_REC_&FL B 

    on		A.ORGNL_CLM_NUM   = B.ORGNL_CLM_NUM   and
        	A.ADJSTMT_CLM_NUM = B.ADJSTMT_CLM_NUM and
        	A.ADJDCTN_DT      = B.ADJDCTN_DT      and
        	A.ADJSTMT_IND     = B.ADJSTMT_IND     and
			A.TMSIS_RUN_ID    = B.TMSIS_RUN_ID
  
	) by tmsis_passthrough;

%MEND AWS_Claims_Family_Table_Link;

/* ORDER THE FINAL DATASET */ 
%MACRO ORDER_VARS(tab_no, TABLE_ORDER);

   EXECUTE (
   CREATE TEMP TABLE &tab_no._&REPORTING_PERIOD._ORD AS
      
     select %&TABLE_ORDER.
             
     from   %if &tab_no=CIP00003 or
				&tab_no=CLT00003 or
				&tab_no=COT00003 %then %do;
				&tab_no._&REPORTING_PERIOD._GROUPER
			%end; %else
			%if &tab_no.=CRX00003 %then %do;
				&tab_no._&REPORTING_PERIOD.
			%end;
     ;
   ) by tmsis_passthrough;  

%MEND ORDER_VARS;




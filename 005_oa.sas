** ========================================================================== 
** program documentation 
** program     : 005_oa.sas
** description : Generate the annual PL segment for Operating Authority
** date        : 09/2019 12/2020
** note        : This program creates a separate Operating Authority table from the arrays in monthly MCP main segment
**               aggregates unique values across the CY year for variables in the array and indicates month.
**               It creates _SPLMTL flag for base.
**               Then inserts Operating Authority records into the permanent TAF table.
** ==========================================================================;

%macro create_OA;
	%create_temp_table(fileseg=MCP, tblname=oa_pl,
		          subcols=%nrbquote( 
									 %monthly_array(WVR_ID_01, nslots=1)
									 %monthly_array(WVR_ID_02, nslots=1)
									 %monthly_array(WVR_ID_03, nslots=1)
									 %monthly_array(WVR_ID_04, nslots=1)
									 ),
		          subcols2=%nrbquote(
									 %monthly_array(WVR_ID_05, nslots=1)
									 %monthly_array(WVR_ID_06, nslots=1)
									 %monthly_array(WVR_ID_07, nslots=1)
									 %monthly_array(WVR_ID_08, nslots=1)
									 %monthly_array(WVR_ID_09, nslots=1)
									 ),
		          subcols3=%nrbquote(
									 %monthly_array(WVR_ID_10, nslots=1)
									 %monthly_array(WVR_ID_11, nslots=1)
									 %monthly_array(WVR_ID_12, nslots=1)
									 %monthly_array(WVR_ID_13, nslots=1)
									 %monthly_array(WVR_ID_14, nslots=1)
									 %monthly_array(WVR_ID_15, nslots=1)
									),
		          subcols4=%nrbquote(
									 %monthly_array(OPRTG_AUTHRTY_01, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_02, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_03, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_04, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_05, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_06, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_07, nslots=1)
									 ),
		          subcols5=%nrbquote(
									 %monthly_array(OPRTG_AUTHRTY_08, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_09, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_10, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_11, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_12, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_13, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_14, nslots=1)
									 %monthly_array(OPRTG_AUTHRTY_15, nslots=1)
									 ) );

									 
/* insert operating authority array elements into OpAuth0 and group values for annual arrays */

  execute (

	create temp table OpAuth0 (
		SUBMTG_STATE_CD varchar(2) 
		,MC_PLAN_ID varchar(12) 
		,WVR_ID varchar(20)
		,OPRTG_AUTHRTY varchar(2)
		,OPRTG_AUTHRTY_FLAG_01 smallint
		,OPRTG_AUTHRTY_FLAG_02 smallint
		,OPRTG_AUTHRTY_FLAG_03 smallint
		,OPRTG_AUTHRTY_FLAG_04 smallint
		,OPRTG_AUTHRTY_FLAG_05 smallint
		,OPRTG_AUTHRTY_FLAG_06 smallint
		,OPRTG_AUTHRTY_FLAG_07 smallint
		,OPRTG_AUTHRTY_FLAG_08 smallint
		,OPRTG_AUTHRTY_FLAG_09 smallint
		,OPRTG_AUTHRTY_FLAG_10 smallint
		,OPRTG_AUTHRTY_FLAG_11 smallint
		,OPRTG_AUTHRTY_FLAG_12 smallint
	);
	
  ) by tmsis_passthrough;

  execute (

	%do m=1 %to 12;
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));

		%do a=1 %to 15;
			%let a=%sysfunc(putn(&a.,z2));

			insert into OpAuth0 
			   (SUBMTG_STATE_CD, MC_PLAN_ID, WVR_ID, OPRTG_AUTHRTY, OPRTG_AUTHRTY_FLAG_&m.) 
			   select SUBMTG_STATE_CD, MC_PLAN_ID, WVR_ID_&a._&m., OPRTG_AUTHRTY_&a._&m., 1 as mnth
					from oa_pl_&year.
					where WVR_ID_&a._&m. is not null or OPRTG_AUTHRTY_&a._&m. is not null ;
		%end;

	%end;
	
  ) by tmsis_passthrough;

  execute (

	create temp table OpAuth1
			 diststyle key distkey(MC_PLAN_ID) as
	  select SUBMTG_STATE_CD, MC_PLAN_ID, WVR_ID, OPRTG_AUTHRTY
			,max(coalesce(OPRTG_AUTHRTY_FLAG_01,0)) as OPRTG_AUTHRTY_FLAG_01
			,max(coalesce(OPRTG_AUTHRTY_FLAG_02,0)) as OPRTG_AUTHRTY_FLAG_02
			,max(coalesce(OPRTG_AUTHRTY_FLAG_03,0)) as OPRTG_AUTHRTY_FLAG_03
			,max(coalesce(OPRTG_AUTHRTY_FLAG_04,0)) as OPRTG_AUTHRTY_FLAG_04
			,max(coalesce(OPRTG_AUTHRTY_FLAG_05,0)) as OPRTG_AUTHRTY_FLAG_05
			,max(coalesce(OPRTG_AUTHRTY_FLAG_06,0)) as OPRTG_AUTHRTY_FLAG_06
			,max(coalesce(OPRTG_AUTHRTY_FLAG_07,0)) as OPRTG_AUTHRTY_FLAG_07
			,max(coalesce(OPRTG_AUTHRTY_FLAG_08,0)) as OPRTG_AUTHRTY_FLAG_08
			,max(coalesce(OPRTG_AUTHRTY_FLAG_09,0)) as OPRTG_AUTHRTY_FLAG_09
			,max(coalesce(OPRTG_AUTHRTY_FLAG_10,0)) as OPRTG_AUTHRTY_FLAG_10
			,max(coalesce(OPRTG_AUTHRTY_FLAG_11,0)) as OPRTG_AUTHRTY_FLAG_11
			,max(coalesce(OPRTG_AUTHRTY_FLAG_12,0)) as OPRTG_AUTHRTY_FLAG_12	  
		 
	  from OpAuth0
	  group by SUBMTG_STATE_CD, MC_PLAN_ID, WVR_ID, OPRTG_AUTHRTY
	  order by SUBMTG_STATE_CD, MC_PLAN_ID, WVR_ID, OPRTG_AUTHRTY;
	  
  ) by tmsis_passthrough;
	                                  
	/* Create temp table with just OPRTG_AUTHRTY_SPLMTL to join to base */

	%create_splmlt (segname=OPRTG_AUTHRTY, segfile=OpAuth1)

	/* Insert into permanent table */

	%macro basecols;
	
			,WVR_ID
			,OPRTG_AUTHRTY
			,OPRTG_AUTHRTY_FLAG_01
			,OPRTG_AUTHRTY_FLAG_02
			,OPRTG_AUTHRTY_FLAG_03
			,OPRTG_AUTHRTY_FLAG_04
			,OPRTG_AUTHRTY_FLAG_05
			,OPRTG_AUTHRTY_FLAG_06
			,OPRTG_AUTHRTY_FLAG_07
			,OPRTG_AUTHRTY_FLAG_08
			,OPRTG_AUTHRTY_FLAG_09
			,OPRTG_AUTHRTY_FLAG_10
			,OPRTG_AUTHRTY_FLAG_11
			,OPRTG_AUTHRTY_FLAG_12

		%mend basecols;

  execute (
		insert into &DA_SCHEMA..TAF_ANN_PL_OA
		(DA_RUN_ID, PL_LINK_KEY, PL_FIL_DT, PL_VRSN, SUBMTG_STATE_CD, MC_PLAN_ID %basecols)
		select 

			%table_id_cols
			%basecols

		from OpAuth1

  ) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(oa_pl_&year. OpAuth0 OpAuth1)          

%mend create_OA;

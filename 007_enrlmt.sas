** ========================================================================== 
** program documentation 
** program     : 007_enrlmt.sas
** description : Generate the annual PR segment for enrollment
** date        : 10/2019 12/2020
** note        : This program aggregates unique values across the CY year for variables in collist.
**               It creates _SPLMTL flag for base.
**               Then inserts enrollment records into the permanent TAF table.
** ==========================================================================;

%macro create_ENRLMT;

	/* Create enrollment segment. Select records and select or create data elements */

	%let collist_e = PRVDR_MDCD_EFCTV_DT
					,PRVDR_MDCD_END_DT
					,PRVDR_MDCD_ENRLMT_STUS_CD
					,STATE_PLAN_ENRLMT_CD
					,PRVDR_MDCD_ENRLMT_MTHD_CD
					,APLCTN_DT
					,PRVDR_MDCD_ENRLMT_STUS_CTGRY;

	%annual_segment(fileseg=ENR, dtfile=PRV, collist=collist_e, mnths=PRVDR_ENRLMT_FLAG, outtbl=enrlmt_pr_&year.);

	/* Create temp table with just ENRLMT_SPLMTL to join to base */

	%create_splmlt (segname=ENRLMT, segfile=enrlmt_pr_&year.)

	/* Insert into permanent table */

		%macro basecols;
	
			,PRVDR_MDCD_EFCTV_DT
			,PRVDR_MDCD_END_DT
			,PRVDR_MDCD_ENRLMT_STUS_CD
			,STATE_PLAN_ENRLMT_CD
			,PRVDR_MDCD_ENRLMT_MTHD_CD
			,APLCTN_DT
			,PRVDR_MDCD_ENRLMT_STUS_CTGRY
			,PRVDR_ENRLMT_FLAG_01
			,PRVDR_ENRLMT_FLAG_02
			,PRVDR_ENRLMT_FLAG_03
			,PRVDR_ENRLMT_FLAG_04
			,PRVDR_ENRLMT_FLAG_05
			,PRVDR_ENRLMT_FLAG_06
			,PRVDR_ENRLMT_FLAG_07
			,PRVDR_ENRLMT_FLAG_08
			,PRVDR_ENRLMT_FLAG_09
			,PRVDR_ENRLMT_FLAG_10
			,PRVDR_ENRLMT_FLAG_11
			,PRVDR_ENRLMT_FLAG_12

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_ENRLMT
		(DA_RUN_ID, PR_LINK_KEY, PR_FIL_DT, PR_VRSN, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID %basecols)
		select 

			%table_id_cols
			%basecols

		from enrlmt_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(enrlmt_pr_&year.)          

%mend create_ENRLMT;

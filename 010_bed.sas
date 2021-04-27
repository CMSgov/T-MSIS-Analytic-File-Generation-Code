** ========================================================================== 
** program documentation 
** program     : 010_bed.sas
** description : Generate the annual PR segment for bed type
** date        : 10/2019 12/2020
** note        : This program aggregates unique values across the CY year for variables in collist.
**               It creates _SPLMTL flag for base.
**               Then inserts bed type records into the permanent TAF table.
** ==========================================================================;

%macro create_BED;

	/* Create bed type segment. Select records and select or create data elements */

	%let collist_s = PRVDR_LCTN_ID, BED_TYPE_CD, BED_CNT;

	%annual_segment(fileseg=BED, dtfile=PRV, collist=collist_s, mnths=PRVDR_BED_FLAG, outtbl=bed_pr_&year.);


	/* Create temp table with just BED_SPLMTL to join to base */

	%create_splmlt (segname=BED, segfile=bed_pr_&year.)

	/* Insert into permanent table */

		%macro basecols;
	
			,PRVDR_LCTN_ID
			,BED_TYPE_CD
			,BED_CNT
			,PRVDR_BED_FLAG_01
			,PRVDR_BED_FLAG_02
			,PRVDR_BED_FLAG_03
			,PRVDR_BED_FLAG_04
			,PRVDR_BED_FLAG_05
			,PRVDR_BED_FLAG_06
			,PRVDR_BED_FLAG_07
			,PRVDR_BED_FLAG_08
			,PRVDR_BED_FLAG_09
			,PRVDR_BED_FLAG_10
			,PRVDR_BED_FLAG_11
			,PRVDR_BED_FLAG_12

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_BED
		(DA_RUN_ID, PR_LOC_LINK_KEY, PR_FIL_DT, PR_VRSN, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID %basecols)
		select 

			%table_id_cols(loctype=2)
			%basecols

		from bed_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(bed_pr_&year.)          

%mend create_BED;

** ========================================================================== 
** program documentation 
** program     : 003_lctn_pr.sas
** description : Generate the annual PR segment for Location addresses
** date        : 10/2019 12/2020
** note        : This program aggregates unique values across the CY year for variables in collist.
**               It creates _SPLMTL flag for base.
**               Then inserts location records into the permanent TAF table.
** ==========================================================================;


%macro create_LCTN;

	/* Create location segment. Select records and select or create data elements */

	%let collist_l = PRVDR_LCTN_ID
					,PRVDR_ADR_BLG_IND
					,PRVDR_ADR_PRCTC_IND
					,PRVDR_ADR_SRVC_IND
					,ADR_LINE_1_TXT
					,ADR_LINE_2_TXT
					,ADR_LINE_3_TXT
					,ADR_CITY_NAME
					,ADR_STATE_CD
					,ADR_ZIP_CD
					,ADR_CNTY_CD
					,ADR_BRDR_STATE_IND
					,PRVDR_SRVC_ST_DFRNT_SUBMTG_ST
					;

	%annual_segment(fileseg=LOC, dtfile=PRV, collist=collist_l, mnths=PRVDR_LCTN_FLAG, outtbl=lctn_pr_&year.);

	/* Create temp table with just LCTN_SPLMTL to join to base */

	%create_splmlt (segname=LCTN, segfile=lctn_pr_&year.)

	/* Insert into permanent table */

		%macro basecols;
	
			,PRVDR_LCTN_ID
			,PRVDR_ADR_BLG_IND
			,PRVDR_ADR_PRCTC_IND
			,PRVDR_ADR_SRVC_IND
			,ADR_LINE_1_TXT
			,ADR_LINE_2_TXT
			,ADR_LINE_3_TXT
			,ADR_CITY_NAME
			,ADR_STATE_CD
			,ADR_ZIP_CD
			,ADR_CNTY_CD
			,ADR_BRDR_STATE_IND
			,PRVDR_SRVC_ST_DFRNT_SUBMTG_ST
			,PRVDR_LCTN_FLAG_01
			,PRVDR_LCTN_FLAG_02
			,PRVDR_LCTN_FLAG_03
			,PRVDR_LCTN_FLAG_04
			,PRVDR_LCTN_FLAG_05
			,PRVDR_LCTN_FLAG_06
			,PRVDR_LCTN_FLAG_07
			,PRVDR_LCTN_FLAG_08
			,PRVDR_LCTN_FLAG_09
			,PRVDR_LCTN_FLAG_10
			,PRVDR_LCTN_FLAG_11
			,PRVDR_LCTN_FLAG_12

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_LCTN
		(DA_RUN_ID, PR_LINK_KEY, PR_LOC_LINK_KEY, PR_FIL_DT, PR_VRSN, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID %basecols)
		select 

			%table_id_cols(loctype=1)
			%basecols

		from lctn_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(lctn_pr_&year.)          

%mend create_LCTN;

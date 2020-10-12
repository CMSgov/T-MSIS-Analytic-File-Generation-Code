/**********************************************************************************************/
/*Program: 006_txnmy.sas
/*modified: Heidi Cohen
/*Date: 10/2019
/*Purpose: Generate the annual PR segment for taxonomy
/*Mod: 
/*Notes: This program aggregates unique values across the CY year for variables in collist.
/*       It creates _SPLMTL flag for base.
/*       It inserts taxonomy records into the permanent TAF table.
/**********************************************************************************************/

%macro create_TXNMY;

	/* Create taxonomy segment. Select records and select or create data elements */

	%let collist_s = PRVDR_CLSFCTN_TYPE_CD
					,PRVDR_CLSFCTN_CD
					;

	%annual_segment(fileseg=TAX, dtfile=PRV, collist=collist_s, mnths=PRVDR_TXNMY_FLAG, outtbl=txnmy_pr_&year.);

	/* Create temp table with just TXNMY_SPLMTL to join to base */

	%create_splmlt (segname=TXNMY, segfile=txnmy_pr_&year.)

	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_TXNMY
		select 

			%table_id_cols
			,PRVDR_CLSFCTN_TYPE_CD
			,PRVDR_CLSFCTN_CD
			,PRVDR_TXNMY_FLAG_01
			,PRVDR_TXNMY_FLAG_02
			,PRVDR_TXNMY_FLAG_03
			,PRVDR_TXNMY_FLAG_04
			,PRVDR_TXNMY_FLAG_05
			,PRVDR_TXNMY_FLAG_06
			,PRVDR_TXNMY_FLAG_07
			,PRVDR_TXNMY_FLAG_08
			,PRVDR_TXNMY_FLAG_09
			,PRVDR_TXNMY_FLAG_10
			,PRVDR_TXNMY_FLAG_11
			,PRVDR_TXNMY_FLAG_12

		from txnmy_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(txnmy_pr_&year.)          

%mend create_TXNMY;

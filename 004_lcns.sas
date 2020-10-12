/**********************************************************************************************/
/*Program: 004_lcns.sas
/*modified: Heidi Cohen
/*Date: 10/2019
/*Purpose: Generate the annual PR segment for license
/*Mod: 
/*Notes: This program aggregates unique values across the CY year for variables in collist.
/*       It creates _SPLMTL flag for base.
/*       It inserts license records into the permanent TAF table.
/**********************************************************************************************/

%macro create_LCNS;

	/* Create license segment. Select records and select or create data elements */

	%let collist_s = PRVDR_LCTN_ID
					,LCNS_TYPE_CD
					,LCNS_OR_ACRDTN_NUM
					,LCNS_ISSG_ENT_ID_TXT;

	%annual_segment(fileseg=LIC, dtfile=PRV, collist=collist_s, mnths=PRVDR_LCNS_FLAG, outtbl=lcns_pr_&year.);

	/* Create temp table with just LCNS_SPLMTL to join to base */

	%create_splmlt (segname=LCNS, segfile=lcns_pr_&year.)

	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_LCNS
		select 

			%table_id_cols(loctype=2)
			,PRVDR_LCTN_ID
			,LCNS_TYPE_CD
			,LCNS_OR_ACRDTN_NUM
			,LCNS_ISSG_ENT_ID_TXT
			,PRVDR_LCNS_FLAG_01
			,PRVDR_LCNS_FLAG_02
			,PRVDR_LCNS_FLAG_03
			,PRVDR_LCNS_FLAG_04
			,PRVDR_LCNS_FLAG_05
			,PRVDR_LCNS_FLAG_06
			,PRVDR_LCNS_FLAG_07
			,PRVDR_LCNS_FLAG_08
			,PRVDR_LCNS_FLAG_09
			,PRVDR_LCNS_FLAG_10
			,PRVDR_LCNS_FLAG_11
			,PRVDR_LCNS_FLAG_12

		from lcns_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(lcns_pr_&year.)          

%mend create_LCNS;

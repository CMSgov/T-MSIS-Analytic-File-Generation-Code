/**********************************************************************************************/
/*Program: 008_grp.sas
/*modified: Heidi Cohen
/*Date: 10/2019
/*Purpose: Generate the annual PR segment for affiliated groups
/*Mod: 
/*Notes: This program aggregates unique values across the CY year for variables in collist.
/*       It creates _SPLMTL flag for base.
/*       It inserts affiliated groups records into the permanent TAF table.
/**********************************************************************************************/

%macro create_GRP;

	/* Create affiliated groups segment. Select records and select or create data elements */

	%let collist_s = SUBMTG_STATE_AFLTD_PRVDR_ID;

	%annual_segment(fileseg=GRP, dtfile=PRV, collist=collist_s, mnths=PRVDR_GRP_FLAG, outtbl=grp_pr_&year.);


	/* Create temp table with just GRP_SPLMTL to join to base */

	%create_splmlt (segname=GRP, segfile=grp_pr_&year.)

	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_GRP
		select 

			%table_id_cols
			,SUBMTG_STATE_AFLTD_PRVDR_ID
			,PRVDR_GRP_FLAG_01
			,PRVDR_GRP_FLAG_02
			,PRVDR_GRP_FLAG_03
			,PRVDR_GRP_FLAG_04
			,PRVDR_GRP_FLAG_05
			,PRVDR_GRP_FLAG_06
			,PRVDR_GRP_FLAG_07
			,PRVDR_GRP_FLAG_08
			,PRVDR_GRP_FLAG_09
			,PRVDR_GRP_FLAG_10
			,PRVDR_GRP_FLAG_11
			,PRVDR_GRP_FLAG_12

		from grp_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(grp_pr_&year.)          

%mend create_GRP;

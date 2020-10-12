/**********************************************************************************************/
/*Program: 009_pgm.sas
/*modified: Heidi Cohen
/*Date: 10/2019
/*Purpose: Generate the annual PR segment for affiliated programs
/*Mod: 
/*Notes: This program aggregates unique values across the CY year for variables in collist.
/*       It creates _SPLMTL flag for base.
/*       It inserts affiliated programs records into the permanent TAF table.
/**********************************************************************************************/

%macro create_PGM;

	/* Create affiliated programs segment. Select records and select or create data elements */

	%let collist_s = AFLTD_PGM_TYPE_CD, AFLTD_PGM_ID;

	%annual_segment(fileseg=PGM, dtfile=PRV, collist=collist_s, mnths=PRVDR_PGM_FLAG, outtbl=pgm_pr_&year.);


	/* Create temp table with just PGM_SPLMTL to join to base */

	%create_splmlt (segname=PGM, segfile=pgm_pr_&year.)

	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_PGM
		select 

			%table_id_cols
			,AFLTD_PGM_TYPE_CD
			,AFLTD_PGM_ID
			,PRVDR_PGM_FLAG_01
			,PRVDR_PGM_FLAG_02
			,PRVDR_PGM_FLAG_03
			,PRVDR_PGM_FLAG_04
			,PRVDR_PGM_FLAG_05
			,PRVDR_PGM_FLAG_06
			,PRVDR_PGM_FLAG_07
			,PRVDR_PGM_FLAG_08
			,PRVDR_PGM_FLAG_09
			,PRVDR_PGM_FLAG_10
			,PRVDR_PGM_FLAG_11
			,PRVDR_PGM_FLAG_12

		from pgm_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(pgm_pr_&year.)          

%mend create_PGM;

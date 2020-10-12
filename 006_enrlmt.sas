/**********************************************************************************************/
/*Program: 006_enrlmt.sas
/*modified: Heidi Cohen
/*Date: 10/2019
/*Purpose: Generate the annual PL segment for Enrollment
/*Mod: 
/*Notes: This program aggregates unique values across the CY year for variables in collist.
/*       It creates _SPLMTL flag for base.
/*       It inserts enrollment records into the permanent TAF table.
/**********************************************************************************************/

%macro create_ENRLMT;

	/* Create enrollment segment. Select records and select or create data elements */

	%let collist_e = MC_PLAN_POP;

	%annual_segment(fileseg=MCE, dtfile=MCP, collist=collist_e, mnths=MC_ENRLMT_FLAG, outtbl=enrlmt_pl_&year.);

	/* Create temp table with just ENRLMT_SPLMTL to join to base */

	%create_splmlt (segname=ENRLMT, segfile=enrlmt_pl_&year.)

	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PL_ENRLMT
		select 

			%table_id_cols
			,MC_PLAN_POP
			,MC_ENRLMT_FLAG_01
			,MC_ENRLMT_FLAG_02
			,MC_ENRLMT_FLAG_03
			,MC_ENRLMT_FLAG_04
			,MC_ENRLMT_FLAG_05
			,MC_ENRLMT_FLAG_06
			,MC_ENRLMT_FLAG_07
			,MC_ENRLMT_FLAG_08
			,MC_ENRLMT_FLAG_09
			,MC_ENRLMT_FLAG_10
			,MC_ENRLMT_FLAG_11
			,MC_ENRLMT_FLAG_12

		from enrlmt_pl_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(enrlmt_pl_&year.)          

%mend create_ENRLMT;

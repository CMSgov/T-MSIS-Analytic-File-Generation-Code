/**********************************************************************************************/
/*Program: 003_lctn.sas
/*modified: Heidi Cohen
/*Date: 10/2019
/*Purpose: Generate the annual PL segment for Location addresses
/*Mod: 
/*Notes: This program aggregates unique values across the CY year for variables in collist.
/*       It creates _SPLMTL flag for base.
/*       It inserts location records into the permanent TAF table.
/**********************************************************************************************/

%macro create_LCTN;

	/* Create location segment. Select records and select or create data elements */

	%let collist_l = MC_LCTN_ID,
					MC_LINE_1_ADR,
					MC_LINE_2_ADR,
					MC_LINE_3_ADR,
					MC_CITY_NAME,
					MC_STATE_CD,
					MC_ZIP_CD,
					MC_CNTY_CD
					;

	%annual_segment(fileseg=MCL, dtfile=MCP, collist=collist_l, mnths=MC_LCTN_FLAG, outtbl=lctn_pl_&year.);


	/* Create temp table with just LCTN_SPLMTL to join to base */

	%create_splmlt (segname=LCTN, segfile=lctn_pl_&year.)

	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PL_LCTN
		select 

			%table_id_cols
			,MC_LCTN_ID
			,MC_LINE_1_ADR
			,MC_LINE_2_ADR
			,MC_LINE_3_ADR
			,MC_CITY_NAME
			,MC_STATE_CD
			,MC_ZIP_CD
			,MC_CNTY_CD
			,MC_LCTN_FLAG_01
			,MC_LCTN_FLAG_02
			,MC_LCTN_FLAG_03
			,MC_LCTN_FLAG_04
			,MC_LCTN_FLAG_05
			,MC_LCTN_FLAG_06
			,MC_LCTN_FLAG_07
			,MC_LCTN_FLAG_08
			,MC_LCTN_FLAG_09
			,MC_LCTN_FLAG_10
			,MC_LCTN_FLAG_11
			,MC_LCTN_FLAG_12

		from lctn_pl_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(lctn_pl_&year.)          

%mend create_LCTN;

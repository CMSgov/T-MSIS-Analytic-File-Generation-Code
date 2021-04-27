** ========================================================================== 
** program documentation 
** program     : 004_sarea.sas
** description : Generate the annual PL segment for Service Area
** date        : 09/2019 12/2020
** note        : This program aggregates unique values across the CY year for variables in collist.
**               It creates _SPLMTL flag for base.
**               Then inserts Service Area records into the permanent TAF table.
** ==========================================================================;

%macro create_SAREA;

	/* Create service area segment. Select records and select or create data elements */

	%let collist_s = MC_SAREA_NAME;

	%annual_segment(fileseg=MCS, dtfile=MCP, collist=collist_s, mnths=MC_SAREA_LCTN_FLAG, outtbl=sarea_pl_&year.);

	/* Create temp table with just SAREA_SPLMTL to join to base */

	%create_splmlt (segname=SAREA, segfile=sarea_pl_&year.)

	/* Insert into permanent table */

	%macro basecols;
	
			,MC_SAREA_NAME
			,MC_SAREA_LCTN_FLAG_01
			,MC_SAREA_LCTN_FLAG_02
			,MC_SAREA_LCTN_FLAG_03
			,MC_SAREA_LCTN_FLAG_04
			,MC_SAREA_LCTN_FLAG_05
			,MC_SAREA_LCTN_FLAG_06
			,MC_SAREA_LCTN_FLAG_07
			,MC_SAREA_LCTN_FLAG_08
			,MC_SAREA_LCTN_FLAG_09
			,MC_SAREA_LCTN_FLAG_10
			,MC_SAREA_LCTN_FLAG_11
			,MC_SAREA_LCTN_FLAG_12

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PL_SAREA
		(DA_RUN_ID, PL_LINK_KEY, PL_FIL_DT, PL_VRSN, SUBMTG_STATE_CD, MC_PLAN_ID %basecols)
		select 

			%table_id_cols
			%basecols

		from sarea_pl_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(sarea_pl_&year.)          

%mend create_SAREA;

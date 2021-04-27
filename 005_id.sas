** ========================================================================== 
** program documentation 
** program     : 005_id.sas
** description : Generate the annual PR segment for identifiers
** date        : 10/2019 12/2020
** note        : This program aggregates unique values across the CY year for variables in collist.
**               It creates _SPLMTL flag for base.
**               Then inserts identifiers records into the permanent TAF table.
**               A separate table with NPI information is also created which has one record per 
**               provider(submtg_state_cd, submtg_state_prvdr_id)and is used 
**               to linking prvdr_npi_01 prvdr_npi_02 prvdr_npi_cnt back to the PR base segment.
** ==========================================================================;

%macro create_ID;

	/* Create identifiers segment. Select records and select or create data elements */

	%let collist_s = PRVDR_LCTN_ID
					,PRVDR_ID_TYPE_CD
					,PRVDR_ID
					,PRVDR_ID_ISSG_ENT_ID_TXT
					;

	%annual_segment(fileseg=IDT, dtfile=PRV, collist=collist_s, mnths=PRVDR_ID_FLAG, outtbl=id_pr_&year.);

	/* create table with prvdr_npi_01 prvdr_npi_02 prvdr_npi_cnt with 
	   one record per provider(submtg_state_cd, submtg_state_prvdr_id)
	   which link to PR base */
	
	/* select all monthly taf_prv_idt records with prvdr_id_type_cd='2' - NPI - and prvdr_id is not null for the link back to PR base*/

	execute (
		create temp table npi_main as
		select b.submtg_state_cd, b.submtg_state_prvdr_id, b.prvdr_id, substring(a.prv_fil_dt,5,2) as month
				from max_run_id_prv_&year. a inner join &DA_SCHEMA..taf_PRV_IDT b on 
				a.submtg_state_cd = b.submtg_state_cd and a.prv_fil_dt = b.prv_fil_dt and a.da_run_id = b.da_run_id
		where b.prvdr_id_type_cd='2' and b.prvdr_id is not null;
	) by tmsis_passthrough;

	/* group to unique NPIs in the year and add prvdr_npi_cnt and max(month) as maxmo, 
	   prvdr_npi_cnt value is the same for each provider's npi records
	   add row numbers (_ndx) with partition and order for prvdr_npi array assignment */

	execute (
		create temp table npi_all as
		select a.submtg_state_cd, a.submtg_state_prvdr_id, a.maxmo, a.prvdr_id as prvdr_npi, b.prvdr_npi_cnt,
             /* grouping code */
             row_number() over (
               partition by a.submtg_state_cd, a.submtg_state_prvdr_id
               order by a.maxmo desc, a.prvdr_id desc
             ) as _ndx
		from (select submtg_state_cd, submtg_state_prvdr_id, prvdr_id, max(month) as maxmo
			 from npi_main group by submtg_state_cd, submtg_state_prvdr_id, prvdr_id) as a 
		left join
			 (select submtg_state_cd, submtg_state_prvdr_id, count(distinct(prvdr_id)) as prvdr_npi_cnt
			 from npi_main group by submtg_state_cd, submtg_state_prvdr_id) as b
		on a.submtg_state_cd=b.submtg_state_cd and a.submtg_state_prvdr_id=b.submtg_state_prvdr_id;
	) by tmsis_passthrough;

	/* create one record per provider, retain prvdr_npi_cnt, and use row numbers (_ndx) to create array prvdr_npi_01 prvdr_npi_02 */

	execute (
		create temp table npi_final
			diststyle key distkey(submtg_state_prvdr_id) as
		select submtg_state_cd, submtg_state_prvdr_id, prvdr_npi_cnt
			%map_arrayvars (varnm=prvdr_npi, N=2)
		from npi_all
		group by submtg_state_cd, submtg_state_prvdr_id, prvdr_npi_cnt;
	) by tmsis_passthrough;
  
	/* Create temp table with just ID_SPLMTL to join to base */
 
	%create_splmlt (segname=ID, segfile=id_pr_&year.)

	/* Insert into permanent table */

		%macro basecols;
	
			,PRVDR_LCTN_ID
			,PRVDR_ID_TYPE_CD
			,PRVDR_ID
			,PRVDR_ID_ISSG_ENT_ID_TXT
			,PRVDR_ID_FLAG_01
			,PRVDR_ID_FLAG_02
			,PRVDR_ID_FLAG_03
			,PRVDR_ID_FLAG_04
			,PRVDR_ID_FLAG_05
			,PRVDR_ID_FLAG_06
			,PRVDR_ID_FLAG_07
			,PRVDR_ID_FLAG_08
			,PRVDR_ID_FLAG_09
			,PRVDR_ID_FLAG_10
			,PRVDR_ID_FLAG_11
			,PRVDR_ID_FLAG_12

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PR_ID
		(DA_RUN_ID, PR_LOC_LINK_KEY, PR_FIL_DT, PR_VRSN, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID %basecols)
		select 

			%table_id_cols(loctype=2)
			%basecols

		from id_pr_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(id_pr_&year.)          

%mend create_ID;

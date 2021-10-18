/**********************************************************************************************/
/*Program: 010_up_top_fnl.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Union the four claims-specific files for the TOP segment, and then output to permanent table
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro up_top_fnl;

	execute (
		create temp table top_fnl_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select * from iph_bene_top_&year.

		union
		select * from lth_bene_top_&year.

		union
		select * from oth_bene_top_&year.

		union
		select * from rxh_bene_top_&year.

	) by tmsis_passthrough;

	** Insert into permanent table;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_UP_TOP

		select
			%table_id_cols
			,PGM_TYPE_CD
			,FILE_TYPE
			,CLM_TYPE_CD
			,CLM_TOT
			,SUM_TOT_MDCD_PD

		from top_fnl_&year.

	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(iph_bene_top_&year. lth_bene_top_&year. oth_bene_top_&year. rxh_bene_top_&year.);

%mend up_top_fnl;

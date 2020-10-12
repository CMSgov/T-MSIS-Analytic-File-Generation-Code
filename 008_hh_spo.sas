/**********************************************************************************************/
/*Program: 008_hh_spo.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual BSF segment 008: HH & SPO
/*Mod: 
/*Notes: This program reads in all columns related to HHs and SPOs, and looks across all
/*       cols to create HH_SPO_SPLMTL, which = 1 when ANY of the related cols = 1. It keeps
/*       this flag only on a temp table to be joined to base segment, and then inserts
/*       all other cols into the permanent table (subset to HH_SPO_SPLMTL=1).
/**********************************************************************************************/

%macro create_HHSPO;

	%create_temp_table(hh_spo,
              subcols=%nrbquote(  %monthly_array(HH_PGM_PRTCPNT_FLAG)
								  %last_best(HH_PRVDR_NUM)
								  %last_best(HH_ENT_NAME) 
								  %last_best(MH_HH_CHRNC_COND_FLAG) 
								  %last_best(SA_HH_CHRNC_COND_FLAG) 
								  %last_best(ASTHMA_HH_CHRNC_COND_FLAG) 
								  %last_best(DBTS_HH_CHRNC_COND_FLAG) 
								  %last_best(HRT_DIS_HH_CHRNC_COND_FLAG) 
								  %last_best(OVRWT_HH_CHRNC_COND_FLAG) 
								  %last_best(HIV_AIDS_HH_CHRNC_COND_FLAG) 
								  %last_best(OTHR_HH_CHRNC_COND_FLAG)
												          
								  %monthly_array(CMNTY_1ST_CHS_SPO_FLAG)
								  %monthly_array(_1915I_SPO_FLAG)
								  %monthly_array(_1915J_SPO_FLAG)
								  %monthly_array(_1932A_SPO_FLAG)
								  %monthly_array(_1915A_SPO_FLAG)
								  %monthly_array(_1937_ABP_SPO_FLAG)

								  /* Create a series of flags to be evaluated to create HH_SPO_SPLMTL */

								  %ever_year(HH_PGM_PRTCPNT_FLAG)
								  %ever_year(CMNTY_1ST_CHS_SPO_FLAG) 
								  %ever_year(_1915I_SPO_FLAG) 
								  %ever_year(_1915J_SPO_FLAG) 
								  %ever_year(_1932A_SPO_FLAG)
								  %ever_year(_1915A_SPO_FLAG)
								  %ever_year(_1937_ABP_SPO_FLAG)
												          
              ),

		outercols=%nrbquote (
								/* Create a flag for any HH CC flag to be evaluated to create HH_SPO_SPLMTL */ 
     
								%any_col(MH_HH_CHRNC_COND_FLAG SA_HH_CHRNC_COND_FLAG ASTHMA_HH_CHRNC_COND_FLAG 
                                         DBTS_HH_CHRNC_COND_FLAG HRT_DIS_HH_CHRNC_COND_FLAG 
                                         OVRWT_HH_CHRNC_COND_FLAG HIV_AIDS_HH_CHRNC_COND_FLAG
                                         OTHR_HH_CHRNC_COND_FLAG,
                                         HH_CHRNC_COND_ANY)

				) );

	/* Create second temp table with HH_SPO_SPLMTL (which will go onto the base segment AND determines 
       the records that go into the permanent HH_SPO table) */

	execute (
		create temp table hh_spo_&year.2
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select *
						     
		    	%any_col(HH_PGM_PRTCPNT_FLAG_EVR CMNTY_1ST_CHS_SPO_FLAG_EVR _1915I_SPO_FLAG_EVR 
                         _1915J_SPO_FLAG_EVR _1932A_SPO_FLAG_EVR _1915A_SPO_FLAG_EVR _1937_ABP_SPO_FLAG_EVR
                         HH_CHRNC_COND_ANY,
			          
			          HH_SPO_SPLMTL)

	    from hh_spo_&year.

	) by tmsis_passthrough;

	/* Create temp table with JUST HH_SPO_SPLMTL to join to base table */

	execute(
		create temp table HH_SPO_SPLMTL_&year. 
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
	   		   ,msis_ident_num
			   ,HH_SPO_SPLMTL

		from hh_spo_&year.2

	) by tmsis_passthrough;

	/* Insert into permanent table, subset to HH_SPO=1 */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select 

			%table_id_cols
			,HH_PGM_PRTCPNT_FLAG_01
			,HH_PGM_PRTCPNT_FLAG_02
			,HH_PGM_PRTCPNT_FLAG_03
			,HH_PGM_PRTCPNT_FLAG_04
			,HH_PGM_PRTCPNT_FLAG_05
			,HH_PGM_PRTCPNT_FLAG_06
			,HH_PGM_PRTCPNT_FLAG_07
			,HH_PGM_PRTCPNT_FLAG_08
			,HH_PGM_PRTCPNT_FLAG_09
			,HH_PGM_PRTCPNT_FLAG_10
			,HH_PGM_PRTCPNT_FLAG_11
			,HH_PGM_PRTCPNT_FLAG_12
			,HH_PRVDR_NUM 
			,HH_ENT_NAME 
			,MH_HH_CHRNC_COND_FLAG 
			,SA_HH_CHRNC_COND_FLAG 
			,ASTHMA_HH_CHRNC_COND_FLAG 
			,DBTS_HH_CHRNC_COND_FLAG 
			,HRT_DIS_HH_CHRNC_COND_FLAG 
			,OVRWT_HH_CHRNC_COND_FLAG 
			,HIV_AIDS_HH_CHRNC_COND_FLAG 
			,OTHR_HH_CHRNC_COND_FLAG 
			,CMNTY_1ST_CHS_SPO_FLAG_01
			,CMNTY_1ST_CHS_SPO_FLAG_02
			,CMNTY_1ST_CHS_SPO_FLAG_03
			,CMNTY_1ST_CHS_SPO_FLAG_04
			,CMNTY_1ST_CHS_SPO_FLAG_05
			,CMNTY_1ST_CHS_SPO_FLAG_06
			,CMNTY_1ST_CHS_SPO_FLAG_07
			,CMNTY_1ST_CHS_SPO_FLAG_08
			,CMNTY_1ST_CHS_SPO_FLAG_09
			,CMNTY_1ST_CHS_SPO_FLAG_10
			,CMNTY_1ST_CHS_SPO_FLAG_11
			,CMNTY_1ST_CHS_SPO_FLAG_12
			,_1915I_SPO_FLAG_01
			,_1915I_SPO_FLAG_02
			,_1915I_SPO_FLAG_03
			,_1915I_SPO_FLAG_04
			,_1915I_SPO_FLAG_05
			,_1915I_SPO_FLAG_06
			,_1915I_SPO_FLAG_07
			,_1915I_SPO_FLAG_08
			,_1915I_SPO_FLAG_09
			,_1915I_SPO_FLAG_10
			,_1915I_SPO_FLAG_11
			,_1915I_SPO_FLAG_12
			,_1915J_SPO_FLAG_01
			,_1915J_SPO_FLAG_02
			,_1915J_SPO_FLAG_03
			,_1915J_SPO_FLAG_04
			,_1915J_SPO_FLAG_05
			,_1915J_SPO_FLAG_06
			,_1915J_SPO_FLAG_07
			,_1915J_SPO_FLAG_08
			,_1915J_SPO_FLAG_09
			,_1915J_SPO_FLAG_10
			,_1915J_SPO_FLAG_11
			,_1915J_SPO_FLAG_12
			,_1932A_SPO_FLAG_01
			,_1932A_SPO_FLAG_02
			,_1932A_SPO_FLAG_03
			,_1932A_SPO_FLAG_04
			,_1932A_SPO_FLAG_05
			,_1932A_SPO_FLAG_06
			,_1932A_SPO_FLAG_07
			,_1932A_SPO_FLAG_08
			,_1932A_SPO_FLAG_09
			,_1932A_SPO_FLAG_10
			,_1932A_SPO_FLAG_11
			,_1932A_SPO_FLAG_12
			,_1915A_SPO_FLAG_01
			,_1915A_SPO_FLAG_02
			,_1915A_SPO_FLAG_03
			,_1915A_SPO_FLAG_04
			,_1915A_SPO_FLAG_05
			,_1915A_SPO_FLAG_06
			,_1915A_SPO_FLAG_07
			,_1915A_SPO_FLAG_08
			,_1915A_SPO_FLAG_09
			,_1915A_SPO_FLAG_10
			,_1915A_SPO_FLAG_11
			,_1915A_SPO_FLAG_12
			,_1937_ABP_SPO_FLAG_01
			,_1937_ABP_SPO_FLAG_02
			,_1937_ABP_SPO_FLAG_03
			,_1937_ABP_SPO_FLAG_04
			,_1937_ABP_SPO_FLAG_05
			,_1937_ABP_SPO_FLAG_06
			,_1937_ABP_SPO_FLAG_07
			,_1937_ABP_SPO_FLAG_08
			,_1937_ABP_SPO_FLAG_09
			,_1937_ABP_SPO_FLAG_10
			,_1937_ABP_SPO_FLAG_11
			,_1937_ABP_SPO_FLAG_12

		from hh_spo_&year.2
		where HH_SPO_SPLMTL=1

	) by tmsis_passthrough;

	
	/* Drop temp tables */

	%drop_tables(hh_spo_&year. hh_spo_&year.2)


%mend create_HHSPO;


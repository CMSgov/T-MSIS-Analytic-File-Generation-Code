/**********************************************************************************************/
/*Program: 007_mfp.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual BSF segment 007: MFP
/*Mod: 
/*Notes: This program reads in the MFP-related cols and creates a temp table to be inserted into
/*       the permanent table. It also creates the column MFP_SPLMTL which = 1 if ANY of the MFP 
/*       monthly flags = 1 or if any of the other MFP-related cols are non-missing. This
/*       flag only is kept in a temp table to be joined to the base table.
/**********************************************************************************************/


%macro create_MFP;

	%create_temp_table(mfp,
              subcols=%nrbquote(  %last_best(MFP_PRTCPTN_ENDD_RSN_CD) 
                                  %last_best(MFP_LVS_WTH_FMLY_CD)
                                  %last_best(MFP_QLFYD_INSTN_CD) 
                                  %last_best(MFP_RINSTLZD_RSN_CD) 
                                  %last_best(MFP_QLFYD_RSDNC_CD)
                                  %monthly_array(MFP_PRTCPNT_FLAG)
                                  %last_best(MFP_PRTCPNT_FLAG,outcol=MFP_PRTCPNT_FLAG_LTST)

								  /* Create an indicator for ANY of the MFP monthly flags = 1 which we will 
								     use to create MFP_SPLMTL */

                                  %ever_year(MFP_PRTCPNT_FLAG)
                                  
              ),

			  outercols=%nrbquote(

								  /* Create MFP_SPLMTL (which will go onto the base segment AND determines 
			                         the records that go into the permanent MFP table) */
					   
					               ,case when MFP_PRTCPNT_FLAG_EVR=1 or 
										          nullif(MFP_PRTCPTN_ENDD_RSN_CD,'00') is not null or 
										          nullif(MFP_QLFYD_INSTN_CD,'00') is not null or
										          nullif(MFP_QLFYD_RSDNC_CD,'00') is not null or
										          nullif(MFP_RINSTLZD_RSN_CD,'00') is not null or
										          nullif(MFP_LVS_WTH_FMLY_CD,'2') is not null
				          
								     then 1 else 0
								     end as MFP_SPLMTL
			) );

	/* Create temp table with just MFP_SPLMTL to join to base */

	execute(
		create temp table MFP_SPLMTL_&year. 
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
	   		   ,msis_ident_num
			   ,MFP_SPLMTL

		from mfp_&year.

	) by tmsis_passthrough;

	/* Insert full temp table into permanent table, subset to MFP_SPLMTL=1 */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select

			%table_id_cols
			,MFP_PRTCPTN_ENDD_RSN_CD 
			,MFP_LVS_WTH_FMLY_CD 
			,MFP_QLFYD_INSTN_CD 
			,MFP_RINSTLZD_RSN_CD 
			,MFP_QLFYD_RSDNC_CD 
			,MFP_PRTCPNT_FLAG_01
			,MFP_PRTCPNT_FLAG_02
			,MFP_PRTCPNT_FLAG_03
			,MFP_PRTCPNT_FLAG_04
			,MFP_PRTCPNT_FLAG_05
			,MFP_PRTCPNT_FLAG_06
			,MFP_PRTCPNT_FLAG_07
			,MFP_PRTCPNT_FLAG_08
			,MFP_PRTCPNT_FLAG_09
			,MFP_PRTCPNT_FLAG_10
			,MFP_PRTCPNT_FLAG_11
			,MFP_PRTCPNT_FLAG_12
			,MFP_PRTCPNT_FLAG_LTST

		from mfp_&year.
		where MFP_SPLMTL=1

	) by tmsis_passthrough; 

	/* Drop temp tables */

	%drop_tables(mfp_&year.)

%mend create_MFP;

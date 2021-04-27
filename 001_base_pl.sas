** ========================================================================== 
** program documentation 
** program     : 001_base_pl.sas
** description : Generate the annual PL segment 001: Base
** date        : 09/2019 12/2020
** note        : This program creates all the columns for the base file. 
**               It takes the last best value or ever in the year value depending on element and
**               calculates continuous range for contract date as well as aggregates values for 
**               the new accreditation arrays.
**               Finally, it pulls in _SPLMTL flags.
**               It then inserts into the permanent table.
** ==========================================================================;

%macro create_BASE;

	/* Create the partial base segment, pulling in only columns are not acceditation */
	
	%create_temp_table(fileseg=MCP, tblname=base_pl,
		          subcols=%nrbquote( 
								     %last_best(MC_NAME)
								     %last_best(MC_PLAN_TYPE_CD)
								     %last_best(MC_PLAN_TYPE_CAT)
									 %last_best(MC_CNTRCT_END_DT)
		                             %nonmiss_month(MC_CNTRCT_END_DT) 
									 %monthly_array(MC_CNTRCT_EFCTV_DT, nslots=1)
									 %monthly_array(MC_CNTRCT_END_DT, nslots=1)
								     %last_best(MC_PGM_CD)
								     %last_best(REIMBRSMT_ARNGMT_CD)
								     %last_best(REIMBRSMT_ARNGMT_CAT)
								     %last_best(MC_SAREA_CD)
								     %ever_year(SAREA_STATEWIDE_IND)
								     %ever_year(OPRTG_AUTHRTY_1115_DEMO_IND)
								     %ever_year(OPRTG_AUTHRTY_1915B_IND)
								     %ever_year(OPRTG_AUTHRTY_1932A_IND)
								     %ever_year(OPRTG_AUTHRTY_1915A_IND)
								     %ever_year(OPRTG_AUTHRTY_1915BC_CONC_IND)
								     %ever_year(OPRTG_AUTHRTY_1915AC_CONC_IND)
								     %ever_year(OPRTG_AUTHRTY_1932A_1915C_IND)
								     %ever_year(OPRTG_AUTHRTY_PACE_IND)
								     %ever_year(OPRTG_AUTHRTY_1905T_IND)
								     %ever_year(OPRTG_AUTHRTY_1937_IND)
								     %ever_year(OPRTG_AUTHRTY_1902A70_IND)
								     %ever_year(OPRTG_AUTHRTY_1915BI_CONC_IND)
								     %ever_year(OPRTG_AUTHRTY_1915AI_CONC_IND)
								     %ever_year(OPRTG_AUTHRTY_1932A_1915I_IND)
								     %ever_year(OPRTG_AUTHRTY_1945_HH_IND)
								     %ever_year(POP_MDCD_MAND_COV_ADLT_IND)
								     %ever_year(POP_MDCD_MAND_COV_ABD_IND)
								     %ever_year(POP_MDCD_OPTN_COV_ADLT_IND)
								     %ever_year(POP_MDCD_OPTN_COV_ABD_IND)
								     %ever_year(POP_MDCD_MDCLY_NDY_ADLT_IND)
								     %ever_year(POP_MDCD_MDCLY_NDY_ABD_IND)
								     %ever_year(POP_CHIP_COV_CHLDRN_IND)
								     %ever_year(POP_CHIP_OPTN_CHLDRN_IND)
								     %ever_year(POP_CHIP_OPTN_PRGNT_WMN_IND)
								     %ever_year(POP_1115_EXPNSN_IND)
								     %ever_year(POP_UNK_IND)
								     %last_best(REG_FLAG)
								     %last_best(CBSA_CD)
								     %last_best(MC_PRFT_STUS_CD)
								     %last_best(BUSNS_PCT)
									 ),
		          subcols2=%nrbquote(
									 %monthly_array(ACRDTN_ORG_01, nslots=1)
									 %monthly_array(ACRDTN_ORG_02, nslots=1)
									 %monthly_array(ACRDTN_ORG_03, nslots=1)
									 ),
		          subcols3=%nrbquote(
									 %monthly_array(ACRDTN_ORG_ACHVMT_DT_01, nslots=1)
									 %monthly_array(ACRDTN_ORG_ACHVMT_DT_02, nslots=1)
									 %monthly_array(ACRDTN_ORG_ACHVMT_DT_03, nslots=1)
									 ),
		          subcols4=%nrbquote(
									 %monthly_array(ACRDTN_ORG_END_DT_01, nslots=1)
									 %monthly_array(ACRDTN_ORG_END_DT_02, nslots=1)
									 %monthly_array(ACRDTN_ORG_END_DT_03, nslots=1)
									 %any_month(MC_PLAN_ID MC_NAME,PLAN_ID_FLAG,condition=%nrstr(is not null))
									 ),
	              outercols=%nrbquote(  
									 %assign_nonmiss_month(MC_EFF_DT,MC_CNTRCT_END_DT_MN,MC_CNTRCT_EFCTV_DT)
									) );


/* create Accreditation0 so that separate records with a unique row for each set of values that occures in any month can be sorted and loaded into new annual array */

  execute (

	create temp table Accreditation0 (
		SUBMTG_STATE_CD varchar(2), 
		MC_PLAN_ID varchar(12), 
		ACRDTN_ORG varchar(2),
		ACRDTN_ORG_ACHVMT_DT date, 
		ACRDTN_ORG_END_DT date
	);

  ) by tmsis_passthrough;

/* insert accreditation array elements into Accreditation0 - need separate records so that values can to be grouped and sorted for annual arrays */

  execute (

	%do m=1 %to 12;
		%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));

		%do a=1 %to 3;
			%let a=%sysfunc(putn(&a.,z2));

			insert into Accreditation0 
			   (SUBMTG_STATE_CD, MC_PLAN_ID, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT) 
			   select SUBMTG_STATE_CD, MC_PLAN_ID, ACRDTN_ORG_&a._&m., ACRDTN_ORG_ACHVMT_DT_&a._&m., ACRDTN_ORG_END_DT_&a._&m. 
					from base_pl_&year.
					where ACRDTN_ORG_&a._&m. is not null;
		%end;

	%end;

  ) by tmsis_passthrough;
  
/* identify unique records for new annual accreditation arrays - groups/sorts unique values for annual arrays */
  
  execute (
  
	create temp table Accreditation1
			 diststyle key distkey(MC_PLAN_ID) as
	  select SUBMTG_STATE_CD, MC_PLAN_ID, 
		  ACRDTN_ORG,
		  ACRDTN_ORG_ACHVMT_DT,
		  ACRDTN_ORG_END_DT
		 
	  from Accreditation0
	  group by SUBMTG_STATE_CD, MC_PLAN_ID, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT
	  order by SUBMTG_STATE_CD, MC_PLAN_ID, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT;
	  
  ) by tmsis_passthrough;
	                                  
/* _ndx identifies record order for new annual accreditation arrays keeps unique values together with the same array index */
									
  execute (
  
	create temp table Accreditation2
			 diststyle key distkey(MC_PLAN_ID) as
	  select  SUBMTG_STATE_CD, MC_PLAN_ID,
		  ACRDTN_ORG,
		  ACRDTN_ORG_ACHVMT_DT,
		  ACRDTN_ORG_END_DT,
		 /* grouping code */
		 row_number() over (
		   partition by SUBMTG_STATE_CD, MC_PLAN_ID
		   order by ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT
		 ) as _ndx
		 
	  from Accreditation1
	  
	  order by SUBMTG_STATE_CD, MC_PLAN_ID, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT;
	  
  ) by tmsis_passthrough;

/* create new annual arrays to join to base */

  execute (
   
    create temp table Accreditation_Array
                 diststyle key distkey(MC_PLAN_ID) as
      select SUBMTG_STATE_CD, MC_PLAN_ID
             %map_arrayvars(varnm=ACRDTN_ORG, N=5)
             %map_arrayvars(varnm=ACRDTN_ORG_ACHVMT_DT, N=5)
			 %map_arrayvars(varnm=ACRDTN_ORG_END_DT, N=5)
			 
      from Accreditation2
	  
      group by SUBMTG_STATE_CD, MC_PLAN_ID
	  
  ) by tmsis_passthrough;

  
/* find effective dates for contract that are continuous in earlier timeframes with the record that has the last-best MC_CNTRCT_END_DT */

    execute (
  
		 /* contract start date */
		create temp table cntrct_vert_month (
				SUBMTG_STATE_CD varchar(2), 
				MC_PLAN_ID varchar(12),
				MC_EFF_DT date, 
				MC_CNTRCT_END_DT date, 
				mc_mnth_eff_dt date, 
				mc_mnth_end_dt date
		);

	) by tmsis_passthrough;

    execute (

		%do m=1 %to 12;
			%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2));

			insert into cntrct_vert_month 
			   (SUBMTG_STATE_CD, MC_PLAN_ID, MC_EFF_DT,  MC_CNTRCT_END_DT, mc_mnth_eff_dt, mc_mnth_end_dt) 
			   (select SUBMTG_STATE_CD, MC_PLAN_ID, MC_EFF_DT, MC_CNTRCT_END_DT, MC_CNTRCT_EFCTV_DT_&m., MC_CNTRCT_END_DT_&m. 
					 from base_pl_&year.
					 where MC_CNTRCT_EFCTV_DT_&m. is not null and MC_CNTRCT_END_DT_&m. is not null and MC_CNTRCT_EFCTV_DT_&m. <= MC_CNTRCT_END_DT);

		%end;
 
	) by tmsis_passthrough;
	
	
    execute (

		create temp table srtd as 
		select 
			 SUBMTG_STATE_CD, MC_PLAN_ID, 
			 MC_EFF_DT, MC_CNTRCT_END_DT,
			 mc_mnth_eff_dt, mc_mnth_end_dt, 
			 case (
					least	(mc_mnth_eff_dt,
		%do m=1 %to 11;
						lag(mc_mnth_eff_dt,&m.) over (partition by SUBMTG_STATE_CD, MC_PLAN_ID order by mc_mnth_end_dt desc, mc_mnth_eff_dt desc)
			%if &m. < 11 %then , ;
		%end;
							) <= (lag(mc_mnth_end_dt) over (partition by SUBMTG_STATE_CD, MC_PLAN_ID order by mc_mnth_end_dt, mc_mnth_eff_dt))  + 1
					)
			  when true then 'C'
			  when false then 'A'
			  else 'B'
			 end
				as cont_rank,
			 least	(mc_mnth_eff_dt,
		%do m=1 %to 11;
				lag(mc_mnth_eff_dt,&m.) over (partition by SUBMTG_STATE_CD, MC_PLAN_ID order by mc_mnth_end_dt desc, mc_mnth_eff_dt desc)
			%if &m. < 11 %then , ;
		%end;
					)
			 as new_beginning
	   from cntrct_vert_month 
	   group by SUBMTG_STATE_CD, MC_PLAN_ID, MC_EFF_DT, MC_CNTRCT_END_DT, mc_mnth_eff_dt, mc_mnth_end_dt 
	   order by SUBMTG_STATE_CD, MC_PLAN_ID, mc_mnth_end_dt desc;
 
	) by tmsis_passthrough;


    execute (
		create temp table selected_cntrct_dt as
		select SUBMTG_STATE_CD, MC_PLAN_ID, MC_EFF_DT, MC_CNTRCT_END_DT, cont_rank, new_beginning
		from (select SUBMTG_STATE_CD, MC_PLAN_ID, MC_EFF_DT, MC_CNTRCT_END_DT, cont_rank, new_beginning,
				row_number() over (partition by SUBMTG_STATE_CD, MC_PLAN_ID order by MC_EFF_DT, MC_CNTRCT_END_DT, cont_rank, mc_mnth_end_dt desc, mc_mnth_eff_dt desc, new_beginning) as cntrct_dt_rank
				from srtd order by SUBMTG_STATE_CD, MC_PLAN_ID, cont_rank
		)
		where cntrct_dt_rank = 1;
	) by tmsis_passthrough;


	%drop_tables(srtd cntrct_vert_month)


 /* Join base_pl, selected_cntrct_dt, new accreditiation arrays */

    execute (

		create temp table base_&year.
	    distkey(MC_PLAN_ID) 
		sortkey(SUBMTG_STATE_CD,MC_PLAN_ID) as

		select a.*
			,case 
				when (a.MC_EFF_DT is null or a.MC_CNTRCT_END_DT is null) or (b.new_beginning is null and (a.MC_EFF_DT > a.MC_CNTRCT_END_DT)) then a.MC_EFF_DT 
				else b.new_beginning end :: date as MC_CNTRCT_EFCTV_DT
			,case 
				when b.cont_rank='A' or ((a.MC_EFF_DT is null or a.MC_CNTRCT_END_DT is null) and b.cont_rank='B') then 1 else 0 end :: smallint as ADDTNL_CNTRCT_PRD_FLAG
			,ACRDTN_ORG_01
			,ACRDTN_ORG_02
			,ACRDTN_ORG_03
			,ACRDTN_ORG_04
			,ACRDTN_ORG_05
			,ACRDTN_ORG_ACHVMT_DT_01
			,ACRDTN_ORG_ACHVMT_DT_02
			,ACRDTN_ORG_ACHVMT_DT_03
			,ACRDTN_ORG_ACHVMT_DT_04
			,ACRDTN_ORG_ACHVMT_DT_05
			,ACRDTN_ORG_END_DT_01
			,ACRDTN_ORG_END_DT_02
			,ACRDTN_ORG_END_DT_03
			,ACRDTN_ORG_END_DT_04
			,ACRDTN_ORG_END_DT_05
	
		from base_pl_&year. a
		     left join
		     Accreditation_Array c on a.SUBMTG_STATE_CD = c.SUBMTG_STATE_CD and a.MC_PLAN_ID = c.MC_PLAN_ID 
		     left join
		     (select SUBMTG_STATE_CD, MC_PLAN_ID, cont_rank, new_beginning
				from selected_cntrct_dt) b
				on a.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and a.MC_PLAN_ID = b.MC_PLAN_ID
 
	) by tmsis_passthrough;

	/* Drop temp tables no longer needed */

	%drop_tables(base_pl_&year. Accreditation0 Accreditation1 Accreditation2 Accreditation_Array selected_cntrct_dt)

	/* Join to the monthly _SPLMTL flags. */

	execute (
		create temp table base_&year._final
	    distkey(MC_PLAN_ID) 
		sortkey(SUBMTG_STATE_CD,MC_PLAN_ID) as

		select a.*
			   ,case when b.LCTN_SPLMTL_CT>0 then 1 else 0 end :: smallint as LCTN_SPLMTL
			   ,case when c.SAREA_SPLMTL_CT>0 then 1 else 0 end :: smallint as SAREA_SPLMTL
			   ,case when d.ENRLMT_SPLMTL_CT>0 then 1 else 0 end :: smallint as ENRLMT_SPLMTL
			   ,case when e.OPRTG_AUTHRTY_SPLMTL_CT>0 then 1 else 0 end :: smallint as OPRTG_AUTHRTY_SPLMTL

		from base_&year. a
		    left join
			LCTN_SPLMTL_&year. b on a.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and a.MC_PLAN_ID = b.MC_PLAN_ID
			left join 
			SAREA_SPLMTL_&year. c on a.SUBMTG_STATE_CD = c.SUBMTG_STATE_CD and a.MC_PLAN_ID = c.MC_PLAN_ID
			left join
			ENRLMT_SPLMTL_&year. d on a.SUBMTG_STATE_CD = d.SUBMTG_STATE_CD and a.MC_PLAN_ID = d.MC_PLAN_ID
			left join
			OPRTG_AUTHRTY_SPLMTL_&year. e on a.SUBMTG_STATE_CD = e.SUBMTG_STATE_CD and a.MC_PLAN_ID = e.MC_PLAN_ID

			order by SUBMTG_STATE_CD, MC_PLAN_ID;

	) by tmsis_passthrough;

	/* Drop temp tables no longer needed */

	%drop_tables(base_&year. LCTN_SPLMTL_&year. SAREA_SPLMTL_&year. ENRLMT_SPLMTL_&year. OPRTG_AUTHRTY_SPLMTL_&year.)

	/* Insert into permanent table */

	%macro basecols;
	
			,MC_NAME
			,MC_PLAN_TYPE_CD
			,MC_PLAN_TYPE_CAT
			,MC_CNTRCT_EFCTV_DT
			,MC_CNTRCT_END_DT
			,ADDTNL_CNTRCT_PRD_FLAG
			,MC_PGM_CD
			,REIMBRSMT_ARNGMT_CD
			,REIMBRSMT_ARNGMT_CAT
			,MC_SAREA_CD
			,SAREA_STATEWIDE_IND
			,OPRTG_AUTHRTY_1115_DEMO_IND
			,OPRTG_AUTHRTY_1915B_IND
			,OPRTG_AUTHRTY_1932A_IND
			,OPRTG_AUTHRTY_1915A_IND
			,OPRTG_AUTHRTY_1915BC_CONC_IND
			,OPRTG_AUTHRTY_1915AC_CONC_IND
			,OPRTG_AUTHRTY_1932A_1915C_IND
			,OPRTG_AUTHRTY_PACE_IND
			,OPRTG_AUTHRTY_1905T_IND
			,OPRTG_AUTHRTY_1937_IND
			,OPRTG_AUTHRTY_1902A70_IND
			,OPRTG_AUTHRTY_1915BI_CONC_IND
			,OPRTG_AUTHRTY_1915AI_CONC_IND
			,OPRTG_AUTHRTY_1932A_1915I_IND
			,OPRTG_AUTHRTY_1945_HH_IND
			,POP_MDCD_MAND_COV_ADLT_IND
			,POP_MDCD_MAND_COV_ABD_IND
			,POP_MDCD_OPTN_COV_ADLT_IND
			,POP_MDCD_OPTN_COV_ABD_IND
			,POP_MDCD_MDCLY_NDY_ADLT_IND
			,POP_MDCD_MDCLY_NDY_ABD_IND
			,POP_CHIP_COV_CHLDRN_IND
			,POP_CHIP_OPTN_CHLDRN_IND
			,POP_CHIP_OPTN_PRGNT_WMN_IND
			,POP_1115_EXPNSN_IND
			,POP_UNK_IND
			,ACRDTN_ORG_01
			,ACRDTN_ORG_02
			,ACRDTN_ORG_03
			,ACRDTN_ORG_04
			,ACRDTN_ORG_05
			,ACRDTN_ORG_ACHVMT_DT_01
			,ACRDTN_ORG_ACHVMT_DT_02
			,ACRDTN_ORG_ACHVMT_DT_03
			,ACRDTN_ORG_ACHVMT_DT_04
			,ACRDTN_ORG_ACHVMT_DT_05
			,ACRDTN_ORG_END_DT_01
			,ACRDTN_ORG_END_DT_02
			,ACRDTN_ORG_END_DT_03
			,ACRDTN_ORG_END_DT_04
			,ACRDTN_ORG_END_DT_05
			,REG_FLAG
			,CBSA_CD
			,MC_PRFT_STUS_CD
			,BUSNS_PCT
			,PLAN_ID_FLAG_01
			,PLAN_ID_FLAG_02
			,PLAN_ID_FLAG_03
			,PLAN_ID_FLAG_04
			,PLAN_ID_FLAG_05
			,PLAN_ID_FLAG_06
			,PLAN_ID_FLAG_07
			,PLAN_ID_FLAG_08
			,PLAN_ID_FLAG_09
			,PLAN_ID_FLAG_10
			,PLAN_ID_FLAG_11
			,PLAN_ID_FLAG_12
			,LCTN_SPLMTL
			,SAREA_SPLMTL
			,ENRLMT_SPLMTL
			,OPRTG_AUTHRTY_SPLMTL

		%mend basecols;

	execute (
		insert into &DA_SCHEMA..TAF_ANN_PL_&tblname.
		(DA_RUN_ID, PL_LINK_KEY, PL_FIL_DT, PL_VRSN, SUBMTG_STATE_CD, MC_PLAN_ID %basecols)
		select 

		    %table_id_cols
		    %basecols

		from base_&year._final

	) by tmsis_passthrough; 

	/* Delete temp tables */

	%drop_tables(base_&year._final)          

%mend create_BASE;

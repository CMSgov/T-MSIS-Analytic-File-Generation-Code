/**********************************************************************************************/
/*Program: 004_up_base_line_comb.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Combine the four line files that were summarized to the header in program 003 - stack
/*         all four header-level rolledup files to then summarize across all four file types
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro base_line_comb;

	** Step 1: Union the four files, only keeping cols that we need to summarize across the file types 
       (cols that are common to all four file types - listed in the macro commoncols_base_hdr.
       Do this in the inner query. In the outer query, summarize to the bene-level and take sum/max
       across the four file value;

	execute (
		create temp table line_bene_base_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
		       ,msis_ident_num

			/* For four combinations of claims (MDCD non-xover, SCHIP non-xover, MDCD xovr and SCHIP xovr),
		       get the same counts and totals. Loop over INDS1 (MDCD SCHIP) and INDS2 (NXOVR XOVR) to assign
			   the four pairs of records */

			%do i=1 %to 2;
			  %let ind1=%scan(&INDS1.,&i.);
			  %do j=1 %to 2;
			   	 %let ind2=%scan(&INDS2.,&j.);

				 %sumrecs(incol=&ind1._&ind2._ANY_MC_CMPRHNSV,
			              outcol=&ind1._&ind2._MC_CMPRHNSV_CLM)

				 %sumrecs(incol=&ind1._&ind2._ANY_MC_PCCM,
			              outcol=&ind1._&ind2._MC_PCCM_CLM)

				 %sumrecs(incol=&ind1._&ind2._ANY_MC_PVT_INS,
			              outcol=&ind1._&ind2._MC_PVT_INS_CLM)

				 %sumrecs(incol=&ind1._&ind2._ANY_MC_PHP,
			              outcol=&ind1._&ind2._MC_PHP_CLM)

				 %sumrecs(incol=&ind1._&ind2._PD)

				 %sumrecs(incol=&ind1._&ind2._FFS_EQUIV_AMT)
				 %sumrecs(incol=&ind1._&ind2._MC_CMPRHNSV_PD)
				 %sumrecs(incol=&ind1._&ind2._MC_PCCM_PD)
				 %sumrecs(incol=&ind1._&ind2._MC_PVT_INS_PD)
				 %sumrecs(incol=&ind1._&ind2._MC_PHP_PD)


				 %sumrecs(incol=&ind1._&ind2._MDCR_CLM)
				 %sumrecs(incol=&ind1._&ind2._MDCR_PD)
				 %sumrecs(incol=&ind1._&ind2._OTHR_CLM)
				 %sumrecs(incol=&ind1._&ind2._OTHR_PD)
				 %sumrecs(incol=&ind1._&ind2._HH_CLM)
				 %sumrecs(incol=&ind1._&ind2._HH_PD)


			  %end;
			%end;

			/* Take max of HCBS indicators (note were hard-coded to 0 for all files except OT,
			   to make roll-up simpler */

			%do h=1 %to 7;
				%let hcbsval=%scan(&HCBSVALS.,&h.);

				%getmax(incol=hcbs_&h._clm_flag,
				        outcol=hcbs_&hcbsval._clm_flag)

			%end;


		from (

			select %commoncols_base_line
			from ipl_hdr_lvl_&year.

			union all
			select %commoncols_base_line
			from ltl_hdr_lvl_&year.

			union all
			select %commoncols_base_line
			from otl_hdr_lvl_&year.

			union all
			select %commoncols_base_line
			from rxl_hdr_lvl_&year.

			)
		group by submtg_state_cd,
		         msis_ident_num

	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(ipl_hdr_lvl_&year. ltl_hdr_lvl_&year. otl_hdr_lvl_&year. rxl_hdr_lvl_&year.);

%mend base_line_comb;

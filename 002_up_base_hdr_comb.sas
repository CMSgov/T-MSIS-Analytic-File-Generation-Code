/**********************************************************************************************/
/*Program: 002_up_base_hdr_comb.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Union the four header files all four files created in 001 (bene-level) to create 
/*         output columns that summarize across the four file types.
/*Mod: 
/*Notes: 
/**********************************************************************************************/
/* Copyright (C) Mathematica Policy Research, Inc.                                            */
/* This code cannot be copied, distributed or used without the express written permission     */
/* of Mathematica Policy Research, Inc.                                                       */ 
/**********************************************************************************************/

%macro base_hdr_comb;

	** Union the four files, creating dummy columns for any file-specific columns (so can union 
       all four files with all the same columns) - these are listed in the macro unio_base_hdr.
       Do this in the inner query. In the outer query, summarize to the bene-level and take sum/max
       across the four file value;

	execute (
		create temp table hdr_bene_base_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd,
		       msis_ident_num

			   /* Create RCPNT_IND based on having ANY FFS and/or MC claims */

			   ,case when max(ANY_FFS)=0 and max(ANY_MC)=0 then '0'
					 when max(ANY_FFS)=1 and max(ANY_MC)=0 then '1'
					 when max(ANY_FFS)=0 and max(ANY_MC)=1 then '2'
					 when max(ANY_FFS)=1 and max(ANY_MC)=1 then '3'
					 else null
					 end as rcpnt_ind

			   %getmax(incol=sect_1115a_demo_ind_any)

			   /* Loop over MDCD/SCHIP and NON_XOVR/XOVR */

				%do i=1 %to 2;
				  %let ind1=%scan(&INDS1.,&i.);
				  %do j=1 %to 2;
				   	 %let ind2=%scan(&INDS2.,&j.);

					 %getmax(incol=&ind1._rcpnt_&ind2._FFS_FLAG)
					 %getmax(incol=&ind1._rcpnt_&ind2._MC_FLAG)
					 %sumrecs(incol=TOT_&ind1._&ind2._PD)

					 %if &ind2.=NON_XOVR %then %do;

					    %sumrecs(incol=&ind1._&ind2._SPLMTL_CLM)
						%sumrecs(incol=TOT_&ind1._&ind2._SPLMTL_PD)

					%end;

				  %end;
				%end;

				/* Loop over the four file types and just take the max of all elements (all are in one file only,
				   so there is only one record with an actual value coming from the inner query) */

				%do f=1 %to 4;
			   	  %let file=%scan(&FLTYPES.,&f.);

				  %if &file. ne RX %then %do;

				  	%getmax(incol=&file._mh_dx_ind_any)
					%getmax(incol=&file._sud_dx_ind_any)
					%getmax(incol=&file._mh_txnmy_ind_any)
					%getmax(incol=&file._sud_txnmy_ind_any)

					%getmax(incol=&file._ffs_mh_clm)
					%getmax(incol=&file._mc_mh_clm)
					%getmax(incol=&file._ffs_sud_clm)
					%getmax(incol=&file._mc_sud_clm)

					%getmax(incol=&file._ffs_mh_pd)
					%getmax(incol=&file._ffs_sud_pd)

				  %end;

				  /* Loop over MDCD/SCHIP and NON_XOVR/XOVR */

				  %do i=1 %to 2;
					  %let ind1=%scan(&INDS1.,&i.);
					  %do j=1 %to 2;
					   	 %let ind2=%scan(&INDS2.,&j.);

						 %getmax(incol=TOT_&ind1._&ind2._FFS_&file._PD)

						 /* Only count claims for OT and RX - IP and LT will be counted when rolling up to
					        visits/days */

						  %if &file. = OT or &file. = RX %then %do;

							 %getmax(incol=&ind1._&ind2._FFS_&file._CLM)
							 %getmax(incol=&ind1._&ind2._MC_&file._CLM)

						  %end;

					%end;

				%end;

			%end; /* end file=1 to 4 loop */


		from (

			%union_base_hdr(IP)

			union all

			%union_base_hdr(LT)

			union all

			%union_base_hdr(OT)

			union all

			%union_base_hdr(RX)

			)
		group by submtg_state_cd,
		         msis_ident_num

	) by tmsis_passthrough;


	** Drop tables no longer needed;

	%drop_tables( iph_bene_base_&year. lth_bene_base_&year. oth_bene_base_&year. rxh_bene_base_&year.);

%mend base_hdr_comb;

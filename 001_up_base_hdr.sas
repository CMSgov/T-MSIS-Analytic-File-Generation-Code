/**********************************************************************************************/
/*Program: 001_up_base_hdr.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Generate counts and sums by bene at the header-level for each claim type to then be 
/*         combined across the four file types for the BASE segment
/*Mod: 
/*Notes: 
/**********************************************************************************************/
/* Copyright (C) Mathematica Policy Research, Inc.                                            */
/* This code cannot be copied, distributed or used without the express written permission     */
/* of Mathematica Policy Research, Inc.                                                       */ 
/**********************************************************************************************/

%macro base_hdr_byfile(file=);

	** Roll-up all header-level cols for given file type;

	execute (
		create temp table &file.h_bene_base_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
		       ,msis_ident_num

			   /* Create indicators for ANY header = FFS and ANY header = MC, which will then
			      be used to create RCPNT_IND */

			   %any_rec(condcol1=clm_type_cd, 
                        cond1=%nrstr( in ('1', 'A')),
						outcol=ANY_FFS)

			   %any_rec(condcol1=clm_type_cd, 
                        cond1=%nrstr( in ('3', 'C')),
						outcol=ANY_MC)

				/* Identify whether any sect_1115a_demo_ind=1 within the given file (will then look across all file types) */

				%any_rec(condcol1=sect_1115a_demo_ind,
                         cond1=%nrstr(='1'),        
                         outcol=sect_1115a_demo_ind_any)
			  
			   /* For all except RX, look if any records with the MH/SUD indicator, and get count of claims (by FFS/MC separately),
			      and sum tot_mdcd_pd_amt for FFS only,  where the given indicator = 1.
				  For txnmy_ind, look if there are any values = 1, 2, 3; */

			   %if &file. ne RX %then %do;

			   	  %any_rec(condcol1=&file._mh_dx_ind,
                           outcol=&file._mh_dx_ind_any)
				  %any_rec(condcol1=&file._sud_dx_ind,
                           outcol=&file._sud_dx_ind_any)

				  %count_rec(condcol1=&file._mh_dx_ind, 
				             condcol2=clm_type_cd, cond2=%str( in ('1','A') ),
                             outcol=&file._ffs_mh_clm)
				  %count_rec(condcol1=&file._mh_dx_ind, 
				             condcol2=clm_type_cd, cond2=%str( in ('3','C') ),
                             outcol=&file._mc_mh_clm)

				  %count_rec(condcol1=&file._sud_dx_ind, 
				             condcol2=clm_type_cd, cond2=%str( in ('1','A') ),
                             outcol=&file._ffs_sud_clm)
				  %count_rec(condcol1=&file._sud_dx_ind, 
				             condcol2=clm_type_cd, cond2=%str( in ('3','C') ),
                             outcol=&file._mc_sud_clm)

				  %sum_paid(condcol1=&file._mh_dx_ind, 
				            condcol2=clm_type_cd, cond2=%str( in ('1','A') ),
                            outcol=&file._ffs_mh_pd)
				  %sum_paid(condcol1=&file._sud_dx_ind, 
				            condcol2=clm_type_cd, cond2=%str( in ('1','A') ),
                            outcol=&file._ffs_sud_pd)

				  %any_rec(condcol1=&file._sud_txnmy_ind, 
                           cond1=%nrstr( in (1,2,3)),
						   outcol=&file._sud_txnmy_ind_any)
				  %any_rec(condcol1=&file._mh_txnmy_ind, 
                           cond1=%nrstr( in (1,2,3)),
						   outcol=&file._mh_txnmy_ind_any)

			   %end;

			   /* For four combinations of claims (MDCD non-xover, SCHIP non-xover, MDCD xovr and SCHIP xovr,
			      get the same counts and totals. Loop over INDS1 (MDCD SCHIP) and INDS2 (NON_XOVR XOVR) to assign
			      the four pairs of records */

			   %do i=1 %to 2;
			   	  %let ind1=%scan(&INDS1.,&i.);
				  %do j=1 %to 2;
				  	 %let ind2=%scan(&INDS2.,&j.);

					 /* Create macro vars to assign claim types for MDCD or SCHIP */

				   	  %assign_toc

				   	  %any_rec(condcol1=&ind1.,
					           condcol2=&ind2.,
							   condcol3=clm_type_cd, cond3=%str( = %nrbquote('&ffsval.')),
							   outcol=&ind1._rcpnt_&ind2._FFS_FLAG )

					  %any_rec(condcol1=&ind1.,
					           condcol2=&ind2.,
							   condcol3=clm_type_cd, cond3=%str( = %nrbquote('&mcval.')),
							   outcol=&ind1._rcpnt_&ind2._MC_FLAG )

					  %sum_paid(condcol1=&ind1.,
					            condcol2=&ind2.,
								condcol3=clm_type_cd, cond3=%str( != %nrbquote('&mcval.')),
								outcol=TOT_&ind1._&ind2._PD)

					  %sum_paid(condcol1=&ind1.,
					            condcol2=&ind2.,
					 		    condcol3=clm_type_cd, cond3=%str( = %nrbquote('&ffsval.')),
						 	    outcol=TOT_&ind1._&ind2._FFS_&file._PD)

					  /* Only count claims for OT and RX - IP and LT will be counted when rolling up to
					     visits/days */

					  %if &file. = OT or &file. = RX %then %do;

						  %count_rec(condcol1=&ind1.,
						             condcol2=&ind2.,
									 condcol3=clm_type_cd, cond3=%str( = %nrbquote('&ffsval.')),
									 outcol=&ind1._&ind2._FFS_&file._CLM) 

						  %count_rec(condcol1=&ind1.,
						             condcol2=&ind2.,
									 condcol3=clm_type_cd, cond3=%str( = %nrbquote('&mcval.')),
								 	 outcol=&ind1._&ind2._MC_&file._CLM )

					  %end;

					  /* For NON_XOVR only, get count of supp claims and sum payments*/ 

					  %if &ind2.=NON_XOVR %then %do;

						  %count_rec(condcol1=&ind1.,
						             condcol2=&ind2.,
						 		     condcol3=clm_type_cd, cond3=%str( = %nrbquote('&suppval.')),
							 	     outcol=&ind1._&ind2._SPLMTL_CLM)

						  %sum_paid(condcol1=&ind1.,
						            condcol2=&ind2.,
						 		    condcol3=clm_type_cd, cond3=%str( = %nrbquote('&suppval.')),
							 	    outcol=TOT_&ind1._&ind2._SPLMTL_PD)


					  %end;

				  %end;
			   %end;


		from &file.h_&year.
		group by submtg_state_cd
		         ,msis_ident_num

	) by tmsis_passthrough;


%mend base_hdr_byfile;

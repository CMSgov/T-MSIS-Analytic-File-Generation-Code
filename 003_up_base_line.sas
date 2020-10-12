/**********************************************************************************************/
/*Program: 003_up_base_line.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Generate counts and sums by bene at the line-level for each claim type to then be 
/*         combined across the four file types for the BASE segment
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro base_line_byfile(file=);

	** Roll-up all claims to the header level, getting line-level col stats. ;

	execute (
		create temp table &file.l_hdr_lvl_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
		       ,msis_ident_num
			   ,&file._link_key

			   	/* For OT only, look at hcbs_srvc_cd - create indicators for each possible value of 1-7.
			       For the other three files, just set all indicators to 0 to be able to union and roll up easily
			       across all file types */

				%if &file. = OT %then %do;

					%do h=1 %to 7;

						%any_rec(condcol1=hcbs_srvc_cd,
						         cond1=%str(=%nrbquote('&h.')),
								 outcol=hcbs_&h._clm_flag)

					%end;

				%end;

				%else %do;

					%do h=1 %to 7;

						,max(0) as hcbs_&h._clm_flag

					%end;

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

					  %sum_paid(condcol1=&ind1.,
					            condcol2=&ind2.,
								condcol3=clm_type_cd, cond3=%str( != %nrbquote('&mcval.')),
								paidcol=mdcd_pd_amt,
								outcol=&ind1._&ind2._PD)

					  %sum_paid(condcol1=&ind1.,
					            condcol2=&ind2.,
								condcol3=clm_type_cd, cond3=%str( = %nrbquote('&mcval.')),
								paidcol=mdcd_ffs_equiv_amt,
								outcol=&ind1._&ind2._FFS_EQUIV_AMT)

					/* Create indicators for any line with TOS = 119, 120, 121, 122  with TOC = 2/B  - will then sum at header-level */

					%any_rec(condcol1=&ind1.,
					         condcol2=&ind2.,
							 condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							 condcol4=tos_cd, cond4=%str( = '119'),
							 outcol=&ind1._&ind2._ANY_MC_CMPRHNSV)

					%any_rec(condcol1=&ind1.,
					         condcol2=&ind2.,
							 condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							 condcol4=tos_cd, cond4=%str( = '120'),
							 outcol=&ind1._&ind2._ANY_MC_PCCM)


					%any_rec(condcol1=&ind1.,
					         condcol2=&ind2.,
							 condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							 condcol4=tos_cd, cond4=%str( = '121'),
							 outcol=&ind1._&ind2._ANY_MC_PVT_INS)

					%any_rec(condcol1=&ind1.,
					         condcol2=&ind2.,
							 condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							 condcol4=tos_cd, cond4=%str( = '122'),
							 outcol=&ind1._&ind2._ANY_MC_PHP)


					 %sum_paid(condcol1=&ind1.,
					           condcol2=&ind2.,
							   condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							   condcol4=tos_cd, cond4=%str( = '119'),
							   paidcol=mdcd_pd_amt,
							   outcol=&ind1._&ind2._MC_CMPRHNSV_PD)

				 	 %sum_paid(condcol1=&ind1.,
					           condcol2=&ind2.,
							   condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							   condcol4=tos_cd, cond4=%str( = '120'),
							   paidcol=mdcd_pd_amt,
							   outcol=&ind1._&ind2._MC_PCCM_PD)

					 %sum_paid(condcol1=&ind1.,
					           condcol2=&ind2.,
							   condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							   condcol4=tos_cd, cond4=%str( = '121'),
							   paidcol=mdcd_pd_amt,
							   outcol=&ind1._&ind2._MC_PVT_INS_PD)

					 %sum_paid(condcol1=&ind1.,
					           condcol2=&ind2.,
							   condcol3=clm_type_cd, cond3=%str( = %nrbquote('&capval.')),
							   condcol4=tos_cd, cond4=%str( = '122'),
							   paidcol=mdcd_pd_amt,
							   outcol=&ind1._&ind2._MC_PHP_PD)

				  %end;
			   %end;


		from &file.l_&year.
		group by submtg_state_cd
		         ,msis_ident_num
				 ,&file._link_key

	) by tmsis_passthrough;


%mend base_line_byfile;

/**********************************************************************************************/
/*Program: 000_pull_de.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Generate counts and sums by bene at the header-level for each claim type to then be 
/*         combined across the four file types for the BASE segment
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro pullde;

   ** Pull needed elements from the DE. For most of the elements, just pull in the value as
      given in the DE.
      For the monthly CHIP code values, create a count of months enrolled in:
         - non-CHIP Medicaid (CHIP_CD=1)
         - MCHIP (CHIP_CD=2)
         - SCHIP (CHIP_CD=3);

	%let CHIPMOS = nonchip_mdcd mchip schip;

	execute (
		create temp table de_&year.
        distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select a.submtg_state_cd
		       ,msis_ident_num
			   ,age_num
			   ,gndr_cd
			   ,race_ethncty_exp_flag
			   ,dual_elgbl_cd_ltst
			   ,chip_cd_ltst
			   ,elgblty_grp_cd_ltst
			   ,masboe_cd_ltst

			   /* Create monthly indicators for each of the three needed CHIP_CD values, and
			      then for each sum, to create the number of months - loop over the three CHIPMOS
			      values above that correspond to output monthly count var names */

			   %do c=1 %to 3;
			   	   %let CHIPMO=%scan(&CHIPMOS.,&c.);

				   %do m=1 %to 12;
				   	 %if &m.<10 %then %let m=0&m.;
					 ,case when chip_cd_&m. = %nrbquote('&c.')
					      then 1 else 0
						  end as chip_cd_&m._&c.
					%end;
					 ,chip_cd_01_&c. + chip_cd_02_&c. + chip_cd_03_&c. + chip_cd_04_&c. + chip_cd_05_&c. + chip_cd_06_&c. + 
					  chip_cd_07_&c. + chip_cd_08_&c. + chip_cd_09_&c. + chip_cd_10_&c. + chip_cd_11_&c. + chip_cd_12_&c.

					as elgblty_&CHIPMO._mos

				%end;

				/* Loop over all values of DUAL_ELGBL_CD, and if any is non-null/00, set dual_elgbl_evr = 1 */

				,case when %do m=1 %to 12;
				   	          %if &m.<10 %then %let m=0&m.;
							  %if &m. > 1 %then %do; or %end;
							  (dual_elgbl_cd_&m. is not null and dual_elgbl_cd_&m. != '00')
							%end;
					 then 1 else 0
					 end as dual_elgbl_evr



		from max_run_id_de_&year. a
		     inner join
			 &DA_SCHEMA..taf_ann_de_base b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.de_fil_dt = b.de_fil_dt and
		   a.da_run_id = b.da_run_id

		where misg_elgblty_data_ind != 1 or misg_elgblty_data_ind is null

	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(max_run_id_de_&year.)



%mend pullde;

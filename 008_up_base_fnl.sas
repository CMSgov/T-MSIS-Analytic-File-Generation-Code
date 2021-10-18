/**********************************************************************************************/
/*Program: 008_up_base_fnl.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Join all the bene-level tables created in prior programs to create the final bene-level
/*         UP BASE and insert into the permanent table 
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro up_base_fnl;

	** Using the header-aggregated bene-level table from 002 as the base, join to that: 
         - DE bene-level table (b)
         - line-aggregated bene-level table (c)
         - LT days bene-level table table (d)
         - IP stays bene-level tables (e and f)
         - Delivery bene-level file (g) ;

	execute (
		create temp table base_fnl_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select a.*

			   /* DE columns */ 
		       ,b.age_num
		       ,b.gndr_cd
			   ,b.race_ethncty_exp_flag
			   ,b.dual_elgbl_cd_ltst
			   ,b.dual_elgbl_evr
			   ,b.chip_cd_ltst
               ,b.elgblty_grp_cd_ltst
			   ,b.masboe_cd_ltst
			   ,elgblty_nonchip_mdcd_mos
			   ,elgblty_mchip_mos
			   ,elgblty_schip_mos

			   ,case when b.msis_ident_num is null 
                     then 1 else 0
					 end as misg_elgblty_flag
			         
			   /* line-aggregated columns */


			   %do i=1 %to 2;
				  %let ind1=%scan(&INDS1.,&i.);
				  %do j=1 %to 2;
				   	 %let ind2=%scan(&INDS2.,&j.);

					 ,c.&ind1._&ind2._PD
					 ,coalesce(c.&ind1._&ind2._MC_CMPRHNSV_CLM,0) as &ind1._&ind2._MC_CMPRHNSV_CLM
					 ,coalesce(c.&ind1._&ind2._MC_PCCM_CLM,0) as &ind1._&ind2._MC_PCCM_CLM
					 ,coalesce(c.&ind1._&ind2._MC_PVT_INS_CLM,0) as &ind1._&ind2._MC_PVT_INS_CLM
					 ,coalesce(c.&ind1._&ind2._MC_PHP_CLM,0) as &ind1._&ind2._MC_PHP_CLM
					 ,c.&ind1._&ind2._FFS_EQUIV_AMT
					 ,c.&ind1._&ind2._MC_CMPRHNSV_PD
					 ,c.&ind1._&ind2._MC_PCCM_PD
					 ,c.&ind1._&ind2._MC_PVT_INS_PD
					 ,c.&ind1._&ind2._MC_PHP_PD

					,coalesce(c.&ind1._&ind2._MDCR_CLM,0) as &ind1._&ind2._MDCR_CLM
					,c.&ind1._&ind2._MDCR_PD
					,coalesce(c.&ind1._&ind2._OTHR_CLM,0) as &ind1._&ind2._OTHR_CLM
					,c.&ind1._&ind2._OTHR_PD
					,coalesce(c.&ind1._&ind2._HH_CLM,0) as &ind1._&ind2._HH_CLM
					,c.&ind1._&ind2._HH_PD

				  %end;
				%end;

				%do h=1 %to 7;
					%let hcbsval=%scan(&HCBSVALS.,&h.);

					,coalesce(c.hcbs_&hcbsval._clm_flag,0) as hcbs_&hcbsval._clm_flag

				%end;

			   /* LT columns */ 

			   %do i=1 %to 2;
			   	  %let ind1=%scan(&INDS1.,&i.);
				  %do j=1 %to 2;
				  	 %let ind2=%scan(&INDS2.,&j.);

					 ,coalesce(d.&ind1._&ind2._FFS_LT_DAYS,0) as &ind1._&ind2._FFS_LT_DAYS
					 ,coalesce(d.&ind1._&ind2._MC_LT_DAYS,0) as &ind1._&ind2._MC_LT_DAYS

				  %end;
				%end;

				/* IP columns */

				%do i=1 %to 2;
			   	  %let ind1=%scan(&INDS1.,&i.);
				  %do j=1 %to 2;
				  	 %let ind2=%scan(&INDS2.,&j.);

					 ,coalesce(&ind1._&ind2._FFS_IP_DAYS,0) as &ind1._&ind2._FFS_IP_DAYS
					 ,coalesce(&ind1._&ind2._MC_IP_DAYS,0) as &ind1._&ind2._MC_IP_DAYS

					 ,coalesce(&ind1._&ind2._FFS_IP_STAYS,0) as &ind1._&ind2._FFS_IP_STAYS
					 ,coalesce(&ind1._&ind2._MC_IP_STAYS,0) as &ind1._&ind2._MC_IP_STAYS

				  %end;
				%end;

				/* Delivery ind */

				,case when gndr_cd='F' and age_num > 9
				      then coalesce(g.dlvry_ind,'0')
					  else '0'
					  end as dlvry_ind


		from hdr_bene_base_&year. a
		     left join
			 de_&year. b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.msis_ident_num = b.msis_ident_num

		   left join
           line_bene_base_&year. c

		on a.submtg_state_cd = c.submtg_state_cd and
		   a.msis_ident_num = c.msis_ident_num

		   left join
		   lt_hdr_days2_&year. d

		   on a.submtg_state_cd = d.submtg_state_cd and
		      a.msis_ident_num = d.msis_ident_num

		   left join
		   ip_stays_days_mdcd e

		   on a.submtg_state_cd = e.submtg_state_cd and
		      a.msis_ident_num = e.msis_ident_num 

		   left join
		   ip_stays_days_schip f

		   on a.submtg_state_cd = f.submtg_state_cd and
		      a.msis_ident_num = f.msis_ident_num 
		      
		   left join
		   bene_deliv_&year. g

		   on a.submtg_state_cd = g.submtg_state_cd and
		      a.msis_ident_num = g.msis_ident_num 

	) by tmsis_passthrough;

	** Insert into permanent table;
	execute (
		insert into &DA_SCHEMA..TAF_ANN_UP_BASE

		(DA_RUN_ID, UP_LINK_KEY, UP_FIL_DT, ANN_UP_VRSN, SUBMTG_STATE_CD, MSIS_IDENT_NUM %basecols)
	
		select
			%table_id_cols
			%basecols

		from base_fnl_&year.

	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(hdr_bene_base_&year. de_&year. line_bene_base_&year. lt_hdr_days2_&year. 
                 ip_stays_days_mdcd ip_stays_days_schip bene_deliv_&year. base_fnl_&year.)


%mend up_base_fnl;

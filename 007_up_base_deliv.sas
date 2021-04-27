/**********************************************************************************************/
/*Program: 007_up_base_deliv.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: Read in IP and OT files, join to lookup tables, and identify benes with delivery
/*         claims
/*Mod: 
/*Notes: 
/**********************************************************************************************/
/* Copyright (C) Mathematica Policy Research, Inc.                                            */
/* This code cannot be copied, distributed or used without the express written permission     */
/* of Mathematica Policy Research, Inc.                                                       */ 
/**********************************************************************************************/

%macro base_deliv;

	** Include lookup text files to create lookup tables;

	%include "&basedir./data/dgns_cd_lookup.txt";
	%include "&basedir./data/prcdr_cd_lookup.txt";
	%include "&basedir./data/rev_cd_lookup.txt";

	** Pull in both IP and OT line-level files, and join to lookup tables to identify delivery
       diagnosis/procedure/rev codes. In inner query, create indicator for line having any maternal/newborn
       code. In outer query, aggregate to bene-level and get max of maternal/newborn indicators;

	%macro join_del_lists(file, diag_cols, prcdr_cols);

		execute (
			create temp table &file._deliv_&year. 
			distkey(msis_ident_num)
        	sortkey(submtg_state_cd,msis_ident_num) as

			select submtg_state_cd
		           ,msis_ident_num
				   ,max(maternal) as maternal_&file.
				   ,max(newborn) as newborn_&file.

			from (

				select submtg_state_cd
			           ,msis_ident_num
				        %do d=1 %to %sysfunc(countw(&diag_cols.));
					 		%let diag=%scan(&diag_cols.,&d.);

							,d&d..newborn as &diag._newborn

							,d&d..maternal as &diag._maternal

						%end;

						%do p=1 %to %sysfunc(countw(&prcdr_cols.));
					 		%let prcdr=%scan(&prcdr_cols.,&p.);

							,p&p..newborn as &prcdr._newborn

							,p&p..maternal as &prcdr._maternal

						%end;


						,r.newborn as rev_newborn

						,r.maternal as rev_maternal

						/* Create a final set of indicators that looks at ALL the above cols */

						,case when rev_maternal=1
						      %do i=1 %to %sysfunc(countw(&diag_cols. &prcdr_cols.));
					 			 %let col=%scan(&diag_cols. &prcdr_cols.,&i.);

								 or &col._maternal=1

							  %end;
							  then 1 else 0
							  end as maternal

						,case when rev_newborn=1
						      %do i=1 %to %sysfunc(countw(&diag_cols. &prcdr_cols.));
					 			 %let col=%scan(&diag_cols. &prcdr_cols.,&i.);

								 or &col._newborn=1

							  %end;
							  then 1 else 0
							  end as newborn

				from &file.l_&year. a
				     
				     %do d=1 %to %sysfunc(countw(&diag_cols.));
					 	%let diag=%scan(&diag_cols.,&d.);

						left join
						dgns_cd_lookup d&d.

						on a.&diag. = d&d..dgns_cd and
						   a.&diag._ind = d&d..dgns_cd_ind
						   

					 %end;

					 %do p=1 %to %sysfunc(countw(&prcdr_cols.));
					 	%let prcdr=%scan(&prcdr_cols.,&p.);

						left join
						prcdr_cd_lookup p&p.

						on a.&prcdr. = p&p..prcdr_cd

					 %end;

					 left join
					 rev_cd_lookup r

					 on a.rev_cd = r.rev_cd

				)

			group by submtg_state_cd
			         ,msis_ident_num

		) by tmsis_passthrough;

		** Drop line-level files (no longer needed);

		%drop_tables(&file.l_&year.)

	%mend join_del_lists;

	%join_del_lists(IP,
                    diag_cols=admtg_dgns_cd dgns_1_cd dgns_2_cd dgns_3_cd dgns_4_cd dgns_5_cd
	                          dgns_6_cd dgns_7_cd dgns_8_cd dgns_9_cd dgns_10_cd dgns_11_cd dgns_12_cd,

			        prcdr_cols=prcdr_1_cd prcdr_2_cd prcdr_3_cd prcdr_4_cd prcdr_5_cd prcdr_6_cd)

	%join_del_lists(OT,
                    diag_cols=dgns_1_cd dgns_2_cd,
			        prcdr_cols=prcdr_cd);

	** Drop tables no longer needed;

	%drop_tables(dgns_cd_lookup)
	%drop_tables(prcdr_cd_lookup)
	%drop_tables(rev_cd_lookup)

	** Now join IP and OT bene-level files together and create delivery indicator based on any maternal and/or newborn claim.
	   When we join all bene-level tables together, will need to reset to '0' for any bene with gndr_cd != F or age_num < 10;

	execute (
		create temp table bene_deliv_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select coalesce(a.submtg_state_cd, b.submtg_state_cd) as submtg_state_cd
		       ,coalesce(a.msis_ident_num, b.msis_ident_num) as msis_ident_num

			   ,case when maternal_ip=1 or maternal_ot=1
			         then 1 else 0
					 end as maternal

				,case when newborn_ip=1 or newborn_ot=1
			         then 1 else 0
					 end as newborn

				,case when maternal=1 and newborn=0
				      then '1'
					  when maternal=1 and newborn=1
					  then '2'
					  when maternal=0 and newborn=1
					  then '3'
					  else '0'
					  end as dlvry_ind

		from ip_deliv_&year. a
		     full join
			 ot_deliv_&year. b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.msis_ident_num = b.msis_ident_num

	) by tmsis_passthrough;

	** Drop tables no longer needed;

	%drop_tables(ip_deliv_&year. ot_deliv_&year.);


%mend base_deliv;

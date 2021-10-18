/**********************************************************************************************/
/*Program: 009_up_top.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 02/2019
/*Purpose: For each claims file separately, sum the total number of claims and tot paid amount
/*         from the headers by bene, pgm_type_cd, and clm_type_cd. 
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro top_by_file(file=,f=);

	** Roll-up all header-level cols for given file type;

	execute (
		create temp table &file.h_bene_top_&year. 
		distkey(msis_ident_num)
        sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
		       ,msis_ident_num
			   ,pgm_type_cd
			   ,%nrbquote('&f.') as file_type
			   ,clm_type_cd
			   ,count(submtg_state_cd) as clm_tot
			   ,sum(tot_mdcd_pd_amt) as sum_tot_mdcd_pd

		from &file.h_&year.
		group by submtg_state_cd
		         ,msis_ident_num
				 ,pgm_type_cd
			     ,file_type
			     ,clm_type_cd

	) by tmsis_passthrough;

	%drop_tables(&file.h_&year.);



%mend top_by_file;

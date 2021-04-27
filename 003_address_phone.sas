/**********************************************************************************************/
/*Program: 003_address_phone.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual DE segment 003: Address/Phone
/*Mod: 
/*Notes: 
/**********************************************************************************************/

%macro create_CNTCT_DTLS;

	/* Create the name part of the segment, pulling in only those elements for which we do 
       NOT look to the prior year */

	%create_temp_table(name,
	              subcols=%nrbquote(  %last_best(ELGBL_1ST_NAME)
	                                  %last_best(ELGBL_LAST_NAME)
	                                  %last_best(ELGBL_MDL_INITL_NAME)

				 ) );

	/* Now pull in address and phone, for which we DO look to prior year */

	%macro address_phone(runyear);

		%create_temp_table(address_phone,
		              inyear=&runyear.,
					  subcols=%nrbquote(  %monthly_array(ELGBL_LINE_1_ADR_HOME)
		                                  %monthly_array(ELGBL_LINE_2_ADR_HOME)
		                                  %monthly_array(ELGBL_LINE_3_ADR_HOME)
		                                  %monthly_array(ELGBL_CITY_NAME_HOME)
		                                  %monthly_array(ELGBL_ZIP_CD_HOME)
		                                  %monthly_array(ELGBL_CNTY_CD_HOME)
		                                  %monthly_array(ELGBL_STATE_CD_HOME)
		                                  
		                                  %monthly_array(ELGBL_LINE_1_ADR_MAIL)
		                                  %monthly_array(ELGBL_LINE_2_ADR_MAIL)
		                                  %monthly_array(ELGBL_LINE_3_ADR_MAIL)
		                                  %monthly_array(ELGBL_CITY_NAME_MAIL)
		                                  %monthly_array(ELGBL_ZIP_CD_MAIL)
		                                  %monthly_array(ELGBL_CNTY_CD_MAIL)
		                                  %monthly_array(ELGBL_STATE_CD_MAIL)
		                                  
		                                  %last_best(ELGBL_PHNE_NUM_HOME)
		                                  
		                                  %nonmiss_month(ELGBL_LINE_1_ADR_HOME)
		                                  %nonmiss_month(ELGBL_LINE_1_ADR_MAIL)
		                                  
		              ),
		              
		              outercols=%nrbquote(  %address_flag
		                                    %assign_nonmiss_month(ELGBL_LINE_1_ADR,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_LINE_1_ADR_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_LINE_1_ADR_MAIL)
		                                    %assign_nonmiss_month(ELGBL_LINE_2_ADR,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_LINE_2_ADR_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_LINE_2_ADR_MAIL)
		                                    %assign_nonmiss_month(ELGBL_LINE_3_ADR,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_LINE_3_ADR_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_LINE_3_ADR_MAIL)
		                                    %assign_nonmiss_month(ELGBL_CITY_NAME,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_CITY_NAME_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_CITY_NAME_MAIL)
		                                    %assign_nonmiss_month(ELGBL_ZIP_CD,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_ZIP_CD_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_ZIP_CD_MAIL)
		                                    %assign_nonmiss_month(ELGBL_CNTY_CD,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_CNTY_CD_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_CNTY_CD_MAIL)
		                                    %assign_nonmiss_month(ELGBL_STATE_CD,ELGBL_LINE_1_ADR_HOME_MN,ELGBL_STATE_CD_HOME,monthval2=ELGBL_LINE_1_ADR_MAIL_MN,incol2=ELGBL_STATE_CD_MAIL)
		              
		              ) );

	%mend address_phone;

	%address_phone(&year.);

	/* If getprior=1 (have prior year(s) of TAF to get prior information for), run the above for all prior years AND
	   then combine for those demographics only */ 

    %if &getprior.=1 %then %do;
		%do p=1 %to %sysfunc(countw(&pyears.));
	 		%let pyear=%scan(&pyears.,&p.);
			%address_phone(&pyear.)
		%end; 

		/* Join current and prior year(s) and first, identify year pulled for latest non-null value of ELGBL_LINE_1_ADR. 
		   Use that year to then take value for all cols */

		execute (
			create temp table address_phone_&year._out
			distkey(msis_ident_num)
			sortkey(submtg_state_cd,msis_ident_num) as

			select c.submtg_state_cd,
			       c.msis_ident_num

				   %last_best(ELGBL_LINE_1_ADR,prior=1)

				   ,case when c.ELGBL_LINE_1_ADR is not null then &year.
				         %do p=1 %to %sysfunc(countw(&pyears.));
	 		      			  %let pyear=%scan(&pyears.,&p.);
							  when p&p..ELGBL_LINE_1_ADR is not null then &pyear.
						 %end;
						 else null
						 end as yearpull

				   %address_same_year(ELGBL_ADR_MAIL_FLAG)
				   %address_same_year(ELGBL_LINE_2_ADR)
				   %address_same_year(ELGBL_LINE_3_ADR)

				   %address_same_year(ELGBL_CITY_NAME) 
				   %address_same_year(ELGBL_ZIP_CD)
				   %address_same_year(ELGBL_CNTY_CD)
				   %address_same_year(ELGBL_STATE_CD)

				   %last_best(ELGBL_PHNE_NUM_HOME,prior=1)

	  	 from address_phone_&year. c
		      %do p=1 %to %sysfunc(countw(&pyears.));
	 		      %let pyear=%scan(&pyears.,&p.);

		  	      left join
		  	      address_phone_&pyear. p&p.
		  	      
			  	 on c.submtg_state_cd = p&p..submtg_state_cd and
			  	    c.msis_ident_num = p&p..msis_ident_num  
			  %end;	
	  	
	  	) by tmsis_passthrough;

	%end; /* end getprior=1 loop */

	/* If getprior=0, simply rename current year address_phone table */

	%if &getprior.=0 %then %do;

		execute (
			alter table address_phone_&year. rename to address_phone_&year._out
		) by tmsis_passthrough;

	%end;

	/* Join name segment to phone/address */

	execute (
		create temp table name_address_phone_&year.
		distkey(msis_ident_num)
		sortkey(submtg_state_cd,msis_ident_num) as

		select a.submtg_state_cd,
		       a.msis_ident_num,
			   a.ELGBL_1ST_NAME,
			   a.ELGBL_LAST_NAME, 
			   a.ELGBL_MDL_INITL_NAME, 
			   b.ELGBL_ADR_MAIL_FLAG,
			   b.ELGBL_LINE_1_ADR, 
			   b.ELGBL_LINE_2_ADR, 
			   b.ELGBL_LINE_3_ADR, 
			   b.ELGBL_CITY_NAME, 
			   b.ELGBL_ZIP_CD,
			   b.ELGBL_CNTY_CD,
			   b.ELGBL_STATE_CD,
			   b.ELGBL_PHNE_NUM_HOME
		
			from name_&year. a
			     inner join
				 address_phone_&year._out b

			on a.submtg_state_cd = b.submtg_state_cd and
			   a.msis_ident_num = b.msis_ident_num

	) by tmsis_passthrough;

	/* Insert into final table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select 

			%table_id_cols
			,ELGBL_1ST_NAME 
			,ELGBL_LAST_NAME 
			,ELGBL_MDL_INITL_NAME 
			,ELGBL_ADR_MAIL_FLAG
			,ELGBL_LINE_1_ADR 
			,ELGBL_LINE_2_ADR 
			,ELGBL_LINE_3_ADR 
			,ELGBL_CITY_NAME 
			,ELGBL_ZIP_CD
			,ELGBL_CNTY_CD
			,ELGBL_STATE_CD
			,ELGBL_PHNE_NUM_HOME

		from name_address_phone_&year.

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(name_&year. name_address_phone_&year. address_phone_&year._out
	             %if &getprior.=1 %then %do;
                      address_phone_&year. 
					  %do p=1 %to %sysfunc(countw(&pyears.));
	 		      		   %let pyear=%scan(&pyears.,&p.);
                   		   address_phone_&pyear. 
                      %end;
                 %end; )

	              
%mend create_CNTCT_DTLS;

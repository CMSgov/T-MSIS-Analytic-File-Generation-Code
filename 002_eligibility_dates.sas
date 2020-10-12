/**********************************************************************************************/
/*Program: 002_eligibility_dates.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual DE segment 002: Eligibility Dates
/*Notes: This program reads in the 16 monthly effective/end date pairs for both Medicaid and CHIP,
/*       and takes from wide to long and combines/dedups overlapping or duplicated spells.
/*       It then inserts this temp table with one record/enrollee/spell into the permanent table.
/*       In the process, it creates monthly and yearly counts of enrolled days (Medicaid and 
/*       CHIP separately) to be added to the Base segment. This temp table is called
/*       enrolled_days_#YR and is the only temp table not deleted at the end of the program. 
/*Mod:   Mod for CCB Q3 to truncate dates before first day or after last day of month TO THAT DAY
/**********************************************************************************************/


%macro create_ELDTS;

	/* Read in all eligibility dates - all 16 slots from all 12 months. The monthly_array_eldts macro
       truncates anything BEFORE the first of the month to the first of the month, and anything AFTER
       the last day of the month to the last day of the month. Must
       run each element as a separate subcol param because of macro text length issues */

	%create_temp_table(eligibility_dates,
	                subcols=%nrbquote( %monthly_array_eldts(MDCD_ENRLMT_EFF_DT_,nslots=16,truncfirst=1)

					),
	                subcols2=%nrbquote( %monthly_array_eldts(MDCD_ENRLMT_END_DT_,nslots=16,truncfirst=0) 
					),
	                subcols3=%nrbquote( %monthly_array_eldts(CHIP_ENRLMT_EFF_DT_,nslots=16,truncfirst=1)
					),
	                subcols4=%nrbquote( %monthly_array_eldts(CHIP_ENRLMT_END_DT_,nslots=16,truncfirst=0)
					) );

	/* Create dummy table with one record per slot/month to join to dates and get to long form */

	  execute(
	   	create temp table numbers
	   	   (slot int, month varchar(2));

		   insert into numbers
		   	values
	   	
				   	%do s=1 %to 16;
				   		%do m=1 %to 12;
				   			%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
				   			
					   		(&s.,%nrbquote('&m.')) %if &s. < 16 or &m. < 12 %then %do; , %end;	  
					   		
					   	%end;
				   		
				    %end; ;
	   
	   ) by tmsis_passthrough;

   /* Run through dates twice - for MDCD and CHIP */
		       

	%macro dates(dtype,dval);

		/* Take dates to long form by joining to numbers and then taking the month/slot corresponding to the merged
	       number record. */

		execute(
		   	create temp table &dtype._dates_long 
	        distkey(msis_ident_num) 
	        sortkey(submtg_state_cd,msis_ident_num)  as

			   select submtg_state_cd
		   		       ,msis_ident_num
					   ,&dtype._ENRLMT_EFF_DT
					   ,&dtype._ENRLMT_END_DT
				from (
		   		select *
		   		       ,case %do s=1 %to 16;
		   		          		 %do m=1 %to 12;
		   			      	     	 %if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
		   			      	
		   		                  when slot=&s. and month=%nrbquote('&m.') then &dtype._ENRLMT_EFF_DT_&s._&m.
		   		                  
		   		               %end;
		   		             %end; 
		   		        end as &dtype._ENRLMT_EFF_DT
		   		        
		   		      ,case %do s=1 %to 16;
		   		          		 %do m=1 %to 12;
		   			      	     	 %if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
		   			      	
		   		                  when slot=&s. and month=%nrbquote('&m.') then &dtype._ENRLMT_END_DT_&s._&m.
		   		                  
		   		               %end;
		   		             %end; 
		   		        end as &dtype._ENRLMT_END_DT
		     from  
		   	
		   	(select a.submtg_state_cd
		   	       ,a.msis_ident_num
		   	       
		   	       %do s=1 %to 16;
		   	       	  %do m=1 %to 12;
		   			      	%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
		   			
		   	       	  	,a.&dtype._ENRLMT_EFF_DT_&s._&m.
						,a.&dtype._ENRLMT_END_DT_&s._&m.
		   	       	  	   	       	  
		   	       	  %end;  	       
		   	       
		   	       %end;
		   	       
		   	       ,b.slot
		   	       ,b.month
		   	       
		   	from eligibility_dates_&year. a
		   	     join
		   	     numbers b
		   	     on true) sub ) sub2

			where &dtype._ENRLMT_EFF_DT is not null

			order by submtg_state_cd,
	                 msis_ident_num,
					 &dtype._ENRLMT_EFF_DT, 
		             &dtype._ENRLMT_END_DT
		   	    
		   ) by tmsis_passthrough;


		/* Now join all records back to each other for each enrollee, so each eff/end date is matched with each of
		   the other eff/end dates for the same enrollee - keep records where eff date is >= other eff date AND
		   end date <= other end date.  */

		execute (
		  	 create temp table &dtype._ids 
	         distkey(dateId) 
	         sortkey(submtg_state_cd,msis_ident_num) as

			 select *

			 /* Create a unique date ID to filter on later */

			 		,trim(submtg_state_cd ||'-'||msis_ident_num || '-' ||cast(row_number() over 
	                   (partition by submtg_state_cd, msis_ident_num
				        order by submtg_state_cd, msis_ident_num, &dtype._ENRLMT_EFF_DT, &dtype._ENRLMT_END_DT) 
	                    as char(3))) as dateId

			 from &dtype._dates_long 

		 ) by tmsis_passthrough;

		execute (
		    create temp table &dtype._overlaps 
		     distkey(dateid) 
		     sortkey(dateid) as

			 select t1.*
			 from &dtype._ids t1 
				  inner join 
			      &dtype._ids t2

			 /* Join records for beneficiary to each other, but omit matches where it's the same record */

		         on t1.submtg_state_cd = t2.submtg_state_cd and
				    t1.msis_ident_num = t2.msis_ident_num and
				    t1.dateId <> t2.dateId

			/* Get every dateID where their effective date is greater than or equal to another record's effective date
				AND their end date is less than or equal to that other record's end date. */

			 where date_cmp(t1.&dtype._ENRLMT_EFF_DT,t2.&dtype._ENRLMT_EFF_DT) in (0,1) and
			       date_cmp(t1.&dtype._ENRLMT_END_DT,t2.&dtype._ENRLMT_END_DT) in (-1,0)

		) by tmsis_passthrough;

		execute (
		    create temp table &dtype._nonoverlaps 
		    distkey(msis_ident_num) 
		    sortkey(submtg_state_cd,msis_ident_num) as

			 select t1.*

			 /* Join initial date to overlapping dateIDs and remove */

			 from &dtype._ids t1

			      left join 
		          &dtype._overlaps t2

		       on t1.dateid = t2.dateid

		  	   where t2.dateid is null     

		 ) by tmsis_passthrough;

		execute (
		     create temp table &dtype._dates_out 
		     distkey(msis_ident_num) 
		     sortkey(submtg_state_cd,msis_ident_num) as

			 select submtg_state_cd
		           ,msis_ident_num
				   ,&dval. as ENRL_TYPE_FLAG
			       ,min(&dtype._ENRLMT_EFF_DT) as &dtype._ENRLMT_EFF_DT
				   ,max(&dtype._ENRLMT_END_DT) as &dtype._ENRLMT_END_DT

			 from
			 (
			   select submtg_state_cd, 
		       msis_ident_num,
		       &dtype._ENRLMT_EFF_DT,
		       &dtype._ENRLMT_END_DT
			   ,sum(C) over (partition by submtg_state_cd, msis_ident_num
					         order by &dtype._ENRLMT_EFF_DT, &dtype._ENRLMT_END_DT
		                     rows UNBOUNDED PRECEDING) as G
			 from
			 (
			 select submtg_state_cd
			       ,msis_ident_num
				   ,&dtype._ENRLMT_EFF_DT
				   ,&dtype._ENRLMT_END_DT
				   ,m_eff_dt
				   ,m_end_dt
				   ,decode(sign(&dtype._ENRLMT_EFF_DT-nvl(m_end_dt+1,&dtype._ENRLMT_EFF_DT)),1,1,0) as C
			 from

		     (select submtg_state_cd
			       ,msis_ident_num
				   ,&dtype._ENRLMT_EFF_DT
				   ,&dtype._ENRLMT_END_DT
				   ,lag(&dtype._ENRLMT_EFF_DT) over (partition by submtg_state_cd, msis_ident_num
				                                     order by &dtype._ENRLMT_EFF_DT, &dtype._ENRLMT_END_DT) 
		                                             as m_eff_dt
				   ,lag(&dtype._ENRLMT_END_DT) over (partition by submtg_state_cd, msis_ident_num
				                                     order by &dtype._ENRLMT_EFF_DT, &dtype._ENRLMT_END_DT)     
		                                             as m_end_dt
			 from &dtype._nonoverlaps
			      order by &dtype._ENRLMT_EFF_DT, &dtype._ENRLMT_END_DT) s1 ) s2 ) s3 

				  group by submtg_state_cd, msis_ident_num, g
	     
		 ) by tmsis_passthrough;


		/* Now for the base segment, must count the number of enrolled days per month, then sum to get yearly. For
		   each segment, count the number of enrolled days in each month - will then combine across records to
		   get total month and total year values */


		execute (
			create temp table &dtype._enrolled_days
		    distkey(msis_ident_num) 
		    sortkey(submtg_state_cd,msis_ident_num) as

			select submtg_state_cd,
			       msis_ident_num,
				   &dtype._ENRLMT_EFF_DT,
				   &dtype._ENRLMT_END_DT

			/* Loop through months and compare effective date to first day of month, and end date to last day of month.
			   If at all within month, count the number of days */

			%do m=1 %to 12;
			   	%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 

		        %if &m. in (01 03 05 07 08 10 12)               %then %let lday=31;
				%if &m. in (09 04 06 11)                        %then %let lday=30;
				%if &m. = 02 and &year. in (2016 2020 2024)     %then %let lday=29;
				%else %if &m. = 02 %then %let lday=28;
		
				,case when date_cmp(&dtype._ENRLMT_EFF_DT,to_date(%nrbquote('&lday. &m. &year.'),'dd mm yyyy')) in (-1,0) and
				          date_cmp(&dtype._ENRLMT_END_DT,to_date(%nrbquote('01 &m. &year.'),'dd mm yyyy')) in (0,1) then

					datediff(day,greatest(&dtype._ENRLMT_EFF_DT,to_date(%nrbquote('01 &m. &year.'),'dd mm yyyy')),
					         least(&dtype._ENRLMT_END_DT,to_date(%nrbquote('&lday. &m. &year.'),'dd mm yyyy'))) + 1

					else 0
					end as &dtype._ENRLMT_DAYS_&m.

			%end;

			from &dtype._dates_out


		) by tmsis_passthrough;

		/* Now sum across records within months, and then across all months to get total year enrollment */

		execute (
			create temp table &dtype._days_out
			distkey(msis_ident_num) 
		    sortkey(submtg_state_cd,msis_ident_num) as

			select *,
			       &dtype._ENRLMT_DAYS_01 + &dtype._ENRLMT_DAYS_02 + &dtype._ENRLMT_DAYS_03 + &dtype._ENRLMT_DAYS_04 +
				   &dtype._ENRLMT_DAYS_05 + &dtype._ENRLMT_DAYS_06 + &dtype._ENRLMT_DAYS_07 + &dtype._ENRLMT_DAYS_08 +
				   &dtype._ENRLMT_DAYS_09 + &dtype._ENRLMT_DAYS_10 + &dtype._ENRLMT_DAYS_11 + &dtype._ENRLMT_DAYS_12
				   as &dtype._ENRLMT_DAYS_YR

			from
			(select submtg_state_cd,
			        msis_ident_num
					%do m=1 %to 12;
					   	%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
						,sum(&dtype._ENRLMT_DAYS_&m.) as &dtype._ENRLMT_DAYS_&m.
					%end;
			from &dtype._enrolled_days
			group by submtg_state_cd,
			         msis_ident_num )

		) by tmsis_passthrough;

		/* Drop some temp tables */

		%drop_tables(&dtype._dates_long &dtype._ids &dtype._overlaps
				     &dtype._nonoverlaps s&dtype._enrolled_days)

	%mend dates;

	%dates(MDCD,1)
	%dates(CHIP,2)

	/* Now union the MDCD and CHIP effective/end dates for the eligibility dates segment */

	execute (
		create temp table dates_out
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		(select msis_ident_num,
                submtg_state_cd,
                ENRL_TYPE_FLAG,
                MDCD_ENRLMT_EFF_DT as ENRLMT_EFCTV_CY_DT,
                MDCD_ENRLMT_END_DT as ENRLMT_END_CY_DT
         from MDCD_dates_out)

		union all

		(select msis_ident_num,
                submtg_state_cd,
                ENRL_TYPE_FLAG,
                CHIP_ENRLMT_EFF_DT as ENRLMT_EFCTV_CY_DT,
                CHIP_ENRLMT_END_DT as ENRLMT_END_CY_DT
         from CHIP_dates_out)

	) by tmsis_passthrough;

	/* Insert into permanent table */

	execute(
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select 
			%table_id_cols
			,ENRL_TYPE_FLAG
			,ENRLMT_EFCTV_CY_DT
			,ENRLMT_END_CY_DT

		from dates_out

	) by tmsis_passthrough;

	/* Now join the monthly and yearly enrolled counts to then keep to join to base.
	   Also create flag EL_DTS_SPLMTL which = 1 for all records (and will be set to 0 for
	   those not on the table when joined to base). */

	execute (
		create temp table enrolled_days_&year.
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num
               ,coalesce(a.submtg_state_cd,b.submtg_state_cd) as submtg_state_cd
			   %do m=1 %to 12;
					%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
					,a.MDCD_ENRLMT_DAYS_&m.
					,b.CHIP_ENRLMT_DAYS_&m.
				%end;
				,a.MDCD_ENRLMT_DAYS_YR
				,b.CHIP_ENRLMT_DAYS_YR

				,1 as EL_DTS_SPLMTL

		from MDCD_days_out a
		     full outer join
		     CHIP_days_out b
		
		on a.msis_ident_num = b.msis_ident_num and
		   a.submtg_state_cd = b.submtg_state_cd


	) by tmsis_passthrough;

	/* Drop temp tables (but must KEEP table with monthly and yearly enrollment to join to base */

	%drop_tables(eligibility_dates_&year. MDCD_dates_out CHIP_dates_out MDCD_days_out CHIP_days_out numbers)
                   

%mend create_ELDTS;

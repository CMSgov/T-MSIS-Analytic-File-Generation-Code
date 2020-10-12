/**********************************************************************************************/
/*Program: 006_waiver.sas
/*Author: Rosalie Malsberger, Mathematica Policy Research
/*Date: 05/2018
/*Purpose: Generate the annual BSF segment 006: Waiver
/*Mod: 
/*Notes: This program arrays out all waiver slots for every month. Also, it creates counts of
/*       enrolled months for each type of waiver (based on a given type in ANY of the monthly slots
/*       for each month). To create _1915C_WVR_TYPE and _1115_WVR_TYPE (latest 1915C and 1115
/*       waivers), must take waivers from wide to long to look across months, and then join
/*       back to the main table.
/*       It then inserts this temp table into the permanent table and deletes the temp table.
/*       It also creates the flag WAIVER_SPLMTL which = 1 if ANY ID or type in the year is
/*       non-null, which will be kept in a temp table to be joined to the base segment.
/**********************************************************************************************/

%macro create_WVR;

	%create_temp_table(waiver,
	              subcols=%nrbquote(  /* Create monthly indicators for each type of Waiver to then sum 
				                       in the outer query. Note this will run for months 1-3 (which is the
				                       text limit of the macro var), and then the other months will run in 
				                       below additional subcol macro vars. */ 

									  %run_waiv_slots(1,3)
	              	              
	                                  %monthly_array(WVR_ID,nslots=&nwaivslots.)
	                                  %monthly_array(WVR_TYPE_CD,nslots=&nwaivslots.) 
                                      %monthly_array(SECT_1115A_DEMO_IND) 
				  ),
				  subcols2=%nrbquote( 
                                     %run_waiv_slots(4,6)
 				  ),
				  subcols3=%nrbquote( 
                                     %run_waiv_slots(7,9)
			 	  ),
				  subcols4=%nrbquote( 
                                     %run_waiv_slots(10,12)

			      ), 

				   /* Create _SPLMTL flag = 1 if ANY ID or type are non-missing (must put in separate subcol
				      var because of text limit) */

				  subcols5=%nrbquote( 
                                     %waiv_nonnull(WAIVER_SPLMTL)

			      ),
	              
	              outercols=%nrbquote(  %sum_months(_1115_PHRMCY_PLUS_WVR)
	                                    %sum_months(_1115_DSTR_REL_WVR)
	                                    %sum_months(_1115_FP_ONLY_WVR)
	                                    %sum_months(_1915C_WVR)
	                                    %sum_months(_1915BC_WVR)
	                                    %sum_months(_1915B_WVR)
	                                    %sum_months(_1115_OTHR_WVR)
	                                    %sum_months(_1115_HIFA_WVR)
	                                    %sum_months(_OTHR_WVR)
	              
	               ) );

	/* Create temp table with just WAIVER_SPLMTL to join to base */

	execute(
		create temp table WAIVER_SPLMTL_&year. 
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

		select submtg_state_cd
	   		   ,msis_ident_num
			   ,WAIVER_SPLMTL

		from waiver_&year.

	) by tmsis_passthrough;
	               
               
/* Take the arrayed waiver types from the waiver table and deduplicate types by month,
   to get a count of the unique types within each category of 1915c or 1115 for each month.
   Then going backwards in months from 12-01, identify the latest type seen for each category.
   If >=2 of the same category in the latest month, assign a value of 89. */
   
  /* First create temp table with one row per slot # per month, to be full joined back to waivers */

  execute(
   	create temp table numbers
   	   (slot int, month varchar(2));

	   insert into numbers
	   	values
   	
			   	%do waiv=1 %to &nwaivslots.;
			   		%do m=1 %to 12;
			   			%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
			   			
				   		(&waiv.,%nrbquote('&m.')) %if &waiv. < &nwaivslots. or &m. < 12 %then %do; , %end;	  
				   		
				   	%end;
			   		
			    %end; ;
   
   ) by tmsis_passthrough;
   
   /* In subquery, run a full join on the slots and waivers to create one record per enrollee per slot per 
      month, and then in the outer query select each slot to be put into the reassgned WVR_TYPE_CD column.
      Create a WAIVER_CAT column that = 1 for 1915c, = 2 for 1115, and = 0 otherwise. 
      Select DISTINCT bene/month/type (excluding slot value) */
   
   execute(
   	create temp table waiver_long 
	distkey(msis_ident_num) 
	sortkey(submtg_state_cd,msis_ident_num) as

	select distinct
               submtg_state_cd
   		       ,msis_ident_num
   		       ,month
			   ,WVR_TYPE_CD
			   ,WAIVER_CAT 
	from (
   		select *
   		       ,case %do waiv=1 %to &nwaivslots.;
   		          		 %do m=1 %to 12;
   			      	     	 %if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
   			      	
   		                  when slot=&waiv. and month=%nrbquote('&m.') then WVR_TYPE_CD&waiv._&m.
   		                  
   		               %end;
   		             %end; 
   		        end as WVR_TYPE_CD
   		        
   		      ,case %do waiv=1 %to &nwaivslots.;
   		          		 %do m=1 %to 12;
   			      	     	 %if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
   			      	
   		                  when slot=&waiv. and month=%nrbquote('&m.') then WAIVER_CAT&waiv._&m.
   		                  
   		               %end;
   		             %end; 
   		        end as WAIVER_CAT
     from  
   	
   	(select a.submtg_state_cd
   	       ,a.msis_ident_num
   	       
   	       %do waiv=1 %to &nwaivslots.;
   	       	  %do m=1 %to 12;
   			      	%if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
   			
   	       	  	,a.WVR_TYPE_CD&waiv._&m.
   	       	  	,case when a.WVR_TYPE_CD&waiv._&m. is not null and ((a.WVR_TYPE_CD&waiv._&m. >= '06' 
                           and a.WVR_TYPE_CD&waiv._&m. <= '20') or WVR_TYPE_CD&waiv._&m.='33')  then 1
   	       	  	      when a.WVR_TYPE_CD&waiv._&m. is not null  
                           and (a.WVR_TYPE_CD&waiv._&m. = '01' or (a.WVR_TYPE_CD&waiv._&m. >= '22' 
                           and a.WVR_TYPE_CD&waiv._&m. <= 30)) then 2
   	       	  	      else 0
   	       	  	      end as WAIVER_CAT&waiv._&m. 
   	       	  	   	       	  
   	       	  %end;  	       
   	       
   	       %end;
   	       
   	       ,b.slot
   	       ,b.month
   	       
   	from waiver_&year. a
   	     join
   	     numbers b
   	     on true) sub ) sub2

	where WAIVER_CAT > 0

   ) by tmsis_passthrough;

   
   /* Take the above waiver_long table and identify first and last unique 1915c and 1115 per month,
      using window function so values are appended back, so that
      we know the # of types per month AND the types themselves (in the case of one type in the month).
      If the first and last are not equal but both not null, set to 89 (multiples).
      Set the initial values of the types to integer so we can take max.*/
      
  execute(
   	create temp table waiver_counts 
	distkey(msis_ident_num) 
	sortkey(submtg_state_cd,msis_ident_num) as
   
	   select submtg_state_cd
	          ,msis_ident_num
	          ,month
			  ,FIRST_WVR_TYPE_CD
			  ,LAST_WVR_TYPE_CD
	          ,case when FIRST_WVR_TYPE_CD = LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                     WAIVER_CAT=1 then WVR_TYPE_CD

	                when FIRST_WVR_TYPE_CD != LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                     LAST_WVR_TYPE_CD is not NULL and WAIVER_CAT=1 then '89'
	                else null
	                end as _1915C_WVR_TYPE
	                
	          ,case when FIRST_WVR_TYPE_CD = LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                      WAIVER_CAT=2 then WVR_TYPE_CD
	                when FIRST_WVR_TYPE_CD != LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                      LAST_WVR_TYPE_CD is not NULL and WAIVER_CAT=2 then '89'
	                else null
	                end as _1115_WVR_TYPE
	                
	    from               
	                
	   
	   (select *
	           ,first_value( WVR_TYPE_CD ignore nulls) over (partition by submtg_state_cd
	                                                           ,msis_ident_num
	                                                           ,month
	                                                           ,WAIVER_CAT) as FIRST_WVR_TYPE_CD

				,last_value( WVR_TYPE_CD ignore nulls) over (partition by submtg_state_cd
	                                                           ,msis_ident_num
	                                                           ,month
	                                                           ,WAIVER_CAT) as LAST_WVR_TYPE_CD
	     from waiver_long) as num
	     
	  ) by tmsis_passthrough;

	execute(
	 	create temp table waiver_latest
		distkey(msis_ident_num) 
		sortkey(submtg_state_cd,msis_ident_num) as

	 	select enrl.submtg_state_cd
	 	          ,enrl.msis_ident_num
	 	          ,coalesce(m12._1915C_WVR_TYPE,m11._1915C_WVR_TYPE,m10._1915C_WVR_TYPE,
	 	                    m09._1915C_WVR_TYPE,m08._1915C_WVR_TYPE,m07._1915C_WVR_TYPE,
	 	                    m06._1915C_WVR_TYPE,m05._1915C_WVR_TYPE,m04._1915C_WVR_TYPE,
	 	                    m03._1915C_WVR_TYPE,m02._1915C_WVR_TYPE,m01._1915C_WVR_TYPE)
	 	                    
	 	           as _1915C_WVR_TYPE
	 	           
	 	          ,coalesce(m12._1115_WVR_TYPE,m11._1115_WVR_TYPE,m10._1115_WVR_TYPE,
	 	                    m09._1115_WVR_TYPE,m08._1115_WVR_TYPE,m07._1115_WVR_TYPE,
	 	                    m06._1115_WVR_TYPE,m05._1115_WVR_TYPE,m04._1115_WVR_TYPE,
	 	                    m03._1115_WVR_TYPE,m02._1115_WVR_TYPE,m01._1115_WVR_TYPE)
	 	                    
	 	           as _1115_WVR_TYPE
	 	          
	 	   from
	 	   
	 	    (select submtg_state_cd,
			          msis_ident_num,
			          count(msis_ident_num) as nmonths
			         
				   from waiver_counts     
					    
					 group by submtg_state_cd,
				            msis_ident_num) as enrl
					 
	 
	      %do m=1 %to 12;
   			    %if %sysfunc(length(&m.)) = 1 %then %let m=%sysfunc(putn(&m.,z2)); 
   			    
   			    left join
   			    
   			    (select submtg_state_cd
   			            ,msis_ident_num
   			            ,max(_1915C_WVR_TYPE) as _1915C_WVR_TYPE
   			            ,max(_1115_WVR_TYPE) as _1115_WVR_TYPE
   			     from waiver_counts
   			     where month=%nrbquote('&m.')
   			     
   			     group by submtg_state_cd
   			              ,msis_ident_num) as m&m.
   			     
   			     on enrl.submtg_state_cd = m&m..submtg_state_cd and
   			        enrl.msis_ident_num = m&m..msis_ident_num
   			     
   			%end;
	
	 ) by tmsis_passthrough;

	 /* Join latest waivers back to main table, subset to WAIVER_SPLMTL=1 */

	 execute (
	 	create temp table waiver_out
		distkey(msis_ident_num)
		sortkey(submtg_state_cd,msis_ident_num) as
		select a.*,
		       b._1915C_WVR_TYPE,
			   b._1115_WVR_TYPE

		from waiver_&year. a
		     full join
			 waiver_latest b

		on a.submtg_state_cd = b.submtg_state_cd and
		   a.msis_ident_num = b.msis_ident_num

		where WAIVER_SPLMTL=1

	 ) by tmsis_passthrough;
	 
	/* Insert into permanent table */

	execute (
		insert into &DA_SCHEMA..TAF_ANN_DE_&tblname.
		select 

			%table_id_cols
			,_1915C_WVR_TYPE 
			,_1115_WVR_TYPE 
			,_1115_PHRMCY_PLUS_WVR_MOS
			,_1115_DSTR_REL_WVR_MOS
			,_1115_FP_ONLY_WVR_MOS
			,_1915C_WVR_MOS
			,_1915BC_WVR_MOS
			,_1915B_WVR_MOS
			,_1115_OTHR_WVR_MOS
			,_1115_HIFA_WVR_MOS
			,_OTHR_WVR_MOS
			,SECT_1115A_DEMO_IND_01
			,SECT_1115A_DEMO_IND_02
			,SECT_1115A_DEMO_IND_03
			,SECT_1115A_DEMO_IND_04
			,SECT_1115A_DEMO_IND_05
			,SECT_1115A_DEMO_IND_06
			,SECT_1115A_DEMO_IND_07
			,SECT_1115A_DEMO_IND_08
			,SECT_1115A_DEMO_IND_09
			,SECT_1115A_DEMO_IND_10
			,SECT_1115A_DEMO_IND_11
			,SECT_1115A_DEMO_IND_12
			,WVR_ID1_01
			,WVR_ID1_02
			,WVR_ID1_03
			,WVR_ID1_04
			,WVR_ID1_05
			,WVR_ID1_06
			,WVR_ID1_07
			,WVR_ID1_08
			,WVR_ID1_09
			,WVR_ID1_10
			,WVR_ID1_11
			,WVR_ID1_12
			,WVR_TYPE_CD1_01
			,WVR_TYPE_CD1_02
			,WVR_TYPE_CD1_03
			,WVR_TYPE_CD1_04
			,WVR_TYPE_CD1_05
			,WVR_TYPE_CD1_06
			,WVR_TYPE_CD1_07
			,WVR_TYPE_CD1_08
			,WVR_TYPE_CD1_09
			,WVR_TYPE_CD1_10
			,WVR_TYPE_CD1_11
			,WVR_TYPE_CD1_12
			,WVR_ID2_01
			,WVR_ID2_02
			,WVR_ID2_03
			,WVR_ID2_04
			,WVR_ID2_05
			,WVR_ID2_06
			,WVR_ID2_07
			,WVR_ID2_08
			,WVR_ID2_09
			,WVR_ID2_10
			,WVR_ID2_11
			,WVR_ID2_12
			,WVR_TYPE_CD2_01
			,WVR_TYPE_CD2_02
			,WVR_TYPE_CD2_03
			,WVR_TYPE_CD2_04
			,WVR_TYPE_CD2_05
			,WVR_TYPE_CD2_06
			,WVR_TYPE_CD2_07
			,WVR_TYPE_CD2_08
			,WVR_TYPE_CD2_09
			,WVR_TYPE_CD2_10
			,WVR_TYPE_CD2_11
			,WVR_TYPE_CD2_12
			,WVR_ID3_01
			,WVR_ID3_02
			,WVR_ID3_03
			,WVR_ID3_04
			,WVR_ID3_05
			,WVR_ID3_06
			,WVR_ID3_07
			,WVR_ID3_08
			,WVR_ID3_09
			,WVR_ID3_10
			,WVR_ID3_11
			,WVR_ID3_12
			,WVR_TYPE_CD3_01
			,WVR_TYPE_CD3_02
			,WVR_TYPE_CD3_03
			,WVR_TYPE_CD3_04
			,WVR_TYPE_CD3_05
			,WVR_TYPE_CD3_06
			,WVR_TYPE_CD3_07
			,WVR_TYPE_CD3_08
			,WVR_TYPE_CD3_09
			,WVR_TYPE_CD3_10
			,WVR_TYPE_CD3_11
			,WVR_TYPE_CD3_12
			,WVR_ID4_01
			,WVR_ID4_02
			,WVR_ID4_03
			,WVR_ID4_04
			,WVR_ID4_05
			,WVR_ID4_06
			,WVR_ID4_07
			,WVR_ID4_08
			,WVR_ID4_09
			,WVR_ID4_10
			,WVR_ID4_11
			,WVR_ID4_12
			,WVR_TYPE_CD4_01
			,WVR_TYPE_CD4_02
			,WVR_TYPE_CD4_03
			,WVR_TYPE_CD4_04
			,WVR_TYPE_CD4_05
			,WVR_TYPE_CD4_06
			,WVR_TYPE_CD4_07
			,WVR_TYPE_CD4_08
			,WVR_TYPE_CD4_09
			,WVR_TYPE_CD4_10
			,WVR_TYPE_CD4_11
			,WVR_TYPE_CD4_12
			,WVR_ID5_01
			,WVR_ID5_02
			,WVR_ID5_03
			,WVR_ID5_04
			,WVR_ID5_05
			,WVR_ID5_06
			,WVR_ID5_07
			,WVR_ID5_08
			,WVR_ID5_09
			,WVR_ID5_10
			,WVR_ID5_11
			,WVR_ID5_12
			,WVR_TYPE_CD5_01
			,WVR_TYPE_CD5_02
			,WVR_TYPE_CD5_03
			,WVR_TYPE_CD5_04
			,WVR_TYPE_CD5_05
			,WVR_TYPE_CD5_06
			,WVR_TYPE_CD5_07
			,WVR_TYPE_CD5_08
			,WVR_TYPE_CD5_09
			,WVR_TYPE_CD5_10
			,WVR_TYPE_CD5_11
			,WVR_TYPE_CD5_12
			,WVR_ID6_01
			,WVR_ID6_02
			,WVR_ID6_03
			,WVR_ID6_04
			,WVR_ID6_05
			,WVR_ID6_06
			,WVR_ID6_07
			,WVR_ID6_08
			,WVR_ID6_09
			,WVR_ID6_10
			,WVR_ID6_11
			,WVR_ID6_12
			,WVR_TYPE_CD6_01
			,WVR_TYPE_CD6_02
			,WVR_TYPE_CD6_03
			,WVR_TYPE_CD6_04
			,WVR_TYPE_CD6_05
			,WVR_TYPE_CD6_06
			,WVR_TYPE_CD6_07
			,WVR_TYPE_CD6_08
			,WVR_TYPE_CD6_09
			,WVR_TYPE_CD6_10
			,WVR_TYPE_CD6_11
			,WVR_TYPE_CD6_12
			,WVR_ID7_01
			,WVR_ID7_02
			,WVR_ID7_03
			,WVR_ID7_04
			,WVR_ID7_05
			,WVR_ID7_06
			,WVR_ID7_07
			,WVR_ID7_08
			,WVR_ID7_09
			,WVR_ID7_10
			,WVR_ID7_11
			,WVR_ID7_12
			,WVR_TYPE_CD7_01
			,WVR_TYPE_CD7_02
			,WVR_TYPE_CD7_03
			,WVR_TYPE_CD7_04
			,WVR_TYPE_CD7_05
			,WVR_TYPE_CD7_06
			,WVR_TYPE_CD7_07
			,WVR_TYPE_CD7_08
			,WVR_TYPE_CD7_09
			,WVR_TYPE_CD7_10
			,WVR_TYPE_CD7_11
			,WVR_TYPE_CD7_12
			,WVR_ID8_01
			,WVR_ID8_02
			,WVR_ID8_03
			,WVR_ID8_04
			,WVR_ID8_05
			,WVR_ID8_06
			,WVR_ID8_07
			,WVR_ID8_08
			,WVR_ID8_09
			,WVR_ID8_10
			,WVR_ID8_11
			,WVR_ID8_12
			,WVR_TYPE_CD8_01
			,WVR_TYPE_CD8_02
			,WVR_TYPE_CD8_03
			,WVR_TYPE_CD8_04
			,WVR_TYPE_CD8_05
			,WVR_TYPE_CD8_06
			,WVR_TYPE_CD8_07
			,WVR_TYPE_CD8_08
			,WVR_TYPE_CD8_09
			,WVR_TYPE_CD8_10
			,WVR_TYPE_CD8_11
			,WVR_TYPE_CD8_12
			,WVR_ID9_01
			,WVR_ID9_02
			,WVR_ID9_03
			,WVR_ID9_04
			,WVR_ID9_05
			,WVR_ID9_06
			,WVR_ID9_07
			,WVR_ID9_08
			,WVR_ID9_09
			,WVR_ID9_10
			,WVR_ID9_11
			,WVR_ID9_12
			,WVR_TYPE_CD9_01
			,WVR_TYPE_CD9_02
			,WVR_TYPE_CD9_03
			,WVR_TYPE_CD9_04
			,WVR_TYPE_CD9_05
			,WVR_TYPE_CD9_06
			,WVR_TYPE_CD9_07
			,WVR_TYPE_CD9_08
			,WVR_TYPE_CD9_09
			,WVR_TYPE_CD9_10
			,WVR_TYPE_CD9_11
			,WVR_TYPE_CD9_12
			,WVR_ID10_01
			,WVR_ID10_02
			,WVR_ID10_03
			,WVR_ID10_04
			,WVR_ID10_05
			,WVR_ID10_06
			,WVR_ID10_07
			,WVR_ID10_08
			,WVR_ID10_09
			,WVR_ID10_10
			,WVR_ID10_11
			,WVR_ID10_12
			,WVR_TYPE_CD10_01
			,WVR_TYPE_CD10_02
			,WVR_TYPE_CD10_03
			,WVR_TYPE_CD10_04
			,WVR_TYPE_CD10_05
			,WVR_TYPE_CD10_06
			,WVR_TYPE_CD10_07
			,WVR_TYPE_CD10_08
			,WVR_TYPE_CD10_09
			,WVR_TYPE_CD10_10
			,WVR_TYPE_CD10_11
			,WVR_TYPE_CD10_12

		from waiver_out

	) by tmsis_passthrough;

	/* Delete temp tables */

	%drop_tables(waiver_&year. numbers waiver_long waiver_counts waiver_latest waiver_out)          


%mend create_WVR;

 
   

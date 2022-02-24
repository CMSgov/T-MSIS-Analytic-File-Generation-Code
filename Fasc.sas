%macro fasc_code(fl=);
title;
**************************************************************************;
* Pull Header.                                                           *;
**************************************************************************;
execute(
   create temp table &fl._header_0 as
   select  
   		   submtg_state_cd
		  ,msis_ident_num
		  ,da_run_id
          ,&fl._fil_dt as fil_dt
          ,&fl._link_key 
    /*      ,ltst_run_ind   */
          ,&fl._vrsn as vrsn
          ,%nrbquote('&fl') as file_type
          ,clm_type_cd
          ,srvc_trkng_type_cd
          ,srvc_trkng_pymt_amt
		  ,blg_prvdr_txnmy_cd

          %if "&fl." = "ip"  %then %do;
          ,mdcd_dsh_pd_amt
		  ,case when (mdcd_dsh_pd_amt is not null and mdcd_dsh_pd_amt !=0)
                then 1 else 0 end as non_msng_dsh_pd
          %end;

          ,tot_mdcd_pd_amt
		  ,blg_prvdr_npi_num

		  %if "&fl."="rx" %then %do;
		  ,cmpnd_drug_ind 
		  %end;

          %if "&fl." ne "rx" %then %do;
          ,dgns_1_cd
          ,dgns_2_cd
          
          ,bill_type_cd
	   	  ,case when length(bill_type_cd)=3 and substring(bill_type_cd,1,1) != '0' then '0'||substring(bill_type_cd,1,3)
				 when length(bill_type_cd)=4 and bill_type_cd not in ('0000') then bill_type_cd
				 else null
				end as bill_type_cd_upd

		  ,case when dgns_1_cd is null then 1 else 0 end as dgns_1_cd_null
          %end;
          %if "&fl." = "ot" %then %do;
          ,srvc_plc_cd
          %end;
		  ,num_cll
		  ,case when clm_type_cd in ('1','A','U') then '1_FFS'
		        when clm_type_cd in ('2','B','V') then '2_CAP'
				when clm_type_cd in ('3','C','W') then '3_ENC'
				when clm_type_cd in ('4','D','X') then '4_SRVC_TRKG'
                when clm_type_cd in ('5','E','Y') then '5_SUPP'
		        else null end as clm_type_grp_ctgry 
         
   from &fl.H

) by tmsis_passthrough;
title;
**************************************************************************;
* Pull Line.                                                             *;
**************************************************************************;
execute(
   create temp table &fl._lne as
   select  
   		   submtg_state_cd
		  ,msis_ident_num
          ,da_run_id
		  ,line_num
          ,&fl._fil_dt as fil_dt
          ,&fl._link_key 
     /*     ,ltst_run_ind  */
          ,&fl._vrsn as vrsn
          ,mdcd_pd_amt
          ,xix_srvc_ctgry_cd
          ,tos_cd
          ,xxi_srvc_ctgry_cd
		  ,bnft_type_cd 
		 
          %if "&fl." ne "rx" %then %do;
		  ,srvcng_prvdr_txnmy_cd
          ,rev_cd
		  ,min(case when rev_cd is null then 1 else 0 end) 
           over (partition by submtg_state_cd,&fl._link_key) as all_null_rev_cd

		  ,max(case when rev_cd in ('0510','0511','0512','0513','0514','0515',
                                    '0516','0517','0518','0519','0520','0521',
                                    '0522','0523','0524','0525','0526','0527',
                                    '0528','0529') then 1 else 0 end)
	 	   over (partition by submtg_state_cd,&fl._link_key) as ever_clinic_rev

		   ,max(case when rev_cd in ('0650', '0651', '0652', '0653', '0654', '0655',
                                     '0656', '0657', '0658', '0659',
                                     '0115', '0125', '0135', '0145') then 1 else 0 end)

	 	   over (partition by submtg_state_cd,&fl._link_key) as ever_hospice_rev

		   ,min(case when rev_cd in (&vs_HH_Rev_cd.) /*('0023') or
		                  substring(rev_cd,1,3) in ('056','057','058','059') */ then 1 
                     when rev_cd is not null then 0 
                     else null end) 		                  
	 	    over (partition by submtg_state_cd,&fl._link_key) as only_hh_rev

          %end;
          %if "&fl." = "ot" %then %do;
		  ,hcbs_txnmy
          ,prcdr_cd
		  ,hcpcs_rate
          ,srvcng_prvdr_num
          ,srvcng_prvdr_npi_num
		  ,min(case when prcdr_cd is null then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as all_null_prcdr_cd

		  ,min(case when hcpcs_rate is null then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as all_null_hcpcs_cd

		  ,max(case when prcdr_cd is null and 
                         hcpcs_rate is null then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_null_prcdr_hcpcs_cd

		  ,max(case when prcdr_cd is not null or
                         hcpcs_rate is not null then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_valid_prcdr_hcpcs_cd

		  ,min(case when prcdr_cd  in (&vs_HH_Proc_cd.) or 
						 hcpcs_rate in (&vs_HH_Proc_cd.) then 1 
                    when prcdr_cd is null and hcpcs_rate is null then null
                    else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as  only_hh_procs
          %end;
          %if "&fl." = "rx"  %then %do;
          ,ndc_cd
		  ,min(case when ndc_cd is null then 1 else 0 end) 
           over (partition by submtg_state_cd,&fl._link_key) as all_null_ndc_cd

          ,max(case when ((length(ndc_cd) =10 or
                           length(ndc_cd) =11 ) and
						   ndc_cd !~ '([^0-9])' )
                    then 1 else 0 end)
           over (partition by submtg_state_cd,&fl._link_key) as ever_valid_ndc

          %end;
		 
		  ,max(case when bnft_type_cd in ('039') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_icf_bnft_typ

		  ,max(case when tos_cd in ('123','131','135') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_dsh_drg_ehr_tos

		 ,max(case when tos_cd in ('119','120','121','122') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_cap_pymt_tos

		  ,max(case when tos_cd in ('119','120','121','122','138','139',
                                   '140','141','142','143','144') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_cap_tos

		  ,max(case when tos_cd in ('119','122') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_php_tos

		  ,max(case when tos_cd in ('123') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_dsh_tos

		  ,max(case when tos_cd in ('131') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_drg_rbt_tos

		 ,max(case when tos_cd in ('036','018') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_dme_hhs_tos

		  ,max(case when xix_srvc_ctgry_cd in ('001B','002B') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_dsh_xix_srvc_ctgry

		  ,max(case when xix_srvc_ctgry_cd in ('07A1','07A2','07A3','07A4','07A5','07A6') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_othr_fin_xix_srvc_ctgry

	      ,max(case when CLL_STUS_CD in ('542','585','654') then 1 else 0 end) 
            over (partition by submtg_state_cd,&fl._link_key) as ever_denied_line

   from &fl.L
) by tmsis_passthrough;

**************************************************************************;
* Combine header and line.                                               *;
**************************************************************************;
execute(
   create temp table &fl._combined as
   select 
	    b.*
	    ,case when %misslogic(msis_ident_num,len(trim(msis_ident_num))) then 1 else 0 end as inval_msis_id
	   	,case when (cmc_php =0  and 
	                    other_pmpm =0 and 
	                    dsh_flag =0  and
	                    clm_type_cd is null and
	                    ever_cap_pymt_tos=1)
			  then 1 else 0 end as cap_tos_null_toc

		,case when (cmc_php =0  and 
	                    other_pmpm =0 and 
	                    dsh_flag =0  and
	                    clm_type_cd in ('4','D','X') )
			  then 1 else 0 end as srvc_trkg	 

		,case when (cmc_php =0  and 
	                    other_pmpm =0 and 
	                    dsh_flag =0  and
	                    clm_type_cd in ('5','E','Y') and
	                   ( %misslogic(msis_ident_num,len(trim(msis_ident_num))) or

	                	 %if "&fl." = "ip" or "&fl." = "lt" %then %do;
	                   (bill_type_cd_upd is null and 
	                    dgns_1_cd is null)
						%end;
					
						%if "&fl." = "ot" %then %do;
	                  	(all_null_rev_cd =1 and 
	                  	 all_null_prcdr_cd=1 and 
	                   	 all_null_hcpcs_cd =1)
						%end;
						%if "&fl." = "rx" %then %do;
	                  	(all_null_ndc_cd =1)
						%end;
					  ) )
	            then 1 else 0 end as supp_clms

				,row_number() over (partition by submtg_state_cd,
			                                     &fl._link_key
	                       order by submtg_state_cd,&fl._link_key)
	                       as rec_cnt
   	from (select 
	           a.*
			   %if "&fl." ne "rx" %then %do;
			   
			   ,case when cmc_php =0  and 
	                      other_pmpm =0 and 
                        ((clm_type_cd in ('4','D','X','5','E','Y') and 
	                       (srvc_trkng_type_cd in ('02') or 
						    ever_dsh_tos=1)) 
	                      or

						  (clm_type_cd in ('4','X','5','Y') and 
							 ever_dsh_xix_srvc_ctgry=1)

	                     %if "&fl." = "ip"  %then %do;
	                      or
						  (clm_type_cd in ('1') and
						   (mdcd_dsh_pd_amt is not null or mdcd_dsh_pd_amt !=0 ) and 
						   ever_dsh_xix_srvc_ctgry=1
	                       )
	                     %end; )
					then 1 else 0 end as dsh_flag      

			   %end;
			   %if "&fl." = "rx"  %then %do;
			   ,0 as dsh_flag
			   %end;
	

		   from ( select 
		          h.*
		          ,l.line_num
		          ,l.mdcd_pd_amt
		          ,l.xix_srvc_ctgry_cd
		          ,l.tos_cd
		          ,l.xxi_srvc_ctgry_cd
				  ,l.bnft_type_cd
				  ,l.ever_icf_bnft_typ
				   %if "&fl." ne "rx" %then %do;
		          ,l.rev_cd
				  ,l.all_null_rev_cd
				  ,case when bill_type_cd_upd is null then 1 else 0 end as upd_bill_type_cd_null
				  ,substring(bill_type_cd_upd,2,1) as bill_typ_byte2
		          ,substring(bill_type_cd_upd,3,1) as bill_typ_byte3
				  ,l.ever_clinic_rev
				  ,l.ever_hospice_rev
				  ,l.only_hh_rev
				  ,l.srvcng_prvdr_txnmy_cd
		           %end;
		           %if "&fl." = "ot" %then %do;
				  ,case when h.srvc_plc_cd is null then 1 else 0 end as srvc_plc_cd_null
				  ,l.hcbs_txnmy
		          ,l.prcdr_cd
				  ,l.hcpcs_rate
				  ,l.all_null_prcdr_cd
				  ,l.all_null_hcpcs_cd
				  ,l.ever_null_prcdr_hcpcs_cd
				  ,l.ever_valid_prcdr_hcpcs_cd
		          ,l.srvcng_prvdr_num
		          ,l.srvcng_prvdr_npi_num
				  ,l.only_hh_procs
		          %end;
		          %if "&fl." = "rx"  %then %do;
		          ,l.ndc_cd
				  ,l.all_null_ndc_cd
				  ,l.ever_valid_ndc

		          %end;
				   ,l.ever_dsh_drg_ehr_tos
				   ,l.ever_cap_tos
				   ,l.ever_cap_pymt_tos
				   ,l.ever_php_tos
				   ,l.ever_dsh_tos
				   ,l.ever_drg_rbt_tos
				   ,l.ever_dme_hhs_tos
				   ,l.ever_dsh_xix_srvc_ctgry
				   ,l.ever_othr_fin_xix_srvc_ctgry
				   ,l.ever_denied_line

		          /**CMC PHP and PMPM **/

		 		   ,case when (srvc_trkng_type_cd not in ('01') or srvc_trkng_type_cd is null) and
							%if "&fl." = "ot" %then %do;
		                       (
								( clm_type_cd in ('2','B','V') and 
		                          (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
		                         ) OR
								 ( clm_type_cd in ('4','D','X','5','E','Y') and 
		                          (ever_cap_tos=1)
		                          )  
		                         ) and
		                    %end; 

		                    %if "&fl." = "ip" or "&fl." = "lt" %then %do;
							  	( clm_type_cd in ('2','B','V') and 
		                          (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
		                         ) and
		                      (bill_type_cd_upd is null and dgns_1_cd is null ) and  
		                    %end;
							%if "&fl." = "rx" %then %do;
							   	( clm_type_cd in ('2','B','V') and 
		                          (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
		                         ) and
		                        (cmpnd_drug_ind is null and all_null_ndc_cd=1 ) and  
		                    %end;   
							   (ever_php_tos=1)
						   then 1 else 0 end as cmc_php
				              

					,case when (srvc_trkng_type_cd not in ('01') or srvc_trkng_type_cd is null) and
							  %if "&fl." = "ot" %then %do;
			                     (
			                       ( clm_type_cd in ('2','B','V') and 
			                         (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
			                        ) OR
									( clm_type_cd in ('4','D','X','5','E','Y') and 
			                           (ever_cap_tos=1)
			                         )
			                       ) and
		                       %end; 

		                       %if "&fl." = "ip" or "&fl." = "lt" %then %do;
							  	 ( ( clm_type_cd in ('2','B','V') and 
		                            (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
		                           ) and
		                      	   (bill_type_cd_upd is null and dgns_1_cd is null )
		                         ) and  
		                       %end;
							   %if "&fl." = "rx" %then %do;
							   	( ( clm_type_cd in ('2','B','V') and 
		                           (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
		                          ) and
		                         (cmpnd_drug_ind is null and all_null_ndc_cd=1 ) 
		                        ) and  
		                       %end;   
								(ever_php_tos=0 or ever_php_tos is null)

							then 1 else 0 end as other_pmpm

		   from &fl._header_0 h 
		        left join
				&fl._lne l

		   on h.submtg_state_cd=l.submtg_state_cd and
		      h.&fl._link_key=l.&fl._link_key ) a ) b

) by tmsis_passthrough;

title;
**************************************************************************;
* Create variables for Non-financial claims    *;
**************************************************************************;
execute(
   create temp table &fl._lne_flag_tos_cat as
   select *
          
         
		  %if "&fl." = "ot" %then %do;
        

		 ,max(dental_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as dental_clms
		 ,sum(dental_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as dental_lne_cnts

		 ,max(trnsprt_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as trnsprt_clms
		 ,sum(trnsprt_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as trnsprt_lne_cnts

 		 ,max(othr_hcbs_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as othr_hcbs_clms
		 ,sum(othr_hcbs_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as othr_hcbs_lne_cnts
 
		 ,min(case when Lab_lne_clms=1 then 1
		           when prcdr_cd is null and hcpcs_rate is null then null
                   else 0 end) over (partition by submtg_state_cd,&fl._link_key) as Lab_clms

        /* ,max(Lab_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as Lab_clms*/
		 ,sum(Lab_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as Lab_lne_cnts

		 ,min(case when Rad_lne_clms=1 then 1
		           when prcdr_cd is null and hcpcs_rate is null then null
                   else 0 end) over (partition by submtg_state_cd,&fl._link_key) as Rad_clms

     	/* ,max(Rad_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as Rad_clms */
		 ,sum(Rad_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as Rad_lne_cnts

         ,max(DME_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as DME_clms
         ,sum(DME_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as DME_lne_cnts
 
        %end;
       
       %if "&fl."="rx" %then %do;
        
		,max(DME_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as DME_clms
		,sum(DME_lne_clms) over (partition by submtg_state_cd,&fl._link_key) as DME_lne_cnts
	   %end;
		  
   from (select 
          		a.*
				%if "&fl."="lt" %then %do;
			     ,b.prvdr_txnmy_icf
				 ,b.prvdr_txnmy_nf
				 ,b.prvdr_txnmy_othr_res

			   %end;
		       %if "&fl." = "ot" %then %do;
		         ,b.code_cat as prcdr_ccs_cat
				 ,c.code_cat as hcpcs_ccs_cat

		         ,case when  not_fin_clm=1 and
                            (all_null_rev_cd=0 or 
		                    (bill_type_cd_upd is not null and srvc_plc_cd is null))
		               then 1 else 0 end as inst_clms

		         ,case when not_fin_clm=1 and
                            (
                            ((all_null_rev_cd=1 or all_null_rev_cd is null) and /**headers with no line included*/
		                     bill_type_cd_upd is not null and 
		                     srvc_plc_cd is not null) or

		                    ((all_null_rev_cd=1 or all_null_rev_cd is null) and 
		                     bill_type_cd_upd is null and 
		                     srvc_plc_cd is not null) or

							 ((all_null_rev_cd=1 or all_null_rev_cd is null) and 
		                     bill_type_cd_upd is null and 
		                     srvc_plc_cd is null and
		                     (ever_null_prcdr_hcpcs_cd =0)/**ALL prcdr code OR HCPCS is non-null*/
                             
                              ) 
                             )
					   then 1 else 0 end as prof_clms

 				,case when not_fin_clm=1 and
                            (
                            ((all_null_rev_cd=1 or all_null_rev_cd is null) and /**headers with no line included*/
		                     bill_type_cd_upd is not null and 
		                     srvc_plc_cd is not null) or

		                    ((all_null_rev_cd=1 or all_null_rev_cd is null) and 
		                     bill_type_cd_upd is null and 
		                     srvc_plc_cd is not null) or

							 ((all_null_rev_cd=1 or all_null_rev_cd is null) and 
		                     bill_type_cd_upd is null and 
		                     srvc_plc_cd is null and
		                     (ever_valid_prcdr_hcpcs_cd =1 ) /**At least one non-null HCPCS or PRCDR code **/
                             ) 
                             )
					   then 1 else 0 end as prof_clms_2

				 ,case when not_fin_clm=1 and
                            ((length(prcdr_cd)=5 and substring(prcdr_cd,1,1)='D') or 
		                        (length(hcpcs_rate)=5 and substring(hcpcs_rate,1,1)='D')
							)
					   then 1 else 0 end as dental_lne_clms

				 ,case when not_fin_clm=1 and
                            (b.code_cat ='Transprt' or 
		                     c.code_cat ='Transprt') 
		               then 1 else 0 end as trnsprt_lne_clms

				 ,case when not_fin_clm=1 and
                           (substring(rev_cd,1,3) in ('066','310') or 
		                         prcdr_cd in (&vs_Othr_HCBS_Proc_cd.) or
								  hcpcs_rate in (&vs_Othr_HCBS_Proc_cd.) or
							     (prcdr_cd in ('T2025') and srvc_plc_cd in ('12') ) or
								 (hcpcs_rate in ('T2025') and srvc_plc_cd in ('12') ) or
							     bnft_type_cd in ('045') or 
							     hcbs_txnmy in (&vs_Othr_HCBS_Taxo.)
                           )
						   then 1 else 0 end as othr_hcbs_lne_clms
						 
		       
				 ,case when not_fin_clm=1 and
                            (b.code_cat ='Lab' or 
		                     c.code_cat ='Lab' )
		               then 1 else 0 end as Lab_lne_clms

				 ,case when not_fin_clm=1 and
                            (b.code_cat ='Rad' or 
		                     c.code_cat ='Rad' )
		               then 1 else 0 end as Rad_lne_clms

			    
				 ,case when not_fin_clm=1 and
                           (b.code_cat ='DME' or 
		                    c.code_cat ='DME' )
		               then 1 else 0 end as DME_lne_clms		 
		 
		       %end;
		       
		       %if "&fl."="ip" or "&fl."="lt" %then %do;

		         ,case when not_fin_clm=1 and
                           ( (substring(bill_type_cd_upd,2,1) in ('1', '4') and
				              substring(bill_type_cd_upd,3,1) in ('1','2') 
		                      ) or b.prvdr_txnmy_ip=1  
							 
							)
				        then 1 else 0 end as inp_clms  

 				 ,case when not_fin_clm=1 and
                            (b.prvdr_txnmy_icf=1 or 
							 (substring(bill_type_cd_upd,2,1) in ('6') and
				              substring(bill_type_cd_upd,3,1) in ('5','6') )
                             )
					   then 1 else 0 end as ic_clms					

			    ,case when  not_fin_clm=1 and
	                     ( b.prvdr_txnmy_nf=1 or 
						  
		                  (substring(bill_type_cd_upd,2,1) in ('2')) or
						  (substring(bill_type_cd_upd,2,2) in ('18') )
						 )
					 then 1 else 0 end as nf_clms

			    ,case when  not_fin_clm=1 and
			             ( b.prvdr_txnmy_othr_res=1 or 
						  
	                      (substring(bill_type_cd_upd,2,2) in ('86') )
	                     )
					then 1 else 0 end as othr_res_clms

               %end;

               %if "&fl."="ip" or "&fl."="lt" or "&fl."="ot" %then %do;	

				 ,case when not_fin_clm=1 and
                            ((substring(bill_type_cd_upd,2,1) in ('1') and
				             substring(bill_type_cd_upd,3,1) in ('3','4')) or

							 (substring(bill_type_cd_upd,2,1) in ('8') and
				              substring(bill_type_cd_upd,3,1) in ('3','4','5','9'))
							 )
						  then 1 else 0 end as op_hosp_clms

				 ,case when not_fin_clm=1 and
                            (substring(bill_type_cd_upd,2,1) in ('7') or
				             ever_clinic_rev =1)
					  then 1 else 0 end as clinic_clms	

	             ,case when not_fin_clm=1 and
                           (substring(bill_type_cd_upd,2,2) in ('81','82') or
				                ever_hospice_rev=1) 
					       then 1 else 0 end as hospice_clms                 
				
                  ,case when not_fin_clm=1 and
                            (substring(bill_type_cd_upd,1,3) in ('032','033','034') or	 
		                     only_hh_rev =1 
                             %if "&fl."="ot" %then %do;
                               or  only_hh_procs=1 
							 %end;
							   )
						then 1 else 0 end as HH_clms
			   %end;
			   
			   %if "&fl."="rx" %then %do;

		         ,case when not_fin_clm=1 and
                           (ever_valid_ndc =1 or ever_valid_ndc is null or
			               (ever_valid_ndc =0 and 
		                    ever_dme_hhs_tos =0) )
					   then 1 else 0 end as rx_clms


				 ,case when not_fin_clm=1 and
                            (ever_valid_ndc =0 and
				             ever_dme_hhs_tos =1) 
		               then 1 else 0 end as DME_lne_clms
			   %end;
			   
			   
		   from (select *,
                         case when cmc_php=0 and other_pmpm=0 and  
                    			   dsh_flag=0 and srvc_trkg=0 and
		                           supp_clms=0 and cap_tos_null_toc=0
							  then 1 else 0 end as not_fin_clm
                  from &fl._combined
                  )a 

		/* ??? Why not use the billing taxonomy code directly from the TAF file instead? 
	      Is it because the taxonomy code on TAF may be incomplete/incorrect?*/

		   %if "&fl."="ip" or "&fl."="lt" %then %do;		   
		   	left join nppes_npi b
			on a.blg_prvdr_npi_num=b.prvdr_npi
		   %end;

		   %if "&fl."="ot" %then %do;
		    left join ccs_proc b
			on a.prcdr_cd=b.cd_rng

			left join ccs_proc c
			on a.hcpcs_rate=c.cd_rng
		   %end;

		   ) s1

   ) by tmsis_passthrough;

**************************************************************************;
* Roll up to header  *;
**************************************************************************;
  
**************************************************************************;
* Select the first claim line from each set of claims. That is hdr level file *;
* Not rolling up because some line vars needed for QA tab*;
**************************************************************************;
execute(
   create temp table &fl._hdr_rolled_0 as
   select b.*
         ,inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
		  dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
		  clinic_clms + hospice_clms + all_othr_inst_clms +

		  lab_clms + rad_clms + hh_clms + dme_clms +
		  all_othr_prof_clms as tot_num_srvc_flag
		 ,case when (inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
		  			  dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
		  			  clinic_clms + hospice_clms + all_othr_inst_clms +
		  			  lab_clms + rad_clms + hh_clms + dme_clms +
		  			  all_othr_prof_clms ) =0 

                then 0 
                when (inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
		  			  dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
		  			  clinic_clms + hospice_clms + all_othr_inst_clms +
		  			  lab_clms + rad_clms + hh_clms + dme_clms +
		  			  all_othr_prof_clms) = 1
                then 1 
                when (inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
		  			  dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
		  			  clinic_clms + hospice_clms + all_othr_inst_clms +
		  			  lab_clms + rad_clms + hh_clms + dme_clms +
		  			  all_othr_prof_clms ) > 1
                then 2 else null end as num_srvc_flag_grp

		,case when inst_clms=0 and 
				   prof_clms=0 
              then 1 else 0 end as not_inst_prof

   from ( select
          a.*
          ,cmc_php + other_pmpm + dsh_flag + other_fin 
		   as tot_num_fin_flag

		
		 %if "&fl." ne "rx" %then %do;
         ,0 as rx_clms  
		 %end;
		 %if "&fl." ne "ot" %then %do;
         
         ,0 as inst_clms
         ,0 as prof_clms

		 ,0 as dental_clms
		 ,0 dental_lne_cnts

		 ,0 as trnsprt_clms
		 ,0 as trnsprt_lne_cnts

 		 ,0 as othr_hcbs_clms
		 ,0 as othr_hcbs_lne_cnts
 
       	 ,0 as Lab_clms
		 ,0 as Lab_lne_cnts

     	 ,0 as Rad_clms
		 ,0 as Rad_lne_cnts

		 ,0 as all_othr_inst_clms
		 ,0 as all_othr_prof_clms
 
        %end;
		%if "&fl." = "ot" or "&fl." = "rx" %then %do;
        ,0 as nf_clms /**Only in IP/LT**/
		,0 as inp_clms
        ,0 as ic_clms 
		,0 as othr_res_clms
		%end;
		%if "&fl." = "rx" %then %do;
         /**Only in OT/IP/LT**/
		 ,0 as op_hosp_clms
		 ,0 as clinic_clms
		 ,0 as hospice_clms
		 ,0 as hh_clms
		%end;
	    %if "&fl." = "ip" or "&fl." = "lt" %then %do;
         /**Only in OT/RX**/
		 ,0 as dme_clms
		 ,0 as DME_lne_cnts
		%end;
	

   from (select 
         *
		  ,case when (srvc_trkg=1 or supp_clms=1 or cap_tos_null_toc=1) 
                then 1 else 0 end as other_fin
		  
		 %if "&fl."="ot" %then %do;
		  ,case when  not_fin_clm=1 and
                      inst_clms=1 and 
		              dental_clms=0 and
					  trnsprt_clms=0 and
					  othr_hcbs_clms=0 and
					  op_hosp_clms=0 and
					  clinic_clms=0 and 
					  hospice_clms=0
                  then 1 else 0 end as all_othr_inst_clms

 		  ,case when  not_fin_clm=1 and
                      prof_clms=1 and 
		              dental_clms=0 and
					  trnsprt_clms=0 and
					  othr_hcbs_clms=0 and
					  Lab_clms=0 and
					  Rad_clms=0 and 
					  hh_clms=0 and
					  DME_clms=0
                 then 1 else 0 end as all_othr_prof_clms

		  %end;
		
   from &fl._lne_flag_tos_cat
   where rec_cnt=1 ) a ) b
   ) by tmsis_passthrough;

title ;

execute(
   create temp table &fl._hdr_rolled as
   select *
         ,case when cmc_php =1 				then '11'
               when other_pmpm =1 			then '12'
               when dsh_flag =1 			then '13'
               when other_fin =1 			then '14'
			   when inp_clms=1 				then '21'
			   when rx_clms=1 				then '41'
			   when nf_clms=1 				then '22'
			   when ic_clms=1 				then '23'
			   when othr_res_clms=1 		then '24'
			   when hospice_clms=1 			then '25'
			   when rad_clms=1 				then '31'
			   when lab_clms=1 				then '32'
			   when hh_clms=1 				then '33'
               when trnsprt_clms=1 			then '34'
			   when dental_clms=1 			then '35'
			   when op_hosp_clms=1 			then '26'
			   when clinic_clms=1 			then '27'
			   when othr_hcbs_clms=1 		then '36'
			   when dme_clms=1 				then '37'	  			  			  
			   when all_othr_inst_clms =1 	then '28'
			   when all_othr_prof_clms =1 	then '38'
			  end as fed_srvc_ctgry_cd       
   from &fl._hdr_rolled_0

) by tmsis_passthrough;

%DROP_temp_tables(&fl._header_0);
%DROP_temp_tables(&fl._lne);
%DROP_temp_tables(&fl._combined);
%DROP_temp_tables(&fl._lne_flag_tos_cat);
%DROP_temp_tables(&fl._hdr_rolled_0);

%mend fasc_code;

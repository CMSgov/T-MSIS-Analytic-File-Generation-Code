/**********************************************************************************************/
/*Program: 003_bsf_ELG00003.sas
/*Author: Gerry Skurski, Mathematica Policy Research
/*Date: 3/2/2017
/*Purpose: Process TMSIS_VAR_DMGRPHC_ELGBLTY and create unique output for BSF.
/*Mod:  
/*Notes: This program is included by 001_batch_bsf.sas
/**********************************************************************************************/
/* © 2020 Mathematica Inc. 																	  */
/* The TMSIS Analytic File (TAF) code was developed by Mathematica Inc. as part of the 	      */
/* MACBIS Business Analytics and Data Quality Development project funded by the U.S. 	      */
/* Department of Health and Human Services – Centers for Medicare and Medicaid Services (CMS) */
/* through Contract No. HHSM-500-2014-00034I/HHSM-500-T0005  							  	  */
/**********************************************************************************************/

%macro create_prmry_lang_cd_table;

execute(
create temp table prmry_lang_cd( 
LANG_CD varchar(3)) diststyle all;
insert into prmry_lang_cd values 
('ABK'),
('ACE'),
('ACH'),
('ADA'),
('ADY'),
('AAR'),
('AFH'),
('AFR'),
('AFA'),
('AIN'),
('AKA'),
('AKK'),
('ALB'),
('ALB'),
('ALE'),
('ALG'),
('TUT'),
('AMH'),
('ANP'),
('APA'),
('ARA'),
('ARG'),
('ARP'),
('ARW'),
('ARM'),
('RUP'),
('ART'),
('ASM'),
('AST'),
('ATH'),
('AUS'),
('MAP'),
('AVA'),
('AVE'),
('AWA'),
('AYM'),
('AZE'),
('BAN'),
('BAT'),
('BAL'),
('BAM'),
('BAI'),
('BAD'),
('BNT'),
('BAS'),
('BAK'),
('BAQ'),
('BTK'),
('BEJ'),
('BEL'),
('BEM'),
('BEN'),
('BER'),
('BHO'),
('BIH'),
('BIK'),
('BIN'),
('BIS'),
('BYN'),
('ZBL'),
('NOB'),
('BOS'),
('BRA'),
('BRE'),
('BUG'),
('BUL'),
('BUA'),
('BUR'),
('CAD'),
('CAT'),
('CAU'),
('CEB'),
('CEL'),
('CAI'),
('KHM'),
('CHG'),
('CMC'),
('CHA'),
('CHE'),
('CHR'),
('CHY'),
('CHB'),
('NYA'),
('CHI'),
('CHN'),
('CHP'),
('CHO'),
('CHU'),
('CHK'),
('CHV'),
('NWC'),
('SYC'),
('COP'),
('COR'),
('COS'),
('CRE'),
('MUS'),
('CRP'),
('CPE'),
('CPF'),
('CPP'),
('CRH'),
('HRV'),
('CUS'),
('CZE'),
('DAK'),
('DAN'),
('DAR'),
('DEL'),
('DIN'),
('DIV'),
('DOI'),
('DGR'),
('DRA'),
('DUA'),
('DUM'),
('DUT'),
('DYU'),
('DZO'),
('FRS'),
('EFI'),
('EGY'),
('EKA'),
('ELX'),
('ENG'),
('ENM'),
('ANG'),
('MYV'),
('EPO'),
('EST'),
('EWE'),
('EWO'),
('FAN'),
('FAT'),
('FAO'),
('FIJ'),
('FIL'),
('FIN'),
('FIU'),
('FON'),
('FRE'),
('FRM'),
('FRO'),
('FUR'),
('FUL'),
('GAA'),
('GLA'),
('CAR'),
('GLG'),
('LUG'),
('GAY'),
('GBA'),
('GEZ'),
('GEO'),
('GER'),
('GMH'),
('GOH'),
('GEM'),
('GIL'),
('GON'),
('GOR'),
('GOT'),
('GRB'),
('GRC'),
('GRE'),
('GRN'),
('GUJ'),
('GWI'),
('HAI'),
('HAT'),
('HAU'),
('HAW'),
('HEB'),
('HER'),
('HIL'),
('HIM'),
('HIN'),
('HMO'),
('HIT'),
('HMN'),
('HUN'),
('HUP'),
('IBA'),
('ICE'),
('IDO'),
('IBO'),
('IJO'),
('ILO'),
('SMN'),
('INC'),
('INE'),
('IND'),
('INH'),
('INA'),
('ILE'),
('IKU'),
('IPK'),
('IRA'),
('GLE'),
('MGA'),
('SGA'),
('IRO'),
('ITA'),
('JPN'),
('JAV'),
('JRB'),
('JPR'),
('KBD'),
('KAB'),
('KAC'),
('KAL'),
('XAL'),
('KAM'),
('KAN'),
('KAU'),
('KRC'),
('KAA'),
('KRL'),
('KAR'),
('KAS'),
('CSB'),
('KAW'),
('KAZ'),
('KHA'),
('KHI'),
('KHO'),
('KIK'),
('KMB'),
('KIN'),
('KIR'),
('TLH'),
('KOM'),
('KON'),
('KOK'),
('KOR'),
('KOS'),
('KPE'),
('KRO'),
('KUA'),
('KUM'),
('KUR'),
('HSB'),
('KRU'),
('KUT'),
('LAD'),
('LAH'),
('LAM'),
('DAY'),
('LAO'),
('LAT'),
('LAV'),
('LEZ'),
('LIM'),
('LIN'),
('LIT'),
('JBO'),
('NDS'),
('DSB'),
('LOZ'),
('LUB'),
('LUA'),
('LUI'),
('SMJ'),
('LUN'),
('LUO'),
('LUS'),
('LTZ'),
('MAC'),
('MAD'),
('MAG'),
('MAI'),
('MAK'),
('MLG'),
('MAY'),
('MAL'),
('MLT'),
('MNC'),
('MDR'),
('MAN'),
('MNI'),
('MNO'),
('GLV'),
('MAO'),
('ARN'),
('MAR'),
('CHM'),
('MAH'),
('MWR'),
('MAS'),
('MYN'),
('MEN'),
('MIC'),
('MIN'),
('MWL'),
('MOH'),
('MDF'),
('LOL'),
('MON'),
('MKH'),
('MOS'),
('MUL'),
('MUN'),
('NAH'),
('NAU'),
('NAV'),
('NDE'),
('NBL'),
('NDO'),
('NAP'),
('NEW'),
('NEP'),
('NIA'),
('NIC'),
('SSA'),
('NIU'),
('NQO'),
('NOG'),
('NON'),
('NAI'),
('FRR'),
('SME'),
('NOR'),
('NNO'),
('NUB'),
('NYM'),
('NYN'),
('NYO'),
('NZI'),
('OCI'),
('ARC'),
('OJI'),
('ORI'),
('ORM'),
('OSA'),
('OSS'),
('OTO'),
('PAL'),
('PAU'),
('PLI'),
('PAM'),
('PAG'),
('PAN'),
('PAP'),
('PAA'),
('NSO'),
('PER'),
('PEO'),
('PHI'),
('PHN'),
('PON'),
('POL'),
('POR'),
('PRA'),
('PRO'),
('PUS'),
('QUE'),
('RAJ'),
('RAP'),
('RAR'),
('ROA'),
('RUM'),
('ROH'),
('ROM'),
('RUN'),
('RUS'),
('SAL'),
('SAM'),
('SMI'),
('SMO'),
('SAD'),
('SAG'),
('SAN'),
('SAT'),
('SRD'),
('SAS'),
('SCO'),
('SEL'),
('SEM'),
('SRP'),
('SRR'),
('SHN'),
('SNA'),
('III'),
('SCN'),
('SID'),
('SGN'),
('BLA'),
('SND'),
('SIN'),
('SIT'),
('SIO'),
('SMS'),
('DEN'),
('SLA'),
('SLO'),
('SLV'),
('SOG'),
('SOM'),
('SON'),
('SNK'),
('WEN'),
('SOT'),
('SAI'),
('ALT'),
('SMA'),
('SPA'),
('SRN'),
('SUK'),
('SUX'),
('SUN'),
('SUS'),
('SWA'),
('SSW'),
('SWE'),
('GSW'),
('SYR'),
('TGL'),
('TAH'),
('TAI'),
('TGK'),
('TMH'),
('TAM'),
('TAT'),
('TEL'),
('TER'),
('TET'),
('THA'),
('TIB'),
('TIG'),
('TIR'),
('TEM'),
('TIV'),
('TLI'),
('TPI'),
('TKL'),
('TOG'),
('TON'),
('TSI'),
('TSO'),
('TSN'),
('TUM'),
('TUP'),
('TUR'),
('OTA'),
('TUK'),
('TVL'),
('TYV'),
('TWI'),
('UDM'),
('UGA'),
('UIG'),
('UKR'),
('UMB'),
('MIS'),
('UND'),
('UZB'),
('VAI'),
('VEN'),
('VIE'),
('VOL'),
('VOT'),
('WAK'),
('WLN'),
('WAR'),
('WAS'),
('WEL'),
('FRY'),
('WAL'),
('WOL'),
('XHO'),
('SAH'),
('YAO'),
('YAP'),
('YID'),
('YOR'),
('YPK'),
('ZND'),
('ZAP'),
('ZZA'),
('ZEN'),
('ZHA'),
('ZUL'),
('ZUN')) by tmsis_passthrough;
%mend create_prmry_lang_cd_table;

%macro create_ELG00003(tab_no, _2x_segment, eff_date, end_date);
%let created_vars = 
        case when trim(PRMRY_LANG_CODE) in('CHI')                         then  'C' /*Chinese*/
             when trim(PRMRY_LANG_CODE) in('GER','GMH','GOH','GSW','NDS') then  'D' /*German*/
             when trim(PRMRY_LANG_CODE) in('ENG','ENM','ANG')             then  'E' /*English*/
             when trim(PRMRY_LANG_CODE) in('FRE','FRM','FRO')             then  'F' /*French*/
             when trim(PRMRY_LANG_CODE) in('GRC','GRE')                   then  'G' /*Greek*/
             when trim(PRMRY_LANG_CODE) in('ITA','SCN')                   then  'I' /*Italian*/
             when trim(PRMRY_LANG_CODE) in('JPN')                         then  'J' /*Japanese*/
             when trim(PRMRY_LANG_CODE) in('NOB','NNO','NOR')             then  'N' /*Norwegian*/
             when trim(PRMRY_LANG_CODE) in('POL')                         then  'P' /*Polish*/
             when trim(PRMRY_LANG_CODE) in('RUS')                         then  'R' /*Russian*/                 
             when trim(PRMRY_LANG_CODE) in('SPA')                         then  'S' /*Spanish*/
             when trim(PRMRY_LANG_CODE) in('SWE')                         then  'V' /*Swedish*/
             when trim(PRMRY_LANG_CODE) in('SRP','HRV')                   then  'W' /*Serbo-Croatian*/ 
             when trim(PRMRY_LANG_CODE) in('UND','','.') 
                  or PRMRY_LANG_CODE is null                              then  null /*Missing*/
			 else 'O' end as PRMRY_LANG_FLG;

%create_prmry_lang_cd_table;

execute (
     /* Validate primary language code */
     create temp table &tab_no._v     
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num) as 
     select t.*
		   ,case when v.LANG_CD is not null then v.LANG_CD else null end as PRMRY_LANG_CODE
		from &tab_no t
		left join prmry_lang_cd v
		on v.LANG_CD = t.PRMRY_LANG_CD
		) by tmsis_passthrough;


execute (
     /* Create temp table to determine which beneficiaries have multiple records */
     create temp table &tab_no._recCt      
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num,recCt) as 
     select 
        submtg_state_cd, 
        msis_ident_num,
		count(TMSIS_RUN_ID) as recCt
		from &tab_no._v
        group by submtg_state_cd, msis_ident_num
		) by tmsis_passthrough;

title "Number of records per beneficiary in &tab_no";
select * from connection to tmsis_passthrough
 ( select recCt,count(msis_ident_num) as beneficiaries from &tab_no._recCt group by recCt ) order by recCt;

 execute(

     /* Set aside table data for benes with only one record */
     create temp table &tab_no._uniq
     distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num) as  
	 select t1.*,

        &created_vars,

		 1 as KEEP_FLAG

		from &tab_no._v t1
        inner join &tab_no._recCt  t2
		 on t1.submtg_state_cd = t2.submtg_state_cd
		and t1.msis_ident_num  = t2.msis_ident_num
		and t2.recCt=1
		) by tmsis_passthrough;

title "Number of beneficiary with unique records in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._uniq );

%MultiIds(sort_key=%str(coalesce(mrtl_stus_cd,'xx') || coalesce(cast(ssn_num as char(10)),'xx') || coalesce(incm_cd,'xx') ||
                        coalesce(vet_ind,'xx') ||coalesce(ctznshp_ind,'xx') || coalesce(imgrtn_stus_cd,'xx') || 
                        coalesce(upper(prmry_lang_cd),'xx') || coalesce(hsehld_size_cd,'xx') || coalesce(mdcr_hicn_num,'xx') ||
                        coalesce(chip_cd,'xx') || coalesce(prmry_lang_englsh_prfcncy_cd,'xx')),suffix=,val=_v)

title "Number of beneficiares who were processed for duplicates in &tab_no";
select * from connection to tmsis_passthrough
 ( select count(msis_ident_num) as beneficiaries from &tab_no._multi );


 execute(

     /* Union together tables for a permanent table */
     create temp table &tab_no._&BSF_FILE_DATE._uniq
	 distkey(msis_ident_num)
	 sortkey(submtg_state_cd,msis_ident_num) as  

	 select comb.*, 
            preg.PREGNANCY_FLAG as PREGNANCY_FLG

	 from (
	 select * from &tab_no._uniq
	 union all
	 select * from &tab_no._multi) as comb

	 /* Compute pregnancy flag and join back to unique table data */
	 left join 
	  (select SUBMTG_STATE_CD,
              MSIS_IDENT_NUM,
			  max(case when trim(PRGNT_IND) in('0','1') then cast(trim(PRGNT_IND) as integer)
                       else null end) as PREGNANCY_FLAG 
	   from &tab_no.A
	   group by SUBMTG_STATE_CD,
	            MSIS_IDENT_NUM) preg
	  
	    on comb.SUBMTG_STATE_CD=preg.SUBMTG_STATE_CD
	   and comb.msis_ident_num=preg.msis_ident_num
    )  by tmsis_passthrough;

/* extract to SAS or load to permanent TMSIS table */
title "Number of records in &tab_no._&BSF_FILE_DATE._uniq";
	  select tot_ct into: tot_ct
	  from (select * from connection to tmsis_passthrough
	  (
       select count(submtg_state_cd) as tot_ct
	   from &tab_no._&BSF_FILE_DATE._uniq
	  ));
 
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_bsf_ELG00003, 0.1. create_initial_table);
%Get_Audt_counts(&DA_SCHEMA.,&DA_RUN_ID., 003_bsf_ELG00003, 0.2. MultiIds);

%drop_table_multi(&tab_no. &tab_no._recCt &tab_no._uniq &tab_no.A &tab_no._multi &tab_no._multi_all &tab_no._multi_step2);

%mend create_ELG00003;


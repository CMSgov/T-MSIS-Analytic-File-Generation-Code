/******************************************************************************************************/
/* Program:     Project_Formats.sas                                                                   */
/* Author:      Deo S. Bencio                                                                         */
/* Date:        2/11/2016                                                                             */
/* Purpose:     Program stores all formats used for project                                  	      */
/*                                                                                                    */
/* Copyright (C) Mathematica Policy Research, Inc.                                                    */
/* This code cannot be copied, distributed or used without the express written permission             */
/* of Mathematica Policy Research, Inc.                                                               */ 
/******************************************************************************************************/

/*LIBNAME library "/sasdata/users/&sysuserid/tmsisshare/Task_5/Data";*/

proc format ;
  value $stfipn
  '01'='AL'
  '02'='AK'
  '04'='AZ'
  '05'='AR'
  '06'='CA'
  '08'='CO'
  '09'='CT'
  '10'='DE'
  '11'='DC'
  '12'='FL'
  '13'='GA'
  '15'='HI'
  '16'='ID'
  '17'='IL'
  '18'='IN'
  '19'='IA'
  '20'='KS'
  '21'='KY'
  '22'='LA'
  '23'='ME'
  '24'='MD'
  '25'='MA'
  '26'='MI'
  '27'='MN'
  '28'='MS'
  '29'='MO'
  '30'='MT'
  '31'='NE'
  '32'='NV'
  '33'='NH'
  '34'='NJ'
  '35'='NM'
  '36'='NY'
  '37'='NC'
  '38'='ND'
  '39'='OH'
  '40'='OK'
  '41'='OR'
  '42'='PA'
  '44'='RI'
  '45'='SC'
  '46'='SD'
  '47'='TN'
  '48'='TX'
  '49'='UT'
  '50'='VT'
  '51'='VA'
  '53'='WA'
  '54'='WV'
  '55'='WI'
  '56'='WY'
  '96'='IA CHIP'
  '97'='PA CHIP'
  ;
  value $mcplan
 '00','99','  ' = 'None'   /* None */
 '01','04','17' = 'Any MCO'   /* MCO plans */
'05'-'06','14'-'16','18','60','70','80' = 'Other MC'   /* Other plans */
 '08'-'13' = 'Any BHO'   /* BHO */
 '07' = 'Any MLTSS'        /* MLTSS */
 '02','03' = 'Any PCCM'   /* PCCM */
 other = 'Unspec MC'
 ;
 value $mc2plan
 '00','99','  ' = 'None'   /* None */
 '01' = '01:MCO'
 '04' = '04:MCO'
 '17' = '17:MCO'
 '05' = '05:Other MC'
 '06' = '06:Other MC'
 '14' = '14:Other MC'
 '15' = '15:Other MC'
 '16' = '16:Other MC'
 '18' = '18:Other MC'
 '60' = '60:Other MC'
 '70' = '70:Other MC'
 '80' = '80:Other MC'
 '08' = '08:BHO'
 '09' = '09:BHO'
 '10' = '10:BHO'
 '11' = '11:BHO'
 '12' = '12:BHO'
 '13' = '13:BHO'
 '07' = '07:MLTSS'
 '02' = '02:PCCM'
 '03' = '03:PCCM'
 Other = 'Other-Unspec'
 ;
 value agegp
 low-20  = '1'
 21-64   = '2'
 65-high = '3'
 .       = '4'
 ;
 value $agegp
 '1' = '< 21'
 '2' = '21-64'
 '3' = '> 64'
 '4' = 'Unknown'
 ;
 value age1gp
 . = 'Missing'
 91 - high = '> 90'
 ;
 value age2gp
 . = ' '
 low -< 1 = '1'
 1 - 5    = '2'
 6 - 19   = '3'
 20 - 21  = '4'
 22 - 44  = '5'
 45 - 64  = '6'
 65 - 74  = '7'
 75 - high = '8'
 ;
 value $age2gp
 '1' = "<1"
 '2' = "1--5"
 '3' = "6--19"
 '4' = "20--21"
 '5' = "22--44"
 '6' = "45--64"
 '7' = "65--74"
 '8' = "75+"
 ' ' = "Missing"
 ;
 value age3gp
 -1 -< 1 = '01'
 1 - 5   = '02'
 6 - 14  = '03'
 15 - 18 = '04'
 19 - 20 = '05'
 21 - 44 = '06'
 45 - 64 = '07'
 65 - 74 = '08'
 75 - 84 = '09'
 85 - 120= '10'
 other   = '11'
 ;
 value $age3gp
 '01' = 'Age <1'
 '02' = 'Age 1-5'
 '03' = 'Age 6-14'
 '04' = 'Age 15-18'
 '05' = 'Age 19-20'
 '06' = 'Age 21-44'
 '07' = 'Age 45-64'
 '08' = 'Age 65-74'
 '09' = 'Age 75-84'
 '10' = 'Age 85+'
 '11' = 'Age Unknown'
 ;
 value $dual
 '00','07','  ' = 'Non-Dual'
 '01'-'06','08'-'10','99' = 'Dual'
 ;
 value $dual1f
 '00','07','  ','Non-Dual' = 'Non-Dual'
 '01'-'06','08'-'10','99' = 'Total Dual'
 ;
 value $dual2f
 '02','04','08' = 'Full Dual'
 '01','03','05','06' = 'Partial Dual'
 '09','10','99' = 'Other Dual'
 '00','07','  ','Non-Dual' = 'Non-Dual'
 ; 
 value $dual3f
 '02','04','08','2','4','8'      		= '1' /* Full Dual */
 '01','03','05','06','1','3','5','6' 	= '2' /* Partial Dual */
 '09','10','9'						 	= '3' /* Other Dual */
 '00','0'					            = '4' /* Non-Dual */
 '99'									= '5' /* Unknown */
 ; 
 value $waiver
 '00','88','99','',' ' = 'Non Waiver'
 '01'-'24' = 'Waiver'
 other     = 'Other Waiver'
 ;
 value $waiverf
 '1' = 'None'
 '2' = 'Multiple'
 '3' = 'Any'
 ;
 value $plantyp
 '99','',' ' = 'None'
 ;
 value $mboe
 'X' = 'NULL'
 ;
 value $sex
 ' ' = 'Missing'
 'M' = 'Male'
 'F' = 'Female'
 'U' = 'Unknown'
 ;
value $spec
'01',	/*'General practice'*/
'02',	/*'General surgery'*/
'03',	/*'Allergy/immunology'*/
'04',	/*'Otolaryngology'*/
'05',	/*'Anesthesiology'*/
'06',	/*'Cardiology'*/
'07',	/*'Dermatology'*/
'08',	/*'Family practice'*/
'09',	/*'Interventional pain management (IPM) (eff. 4/*1/2003)'*/
'10',	/*'Gastroenterology'*/
'11',	/*'Internal medicine'*/
'12',	/*'Osteopathic manipulative therapy'*/
'13',	/*'Neurology'*/
'14',	/*'Neurosurgery'*/
'16',	/*'Obstetrics/gynecology'*/
'17',	/*'Hospice and palliative care'*/
'18',	/*'Ophthalmology'*/
'20',	/*'Orthopedic surgery'*/
'21',	/*'Cardiac electrophysiology'*/
'22',	/*'Pathology'*/
'23',	/*'Sports medicine'*/
'24',	/*'Plastic and reconstructive surgery'*/
'25',	/*'Physical medicine and rehabilitation'*/
'26',	/*'Psychiatry'*/
'27',	/*'Geriatric psychiatry'*/
'28',	/*'Colorectal surgery (formerly proctology)'*/
'29',	/*'Pulmonary disease'*/
'30',	/*'Diagnostic radiology'*/
'33',	/*'Thoracic surgery'*/
'34',	/*'Urology'*/
'36',	/*'Nuclear medicine'*/
'37',	/*'Pediatric medicine'*/
'38',	/*'Geriatric medicine'*/
'39',	/*'Nephrology'*/
'40',	/*'Hand surgery'*/
'44',	/*'Infectious disease'*/
'46',	/*'Endocrinology (eff. 5/1992)'*/
'66',	/*'Rheumatology (eff. 5/1992'*/
'70',	/*'Multispecialty clinic or group practice'*/
'72',	/*'Pain management (eff. 1/1/2002)'*/
'76',	/*'Peripheral vascular disease (eff. 5/1992)'*/
'77',	/*'Vascular surgery (eff. 5/1992)'*/
'78',	/*'Cardiac surgery (eff. 5/1992)'*/
'79',	/*'Addiction medicine (eff. 5/1992)'*/
'81',	/*'Critical care (intensivists) (eff. 5/1992)'*/
'82',	/*'Hematology (eff. 5/1992)'*/
'83',	/*'Hematology/oncology (eff. 5/1992)'*/
'84',	/*'Preventive medicine (eff. 5/1992)'*/
'85',	/*'Maxillofacial surgery'*/
'86',	/*'Neuropsychiatry (eff. 5/1992)'*/
'90',	/*'Medical oncology (eff. 5/1992)'*/
'91',	/*'Surgical oncology (eff. 5/1992)'*/
'92',	/*'Radiation oncology (eff. 5/1992)'*/
'93',	/*'Emergency medicine (eff. 5/1992)'*/
'94',	/*'Interventional radiology (eff. 5/1992)'*/
'98',	/*'Gynecologist/oncologist (eff. 10/1994)'*/
'99',	/*'Unknown physician specialty'*/
'C0' =  'Physician    '	/*'Sleep medicine'*/
'  ' =  'NotApplicable'
other = 'NonPhysician '
;
value $pcp
'01','08','11','37','38','84' = 'PCP   '      /* removed '50' and '97' from the list 3/15/16 */
other = 'NonPCP'
;
value $tocfm
'1','4','5','A','D','E','U','W','X','Y' = '1'
'3','C' = '2'
'2','B','V' = '3'
'Z'     = '4'
;
value $toclabl
'1' = 'FFS   '
'2' = 'Cap MC'
'3' = 'CapPmt'
'4' = 'Denied'
;
value $BED_CDF
'1' = "Inpatient      "
'2' = "Skilled Nursing"
'3' = "ICF-IID        "
;
value cntfm
. = 'Not Reported'
;
value $eligcat
'1' = 'Mcaid Adults'
'2' = 'Mcaid Child'
'3' = 'Disabled'
'4' = 'Mcaid Aged'
'5' = 'Unk'
'6' = 'CHIP'
;
value ffs_mc
0 = 'FFS    '
1 = 'Comp MC'
;
value clmtyp
1 = 'Inpatient'
2 = 'Long Term Care'
3 = 'Other'
4 = 'RX'
;
value $mc3plan
'01'= 'MCO'
'02'= 'Traditional PCCM'
'03'= 'Enhanced PCCM'
'04'= 'HIO'
'05'= 'Medical-only PIHP'  
'06'= 'Medical-only PAHP' 
'07'= 'LTC PIHP'
'08'= 'MH PIHP'
'09'= 'MH PAHP'
'10'= 'SUD PIHP'
'11'= 'SUD PAHP'
'12'= 'MH and SUD PIHP'
'13'= 'MH and SUD PAHP'
'14'= 'Dental PAHP'
'15'= 'Transportation PAHP'
'16'= 'Disease Management PAHP'
'17'= 'PACE'
'18'= 'Pharmacy PAHP'
'99'= 'Unknown'
'60'= 'ACO'
'70'= 'Health/Medical Home'
'80'= 'Integrated Care For Dual Eligibles'
other = 'Unknown'
;
value $addrank
'01' = '1'
'02' = '4'
'03' = '2'
'04' = '5'
'05' = '6'
'06' = '3'
other = '7'
;
value $wavrank
'17' = '1'
'18' = '2'
'19' = '3'
'06','07','08','09','10','11','12','13','14','6','7','8','9' = '4'
'15' = '5'
'02','03','04','05','2','3','4','5' = '6'
'01','1' = '7'
'16' = '8'
other = '9'
;
value $mgprank
'01','1' = '1'
'04','4' = '2'
'05','5' = '3'
'06','6' = '4'
'15' = '5'
'07','7' = '6'
'14' = '7'
'17' = '8'
'08','8' = '09'
'09','9' = '10'
'10' = '11'
'11' = '12'
'12' = '13'
'13' = '14'
'18' = '15'
'16' = '16'
'02','2' = '17'
'03','3' = '18'
'60' = '19'
'70' = '20'
'80' = '21'
'99' = '22'
other = '23'
;
value $prvrank
'4' = '1'
'3' = '2'
'2' = '3'
'1' = '4'
other = '5'
;
value $region
'CT','MA','ME','NH','RI','VT' 			= '01'
'NJ','NY' 								= '02'
'DE','DC','MD','PA','VA','WV' 			= '03'
'AL','FL','GA','KY','MS','NC','SC','TN' = '04'
'IL','IN','MI','MN','OH','WI'           = '05'
'AR','LA','NM','OK','TX'				= '06'
'IA','KS','MO','NE'						= '07'
'CO','MT','ND','SD','UT','WY'			= '08'
'AZ','CA','HI','NV'						= '09'
'AK','ID','OR','WA'						= '10'
other									= '11'
;
value $chipfm
'4','9' = '9'
;
value $missin
'9',' ','.' = '9'
;
value $pregfm
'9',' ','.' = '.'
;
value $blanks
' ' = 'Missing'
;
value blanks
. = 'Missing'
;
value $eligwfm
'01'-'09','1','2','3','4','5','6','7','8','9','72'-'75' 		 = 'Medicaid Mandatory Coverage - Family/Adult'
'11'-'19','20'-'26'			 = 'Medicaid Mandatory Coverage - ABD'
'27'-'29','30'-'36'			 = 'Medicaid Options for Coverage - Family/Adult'
'37'-'39','40'-'49','50'-'52'			 = 'Medicaid Options for Coverage - ABD'
'53'-'56'			 = 'Medicaid Medically Needy - Family/Adult'
'59','60'			 = 'Medicaid Medically Needy - ABD'
'61'-'63'			 = 'CHIP Coverage - Children'
'64'-'66'			 = 'CHIP Additional Options for Coverage - Children'
'67'-'68'			 = 'CHIP Additional Options for Coverage - Pregnant Women'
'69','70'-'71'		 = '1115 Expansion'
'  ','.'			 = 'Unknown'
;
value $elig2fm
'01'-'09', '72'-'75','1','2','3','4','5','6','7','8','9' = '01'
'11'-'19','20'-'26'			 = '02'
'27'-'29','30'-'36'			 = '03'
'37'-'39','40'-'49','50'-'52'= '04'
'53'-'56'			 = '05'
'59','60'			 = '06'
'61'-'63'			 = '07'
'64'-'66'			 = '08'
'67'-'68'			 = '09'
'69','70'-'71'		 = '10'
other				 = '11'
;

value $hhfm
'88888888888888888888','     ' = '0'
'99999999999999999999'         = '9'
other	 	    = '1'
;
value $langfm
'CHI'                         = 'C' /*Chinese*/
'GER','GMH','GOH','GSW','NDS' = 'D' /*German*/
'ENG','ENM','ANG'             = 'E' /*English*/
'FRE','FRM','FRO'             = 'F' /*French*/
'GRC','GRE'                   = 'G' /*Greek*/
'ITA','SCN'                   = 'I' /*Italian*/
'JPN'                         = 'J' /*Japanese*/
'NOB','NNO','NOR'             = 'N' /*Norwegian*/
'POL'                         = 'P' /*Polish*/
'RUS'                         = 'R' /*Russian*/                 
'SPA'                         = 'S' /*Spanish*/
'SWE'                         = 'V' /*Swedish*/
'SRP','HRV'                   = 'W' /*Serbo-Croatian*/ 
'UND'                         = 'U' /*Unknown*/
'   ',''					  = ' ' /*Missing*/
other                         = 'O' /*Other*/
;

value $twofm /*added to fix 2-digit characters reported as 1*/
'1' = '01'
'2' = '02'
'3' = '03'
'4' = '04'
'5' = '05'
'6' = '06'
'7' = '07'
'8' = '08'
'9' = '09'
;

value $threefm /*added to fix 3-digit characters reported as 1*/
'1' = '001'
'2' = '002'
'3' = '003'
'4' = '004'
'5' = '005'
;
value $otmh9f
'290'-'302','306'-'319' = '1'
other = '0'
;
value $otmh0f
'F01'-'F09','F20'-'F99' = '1'
other = '0'
;
value $otsu9f
'303'-'305' = '1'
other = '0'
;
value $otsu0f
'F10'-'F19' = '1'
other = '0'
;
value $otmtax
'2084B0040X','2084P0804X','2084F0202X','2084P0805X','2084P0800X','2084P0015X','106E00000X','106S00000X','103K00000X','103G00000X',
'103GC0700X','101Y00000X','101YM0800X','101YP1600X','101YP2500X','101YS0200X','106H00000X','102X00000X','102L00000X','103T00000X',
'103TA0700X','103TC0700X','103TC2200X','103TB0200X','103TC1900X','103TE1000X','103TE1100X','103TF0000X','103TF0200X','103TP2701X',
'103TH0004X','103TH0100X','103TM1700X','103TM1800X','103TP0016X','103TP0814X','103TP2700X','103TR0400X','103TS0200X','103TW0100X',
'104100000X','1041C0700X','1041S0200X','167G00000X','163WP0808X','163WP0809X','163WP0807X','1835P1300X','364SP0808X','364SP0809X',
'364SP0807X','364SP0810X','364SP0811X','364SP0812X','364SP0813X','363LP0808X','251S00000X','261QM0850X','261QM0801X','273R00000X',
'311500000X','3104A0630X','3104A0625X','310500000X','320800000X','323P00000X','322D00000X','385HR2055X','2084P0005X','2080P0006X',
'2080P0008X','225XM0800X','252Y00000X','261QM0855X','283Q00000X','320900000X','320600000X','385HR2060X' = '1'
' ' = '9'
other = '0'
;
value $otstax
'207LA0401X','207QA0401X','207RA0401X','2084A0401X','2084P0802X','101YA0400X','103TA0400X','163WA0400X','261QM2800X','261QR0405X',
'276400000X','324500000X','3245S0500X' = '1'
' ' = '9'
other = '0'
;
value posneg
. = 'Missing'
low-<0 = 'Negative'
0 = 'Zero'
0<-high = 'Positive'
other = 'Invalid'
;
run;

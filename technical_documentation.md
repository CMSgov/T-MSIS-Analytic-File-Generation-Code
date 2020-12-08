# CMS T-MSIS Analytic File (TAF) Generation Code – Technical Documentation

The programs in this repository can be organized according to the file type being used to generate TAF. Most of the programs support the creation of only one type of TAF data set. The exceptions are the following programs, which contain macro functions that are shared across more than one TAF file type.

* AWS_Shared_Macros.sas
* AWS_Grouper_Macro.sas

All other programs are specific to only one TAF file type.

## Beneficiary Summary File (BSF)

The program 001_batch_bsf.sas is a driver program that calls each of the other programs in BSF creation in sequence. This is the only program that is run to create the BSF; all other programs listed below are called by this program and do not need to be run separately.

The program 000_bsf_macros.sas contains the SAS macro functions that are used throughout the rest of the BSF creation. 

The next group of programs are organized according to the T-MSIS table segments that provide the source data. Each program in the following list reads in source data from a single table segment and prepares an extract of that segment. The program names reflect the table segment as defined in the T-MSIS schema. 

* 002_bsf_ELG00002.sas
* 003_bsf_ELG00003.sas
* 004_bsf_ELG00004.sas
* 005_bsf_ELG00005.sas
* 006_bsf_ELG00006.sas
* 007_bsf_ELG00007.sas
* 008_bsf_ELG00008.sas
* 009_bsf_ELG00009.sas
* 010_bsf_ELG00010.sas
* 011_bsf_ELG00011.sas
* 012_bsf_ELG00012.sas
* 013_bsf_ELG00013.sas
* 014_bsf_ELG00014.sas
* 015_bsf_ELG00015.sas
* 016_bsf_ELG00016.sas
* 017_bsf_ELG00017.sas
* 018_bsf_ELG00018.sas
* 020_bsf_ELG00020.sas
* 021_bsf_ELG00021.sas
* 022_bsf_TPL00002.sas
* 022b_bsf_ELG00022.sas

The final program, 023_bsf_final.sas, joins the extracts together to create the BSF output.

## Inpatient Hospital (IP) Claims

Note: The IP TAF consists of two separate segments. See the TAF data dictionaries for further information.

The program IP_build.sas is the primary code for building the IP Claims TAF; all other programs are called from this program and do not need to be run separately.

The program AWS_IP_Macros.sas contains SAS macro functions specific to the IP claims. 

## Long-Term Care (LT) Claims

Note: The LT TAF consists of two separate segments. See the TAF data dictionaries for further information.

The program LT_build.sas is the primary code for building the LT Claims TAF; all other programs are called from this program and do not need to be run separately.

The program AWS_LT_Macros.sas contains SAS macro functions specific to the LT claims. 

## Pharmacy (RX) Claims

Note: The RX TAF consists of two separate segments. See the TAF data dictionaries for further information.

The program RX_build.sas is the primary code for building the RX Claims TAF; all other programs are called from this program and do not need to be run separately.

The program AWS_RX_Macros.sas contains SAS macro functions specific to the RX claims. 

## Other Services (OT) Claims

Note: The OT TAF consists of two separate segments. See the TAF data dictionaries for further information.

The program OT_build.sas is the primary code for building the OT Claims TAF; all other programs are called from this program and do not need to be run separately.

The program AWS_OT_Macros.sas contains SAS macro functions specific to the OT claims. 

## Managed Care Plans (MCP)

Note: The MCP TAF consists of four separate segments. See the TAF data dictionaries for further information.

The program 101_mc_build.sas is the primary code for building the MCP TAF; all other programs are called from this program and do not need to be run separately.

The programs 002_mc_macros.sas and 003_mc_selection_macros.sas contain SAS macro functions specific to the managed care data. 

## Providers (PRV)

Note: The PRV TAF consists of nine separate segments. See the TAF data dictionaries for further information.

The program 101_prvdr_build.sas is the primary code for building the PRV TAF; all other programs are called from this program and do not need to be run separately.

The programs 002_prvdr_macros.sas and 003_prvdr_selection_macros.sas contain SAS macro functions specific to the provider data. 

## Annual Demographic and Eligibility (DE)

Note: The Annual DE TAF consists of eight separate segments. See the TAF data dictionaries for further information.

The program de_annual_build.sas is a driver program that calls each of the other programs used in creating the Annual DE TAF. This is the only program that is run to create the DE files; all other programs are called by this file and do not need to be run separately.

The next group of programs are organized according to the DE segment that they produce. The program names reflect the DE segment as defined in the TAF documentation.

* 001_base.sas
* 002_eligibility_dates.sas
* 003_address_phone.sas
* 005_managed_care.sas
* 006_waiver.sas
* 007_mfp.sas
* 008_hh_spo.sas
* 009_disability_need.sas

The program annual_macros.sas contains SAS macro functions that are specific to the creation of the Annual DE TAF. 

## Annual Use and Payment (UP)

Note: The Annual UP TAF consists of two separate segments, the base and type of program (TOP) segment. See the TAF data dictionaries for further information.

The program up_annual_build.sas is a driver program that calls each of the other programs used in creating the Annual UP TAF. This is the only program that is run to create the UP files; all other programs are called by this file and do not need to be run separately.

The following programs are part of creating the base segment of the UP TAF:

* 000_up_base_de.sas
* 001_up_base_hdr.sas
* 002_up_base_hdr_comb.sas
* 003_up_base_line.sas
* 004_up_base_line_comb.sas
* 005_up_base_lt.sas
* 006_up_base_ip.sas
* 007_up_base_deliv.sas
* 008_up_base_fnl.sas

The following programs are part of creating the type of program (TOP) segment of the UP TAF:

* 009_up_top.sas
* 010_up_top_fnl.sas

The program annual_up_macros.sas contains SAS macro functions that are specific to the creation of the Annual UP TAF.

## Annual Managed Care Plan (APL)

The program pl_annual_build.sas is a driver program that calls each of the other programs used in creating the APL TAF. This is the only program that is run to create the APL file; all other programs are called by this file and do not need to be run separately.

The following programs are called in sequence as part of creating the APL TAF:

* 001_base_pl.sas
* 003_lctn.sas
* 004_sarea.sas
* 005_oa.sas
* 006_enrlmt.sas

The program annual_macros_pl.sas contains SAS macro functions that are specific to the creation of the APL TAF.

## Annual Provider (APR)

The program pr_annual_build.sas is a driver program that calls each of the other programs used in creating the APR TAF. This is the only program that is run to create the APR file; all other programs are called by this file and do not need to be run separately.

The following programs are called in sequence as part of creating the APR TAF:

* 001_base_pr.sas
* 003_lctn_pr.sas
* 004_lcns.sas
* 005_id.sas
* 006_txnmy.sas
* 007_enrlmt.sas
* 008_grp.sas
* 009_pgm.sas
* 010_bed.sas

The program annual_macros_pr.sas contains SAS macro functions that are specific to the creation of the APR TAF.


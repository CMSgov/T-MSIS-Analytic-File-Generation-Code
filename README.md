# CMS T-MSIS Analytic File (TAF) Generation Code

This project aims to provide transparency to state Medicaid agencies and other stakeholders who are interested in the logic and processes that are used to create CMS’ interim T-MSIS Analytic Files (TAF). These new TAF data sets exist alongside T-MSIS and serve as an alternate data source tailored to meet the broad research needs of the Medicaid and CHIP data user community. 

Background information about the TAF can be found on Medicaid.Gov at this link: 
https://www.medicaid.gov/medicaid/data-systems/macbis/medicaid-chip-research-files/transformed-medicaid-statistical-information-system-t-msis-analytic-files-taf/index.html

## Prerequisites

The TAF generation code in this repository is SAS code containing explicit pass-through SQL that passes the processing steps to a specific CMS AWS (Amazon Web Services) Redshift environment where the data reside. The code in this repository will not function outside of that environment. Furthermore, the code depends upon a specific set of T-MSIS input files that are produced and maintained in the Redshift environment. The TAF generation code is provided for readers who wish to better understand the TAF generation process, but it is not expected that most readers will be able to use the code in this repository to generate TAFs.

## Getting the code

The easiest way to obtain the code is to clone it with git. If you're not familiar with git, a tool like [Github Desktop](https://desktop.github.com/) or [SourceTree](https://www.sourcetreeapp.com/) can help make the experience easier. The HTTPS link is https://github.com/CMSgov/T-MSIS-Analytic-File-Generation-Code.git

If you're familiar with git and just want to work from the command line, you just need to run:
```
git clone https://github.com/CMSgov/T-MSIS-Analytic-File-Generation-Code.git
```
If you would prefer not to use git, you can also download the most recent code as a [ZIP file](https://github.com/CMSgov/T-MSIS-Analytic-File-Generation-Code/archive/master.zip).

## Running the Code

The TAF generation code is written in Redshift SQL executed through explicit SQL passthrough embedded within a SAS code wrapper. The format of the T-MSIS data is documented [here](https://www.medicaid.gov/medicaid-chip-program-information/by-topics/data-and-systems/downloads/t-msis-data-dictionary.zip).

This code executes after state T-MSIS data submissions are received and ingested into the CMS AWS Redshift database.   The ingest process contains logic that integrates new submission data with existing data which can cause the original content to be modified. Additional input files, e.g. the T-MSIS final action table, diagnosis code lookup tables, and procedure code lookup tables, are also required. Therefore, merely running this T-MSIS Analytic File (TAF) generation code against T-MSIS submission data will not produce identical results compared to CMS’ TAF.

## More technical documentation

Additional technical documentation can be found in this repository in the file [Technical_Documentation.md](technical_documentation.md).
Supplementary information regarding the data quality of state T-MSIS Analytic Files (TAF) Research Identifiable Files (RIF) can be referenced [here](https://www.medicaid.gov/dq-atlas/welcome).

## Contributing

We would be happy to receive suggestions on how to fix bugs or make improvements, though we will not support changes made through this repository. Instead, please send your suggestions to [MACBISData@cms.hhs.gov](mailto:MACBISData@cms.hhs.gov).    

## Public domain

This project is in the worldwide [public domain](https://github.com/CMSgov/T-MSIS-Analytic-File-Generation-Code/blob/master/LICENSE.md). 

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).


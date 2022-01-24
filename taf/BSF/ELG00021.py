from taf.BSF import BSF_Runner

from taf.BSF.ELG import ELG


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class ELG00021(ELG):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00021', 'TMSIS_ENRLMT_TIME_SGMT_DATA', 'ENRLMT_EFCTV_DT', 'ENRLMT_END_DT')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def processEnrollment(self, enrl_type, enrl_type_cd):

        #  Step 0: A) Set effective and end dates to death date if death date occurs before.
        #          B) Set null end dates to December 31, 9999
        z = f"""
            create or replace temporary view {self.tab_no}_{enrl_type}_step0 as

            select *
            from {self.tab_no}_v
            where enrlmt_type_cd = %nrbquote('{enrl_type_cd}')
            """
        self.bsf.append(type(self).__name__, z)

        #  Steps 1-2: A) Remove records where beneficiary died before start of month
        # 	           B) Identify records completely within another record's date range
        z = f"""
            create or replace temporary view {self.tab_no}_{enrl_type}_step1 as

            select *
                    --  Create a unique date ID to filter on later
                    ,trim(submtg_state_cd ||'-'||msis_ident_num || '-' ||
                            cast(rank() over (partition by submtg_state_cd ,msis_ident_num
                            order by submtg_state_cd, msis_ident_num, {self.eff_date}, {self.end_date}) as char(3))) as dateId
            from {self.tab_no}_{enrl_type}_step0
            """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}_{enrl_type}_step1_overlaps as

            select t1.*
            from {self.tab_no}_{enrl_type}_step1 t1
            inner join {self.tab_no}_{enrl_type}_step1 t2
            --  Join records for beneficiary to each other, but omit matches where it's the same record
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num = t2.msis_ident_num
                and t1.dateId <> t2.dateId
            --  Get every dateID where their effective date is greater than or equal to another record's effective date
                AND their end date is less than or equal to that other record's end date.
            where date_cmp(t1.{self.eff_date},t2.{self.eff_date}) in(0,1)
              and date_cmp(t1.{self.end_date},t2.{self.end_date}) in (-1,0)
            """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}_{enrl_type}_step2 as

            select t1.*

            from {self.tab_no}_{enrl_type}_step1 t1
            --  Join initial date to overlapping dateIDs and remove
            left join {self.tab_no}_{enrl_type}_step1_overlaps t2
            on t1.dateid = t2.dateid
            where t2.dateid is null
            """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}_{enrl_type}_step3 as

            select submtg_state_cd, msis_ident_num
                ,min({self.eff_date}) as {self.eff_date}
                ,max({self.end_date}) as {self.end_date}

            from
            (
            select submtg_state_cd ,msis_ident_num ,{self.eff_date} ,{self.end_date}
                    ,sum(C) over (partition by submtg_state_cd, msis_ident_num
                                order by {self.eff_date}, {self.end_date}
                                rows UNBOUNDED PRECEDING) as G
            from
            (
            select submtg_state_cd
                ,msis_ident_num
                ,{self.eff_date}
                ,{self.end_date}
                ,m_eff_dt
                ,m_end_dt
                ,decode(sign({self.eff_date}-nvl(m_end_dt+1,{self.eff_date})),1,1,0) as C
            from
            (select submtg_state_cd
                ,msis_ident_num
                ,{self.eff_date}
                ,{self.end_date}
                ,lag({self.eff_date}) over (partition by submtg_state_cd, msis_ident_num
                                        order by {self.eff_date}, {self.end_date}) as m_eff_dt
                ,lag({self.end_date}) over (partition by submtg_state_cd, msis_ident_num
                                        order by {self.eff_date}, {self.end_date}) as m_end_dt
            from {self.tab_no}_{enrl_type}_step2
            order by {self.eff_date}, {self.end_date}) s1 ) s2 ) s3
            group by submtg_state_cd, msis_ident_num, g

            """
        self.bsf.append(type(self).__name__, z)

        #  Step 4: Prep long table for transposition
        z = f"""
            create or replace temporary view {self.tab_no}_{enrl_type}_step4 as

            select *
                    ,row_number() over (partition by submtg_state_cd ,msis_ident_num
                            order by submtg_state_cd, msis_ident_num, {self.eff_date}, {self.end_date}) as keeper
                    -- If data indicates they were deceased well before their original effective dates, set to 0
                    ,greatest(datediff(day,greatest(&st_dt,{self.eff_date}),
                            least({self.bsf.RPT_PRD}, {self.end_date}))+1,0) as NUM_DAYS
                    ,case when date_cmp({self.end_date},{self.bsf.RPT_PRD}) in(0,1) then 1 else 0 end as ELIG_LAST_DAY
            from  {self.tab_no}_{enrl_type}_step3
            """
        self.bsf.append(type(self).__name__, z)

        #  Determine Max number of keeper
        # select max_keep into :max_keep
        # from (select * from connection to tmsis_passthrough
        #       (select max(keeper) as max_keep from {self.tab_no}_{enrl_type}_step4))

        #  Step 5: Transpose data from long to wide and create &type specific eligibility columns
        #  Allow for up to 16 columns in array. If there are <16 total spells, use nulls
        z = f"""
            create or replace temporary view {enrl_type}_spells as

            select m.*
                ,1 as {enrl_type}_ENR

                %do I=1 %to 16
                %if %eval(&I <= &max_keep) %then
                %do
                    ,t&I..{self.eff_date} as {enrl_type}_ENRLMT_EFF_DT_&I.
                    ,t&I..{self.end_date} as {enrl_type}_ENRLMT_END_DT_&I.
                %end %else
                %do
                    ,cast(null as date) as {enrl_type}_ENRLMT_EFF_DT_&I.
                    ,cast(null as date) as {enrl_type}_ENRLMT_END_DT_&I.
                %end
                %end

            from (select submtg_state_cd
                        ,msis_ident_num
                        ,sum(NUM_DAYS) as DAYS_ELIG_IN_MO_CNT
                        ,max(ELIG_LAST_DAY) as ELIG_LAST_DAY
                from {self.tab_no}_{enrl_type}_step4 group by submtg_state_cd, msis_ident_num) m
                %do I=1 %to 16
                %if %eval(&I <= &max_keep) %then

                    %do
                    %tbl_joiner(&I)
                    %end
                %end
            """
        self.bsf.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        # For bene/state combinations, remove records that overlap across enrollment types.

        # First use sort order to identify which records are priority across enrollment types
        z = f""" create or replace temporary view {self.tab_no}_step1 as
                select *
                ,row_number() over (partition by submtg_state_cd
                                            ,msis_ident_num
                                order by submtg_state_cd,
                                        msis_ident_num,
                                        TMSIS_RPTG_PRD desc,
                                        {self.eff_date} desc,
                                        {self.end_date} desc,
                                        REC_NUM desc,
                                        coalesce(enrlmt_type_cd,'999')) as ORDER_FLAG
                from {self.tab_no}
                """
        self.bsf.append(type(self).__name__, z)

        # Next match records to themselves and limit to where enrollment types are different.

        # Create a flag that indicates if the record overlaps with another in any time frame.
        # Set the flag to 1 only if it overlaps and is for a lower prioty record. Ie - If
        # there is an overlap, it will be true for both records, so only flag the higher order
        # (lesser priority) for removal.
        z = f"""create or replace temporary view {self.tab_no}_step2 as
                select t1.*

            ,case when ((t1.{self.eff_date} between t2.{self.eff_date} and t2.{self.end_date}) or
                        (t2.{self.eff_date} between t1.{self.eff_date} and t1.{self.end_date}))
                    and t1.ORDER_FLAG > t2.ORDER_FLAG then 1 else 0 end as OVR_LAP_RMV

                from {self.tab_no}_step1 t1
                left join {self.tab_no}_step1 t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num = t2.msis_ident_num
                and coalesce(t1.enrlmt_type_cd,'X') <> coalesce(t2.enrlmt_type_cd,'X')

                where OVR_LAP_RMV = 1 /* Only keep records to be removed in this table
                """
        self.bsf.append(type(self).__name__, z)

        # Next match the records to be removed back to the ordered records and drop the record
        # if it appears in the removal table.
        z = f""" create or replace temporary view {self.tab_no}_step3 as
                select t1.*
                from {self.tab_no}_step1 t1
                left join {self.tab_no}_step2 t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num = t2.msis_ident_num
                and t1.order_flag = t2.order_flag
                where t2.msis_ident_num is null
                """
        self.bsf.append(type(self).__name__, z)

        #  After records have been sanitized of overlaps across enrollment types, begin the processing
        #  for within enrollment type collapsing.
        z = f"""
            create or replace temporary view {self.tab_no}_v as
            as
            select distinct
                e21.submtg_state_cd
                ,e21.msis_ident_num
                ,e21.tmsis_run_id
                ,e21.enrlmt_type_cd
                ,greatest(case when date_cmp(e02.death_date,{self.eff_date}) in(-1,0)
                        or (e02.death_date is not null and {self.eff_date} is null) then e02.death_date
                    else {self.eff_date} end, &st_dt::date) as {self.eff_date}
                ,least(case when date_cmp(e02.death_date,{self.end_date}) in(-1,0)
                        or (e02.death_date is not null and {self.end_date} is null) then e02.death_date
                    when {self.end_date} is null then '31DEC9999' else {self.end_date} end, {self.bsf.RPT_PRD}::date) as {self.end_date}
            from {self.tab_no}_step3 e21
            left join ELG00002_{self.bsf.BSF_FILE_DATE}_uniq e02
            on e21.submtg_state_cd = e02.submtg_state_cd
            and e21.msis_ident_num = e02.msis_ident_num
            where e21.msis_ident_num is not null
                /* Filter out records where the death_date is before the start of the month
                /* This is separate from process enrollment because it includes UNK enrollment types
                and date_cmp(least(&st_dt,nvl(death_date,{self.end_date},'31DEC9999')),&st_dt) in(0,1)
                /* Also remove any records where the effective date is after their death date
                and date_cmp({self.eff_date},least(death_date,'31DEC9999')) <> 1
            """
        self.bsf.append(type(self).__name__, z)

        self.processEnrollment('MDCD', 1)
        self.processEnrollment('CHIP', 2)

        #  Get master list of eligible beneficiaries and check for unknown enrollment info
        z = f"""
            create or replace temporary view {self.tab_no}_buckets as

            select
                b.submtg_state_cd
                ,b.msis_ident_num
                ,max(b.tmsis_run_id) as tmsis_run_id

                %do I=1 %to &DAYS_IN_MONTH
                yr_mn = %substr(&rpt_out,3,7)
                %if %eval(&I<10 ) %then
                dt_to_ck = %nrbquote('0&I.&yr_mn')
                    %else dt_to_ck = %nrbquote('&I.&yr_mn')

                ,max(case when cast(ENRLMT_TYPE_CD as integer) in(1,2)
                        and date_cmp({self.eff_date},&dt_to_ck) in(-1,0)
                        and date_cmp({self.end_date},&dt_to_ck) in(1,0) then 1 else 0 end) as DT_CHK_&I
                %end

                ,sum(case when cast(ENRLMT_TYPE_CD as integer) not in(1,2) or ENRLMT_TYPE_CD is null then 1 else 0 end) as bucket_c
                from {self.tab_no}_v b
                group by b.submtg_state_cd, b.msis_ident_num
        """
        self.bsf.append(type(self).__name__, z)

        # create table states as
        # select * from connection to tmsis_passthrough
        # ( select distinct submtg_state_cd from {self.tab_no})

        #  Create state abbrev case statement using SAS function and macro variables
        # title 'Generated case statements for FIPS codes'
        # select cat("when b.submtg_state_cd='",submtg_state_cd,"' then ","'",fipstate(input(submtg_state_cd,2.)),"'")
        # into :state_format
        # separated by ' '
        # from states

        #  Combine master list to Medicaid and CHIP specific tables
        z = f"""
            create or replace temporary view {self.tab_no}_combined as

            select b.*,
                    coalesce(m.MDCD_ENR,0) as MDCD_ENR,
                    coalesce(c.CHIP_ENR,0) as CHIP_ENR,
                %do I=1 %to 16
                    m.MDCD_ENRLMT_EFF_DT_&I.,
                    m.MDCD_ENRLMT_END_DT_&I.,
                %end

                %do I=1 %to 16
                    c.CHIP_ENRLMT_EFF_DT_&I.,
                    c.CHIP_ENRLMT_END_DT_&I.,
                %end

                    --  Number of days in month equal to last day value of month
                    &DAYS_IN_MONTH as DAYS_IN_MONTH,

                    --  Sum the number of days in the month that they had enrollment in Medicad or CHIP
                    DT_CHK_1
                    %do I=2 %to &DAYS_IN_MONTH
                        + DT_CHK_&I
                    %end as DAYS_ELIG_IN_MO_CNT,

                    --  Eligible entire month if the number of days in the month is <= sum of days in Medicaid or CHIP
                    case when DT_CHK_1 = 1
                    %do I=2 %to &DAYS_IN_MONTH
                        and DT_CHK_&I = 1
                    %end then 1 else 0 end as ELIGIBLE_ENTIRE_MONTH_IND,

                    --  Eligible Last day if they are eligible on the last day for any of their Medicaid or CHIP records
                    greatest(m.ELIG_LAST_DAY,c.ELIG_LAST_DAY,0) as ELIGIBLE_LAST_DAY_OF_MONTH_IND,

                    case when m.MDCD_ENR = 1 then 1
                        when c.CHIP_ENR  = 1 then 2
                        else null end as ENROLLMENT_TYPE_FLAG,

                    --  Single Enroll if only one spell across Medicaid and CHIP and no unknown
                    case when b.bucket_c=1 or
                            (MDCD_ENR=1 and CHIP_ENR=1) or
                            (m.MDCD_ENRLMT_EFF_DT_1 is not null and m.MDCD_ENRLMT_EFF_DT_2 is not null) or
                            (c.CHIP_ENRLMT_EFF_DT_1 is not null and c.CHIP_ENRLMT_EFF_DT_2 is not null)
                        then 0 else 1 end as SINGLE_ENR_FLAG,

                    case &state_format end as ST_ABBREV

                from {self.tab_no}_buckets b

                left outer join MDCD_SPELLS m
                on b.submtg_state_cd=m.submtg_state_cd
                and b.msis_ident_num=m.msis_ident_num

                left outer join CHIP_SPELLS c
                on b.submtg_state_cd=c.submtg_state_cd
                and b.msis_ident_num=c.msis_ident_num
                """
        self.bsf.append(type(self).__name__, z)

        #  Output table with SSN_IND and region column to provide master table for final program
        # Add ssn_ind from initial max ID pull
        z = f"""

            create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq as

            select
                c.*
                ,s.ssn_ind
                ,case when ST_ABBREV in('CT','MA','ME','NH','RI','VT') 			    then '01'
                        when ST_ABBREV in('NJ','NY','PR','VI')						then '02'
                        when ST_ABBREV in('DE','DC','MD','PA','VA','WV') 			then '03'
                        when ST_ABBREV in('AL','FL','GA','KY','MS','NC','SC','TN')  then '04'
                        when ST_ABBREV in('IL','IN','MI','MN','OH','WI')            then '05'
                        when ST_ABBREV in('AR','LA','NM','OK','TX')				    then '06'
                        when ST_ABBREV in('IA','KS','MO','NE')						then '07'
                        when ST_ABBREV in('CO','MT','ND','SD','UT','WY')			then '08'
                        when ST_ABBREV in('AZ','CA','HI','NV','AS','GU','MP')		then '09'
                        when ST_ABBREV in('AK','ID','OR','WA')						then '10'
                        else '11' end as REGION
                from {self.tab_no}_combined c

            left join ssn_ind s
            on c.submtg_state_cd=s.submtg_state_cd
                """
        self.bsf.append(type(self).__name__, z)


# -----------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work

#   ii. moral rights retained by the original author(s) and/or performer(s)

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive) and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>

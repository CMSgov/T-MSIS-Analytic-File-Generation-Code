from taf.BSF import BSF_Runner

from taf.BSF.ELG import ELG


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class ELG00002(ELG):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00002', 'TMSIS_PRMRY_DMGRPHC_ELGBLTY', 'PRMRY_DMGRPHC_ELE_EFCTV_DT', 'PRMRY_DMGRPHC_ELE_END_DT')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        # Create temp table to determine which beneficiaries have multiple records
        z = f"""
                create or replace temporary view {self.tab_no}_recCt as
                select
                    submtg_state_cd,
                    msis_ident_num,
                    count(TMSIS_RUN_ID) as recCt
                from
                    {self.tab_no}
                group by
                    submtg_state_cd,
                    msis_ident_num
            """
        self.bsf.append(type(self).__name__, z)

        # select recCt, count(msis_ident_num) as beneficiaries from ELG00002_recCt group by recCt

        # Set aside table data for benes with only one record
        #  -- why is there GNDR_CD and GNDR_CODE?
        z = f"""
                create or replace temporary view {self.tab_no}_uniq as
                select
                    t1.*,
                    upper(GNDR_CD) as GNDR_CODE,
                    1 as KEEP_FLAG
                from
                    {self.tab_no} as t1
                inner join
                    {self.tab_no}_recCt as t2
                        on t1.submtg_state_cd = t2.submtg_state_cd
                        and t1.msis_ident_num = t2.msis_ident_num
                        and t2.recCt = 1
            """
        self.bsf.append(type(self).__name__, z)

        created_vars = "upper(GNDR_CD) as GNDR_CODE"
        sort_key = "coalesce(gndr_cd,'xx')||coalesce(cast(birth_dt as char(10)),'xx')||coalesce(cast(death_dt as char(10)),'xx')"
        self.MultiIds(created_vars, sort_key)

        # Number of beneficiary with unique records in {self.tab_no}
        # select count(msis_ident_num) as beneficiaries from ELG00002_uniq

        z = f"""
                create or replace temporary view {self.tab_no}_death as
                select
                    submtg_state_cd,
                    msis_ident_num,

                    DEATH_DT as DEATH_DATE,

                    (case when DEATH_DT is not null and (DEATH_DT <= '2021-01-31') then 1 else 0 end) as DECEASED_FLG,

                    row_number() over (
                        partition by
                            submtg_state_cd,
                            msis_ident_num
                        order by
                            submtg_state_cd,
                            msis_ident_num,
                            TMSIS_RPTG_PRD desc,
                            {self.eff_date} desc,
                            {self.end_date} desc,
                            REC_NUM desc,
                            DEATH_DT desc
                    ) as best_record

                from {self.tab_no}
            """
        self.bsf.append(type(self).__name__, z)

        # ELG00002A

        # select * from ELG00002_death
        # create temp table {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq
        z = f"""
                create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq as
                select
                    *,
                    case when BIRTH_DT is null or AGE < -1 then null
                        when AGE between -1  and 0 then 1
                        when AGE between  1  and 5 then 2
                        when AGE between  6 and 14 then 3
                        when AGE between 15 and 18 then 4
                        when AGE between 19 and 20 then 5
                        when AGE between 21 and 44 then 6
                        when AGE between 45 and 64 then 7
                        when AGE between 65 and 74 then 8
                        when AGE between 75 and 84 then 9
                        when AGE between 85 and 125 then 10
                        else null end as AGE_GROUP_FLG
                    from (
                        select
                            *,
                            case when AGE_CALC > 125 then 125
                                when AGE_CALC < -1 then null
                                else AGE_CALC end as AGE
                        from (
                            select
                                comb.*,
                                coalesce(d.DECEASED_FLG,0) as DECEASED_FLG,
                                d.DEATH_DATE,

                                case when BIRTH_DT is null then null
                                    when coalesce(d.DECEASED_FLG,0) = 1
                                    then floor(datediff(d.DEATH_DATE, comb.BIRTH_DT)/365.25)
                                    else floor(datediff('2021-01-31', comb.BIRTH_DT)/365.25) end as AGE_CALC

                            from
                                (select * from {self.tab_no}_uniq
                                    union all
                                select * from {self.tab_no}_multi) comb

                                -- Compute deceased flag and death_dt and join back to unique table data
                                left join {self.tab_no}_death d

                            on comb.SUBMTG_STATE_CD = d.SUBMTG_STATE_CD
                            and comb.msis_ident_num = d.msis_ident_num
                            and d.best_record = 1
                        )
                    )
                """
        self.bsf.append(type(self).__name__, z)

        # Number of records in {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq
        # select count(submtg_state_cd) as tot_ct from ELG00002_blah_uniq
        # select * from ELG00002_blah_uniq


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

from taf.BSF import BSF_Runner
from taf.BSF.BSF_Metadata import BSF_Metadata

from taf.BSF.ELG import ELG


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class ELG00014(ELG):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00014', 'TMSIS_MC_PRTCPTN_DATA', 'MC_PLAN_ENRLMT_EFCTV_DT', 'MC_PLAN_ENRLMT_END_DT')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def mc_plan(self, max_keep):

        mc_plans = []
        new_line = '\n\t\t\t'
        for i in list(range(1, 16 + 1)):
            if i <= max_keep:
                mc_plans.append(f"""
                    , t{i}.MC_PLAN_IDENTIFIER as MC_PLAN_ID{i}
                    , t{i}.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD{i}
                """.format())
            else:
                mc_plans.append(f"""
                    , cast(null as varchar(12)) as MC_PLAN_ID{i}
                    , cast(null as varchar(2)) as MC_PLAN_TYPE_CD{i}
                """.format())

        return new_line.join(mc_plans)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        ENRLD_MC_PLAN_TYPE_CODE = f"""
                case  when length(trim(enrld_mc_plan_type_cd))=1
                    and trim(enrld_mc_plan_type_cd) <> ''
                    then lpad(enrld_mc_plan_type_cd,2,'0') else enrld_mc_plan_type_cd end
            """

        mc_plan = f"""
            case when trim(mc_plan_id)  in ('0','00','000','0000','00000','000000','0000000',
                                            '00000000','000000000','0000000000','00000000000','000000000000',
                                            '8','88','888','8888','88888','888888','8888888',
                                            '88888888','888888888','8888888888','88888888888','888888888888',
                                            '9','99','999','9999','99999','999999','9999999',
                                            '99999999','999999999','9999999999','99999999999','999999999999','')
            then null else trim(mc_plan_id) end
            """

        #  Reset plan type code for specific case
        z = f"""
            create or replace temporary view {self.tab_no}_step1 as

            select distinct
                submtg_state_cd,
                msis_ident_num,
                rec_num,
                {self.eff_date},
                {self.end_date},
                tmsis_rptg_prd,
                {mc_plan} as MC_PLAN_IDENTIFIER,
                case when ({mc_plan}) is null and {ENRLD_MC_PLAN_TYPE_CODE} = '00' then null
                    else {ENRLD_MC_PLAN_TYPE_CODE} end as ENRLD_MC_PLAN_TYPE_CODE,

                row_number() over (partition by submtg_state_cd,
                                        msis_ident_num,
                                        {mc_plan},
                                        (case when ({mc_plan}) is null and {ENRLD_MC_PLAN_TYPE_CODE} = '00' then null
                                         else {ENRLD_MC_PLAN_TYPE_CODE} end)

                            order by submtg_state_cd,
                                        msis_ident_num,
                                        TMSIS_RPTG_PRD desc,
                                        {self.eff_date} desc,
                                        {self.end_date} desc,
                                        REC_NUM desc,
                                        {mc_plan},
                                        (case when ({mc_plan}) is null and {ENRLD_MC_PLAN_TYPE_CODE} = '00' then null
                                         else {ENRLD_MC_PLAN_TYPE_CODE} end)) as mc_deduper

                from (select * from {self.tab_no}
                    where enrld_mc_plan_type_cd is not null
                        or mc_plan_id is not null) t1
                """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}_step2 as

            select *,
                row_number() over (partition by submtg_state_cd,
                                        msis_ident_num
                            order by submtg_state_cd,
                                        msis_ident_num,
                                        TMSIS_RPTG_PRD desc,
                                        {self.eff_date} desc,
                                        {self.end_date} desc,
                                        REC_NUM desc,
                                        MC_PLAN_IDENTIFIER,
                                        ENRLD_MC_PLAN_TYPE_CODE) as keeper

                from (select * from {self.tab_no}_step1
                    where (enrld_mc_plan_type_code is not null
                        or mc_plan_identifier is not null) and mc_deduper=1) t1
                """
        self.bsf.append(type(self).__name__, z)

        # title "Number of waiver codes per beneficiary in {self.tab_no}"
        # select * from connection to tmsis_passthrough
        #  ( select keeper,count(msis_ident_num) as plans from {self.tab_no}_step2 group by keeper ) order by keeper

        # Determine Max number of Keeper Records
        # select max_keep into :max_keep
        # from (select * from connection to tmsis_passthrough
        #       (select max(keeper) as max_keep from {self.tab_no}_step2))

        # %check_max_keep(16)
        max_keep = 16

        z = f"""
            create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq as

            select
                t1.submtg_state_cd
                ,t1.msis_ident_num

                { self.mc_plan(max_keep) }

                from (select * from {self.tab_no}_step2 where keeper=1) t1

                { BSF_Metadata.dedup_tbl_joiner('ELG00014', range(2, 17), max_keep) }

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

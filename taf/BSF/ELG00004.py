from taf.BSF import BSF_Runner



from taf.BSF.ELG import ELG


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class ELG00004(ELG):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00004', 'TMSIS_ELGBL_CNTCT', 'ELGBL_ADR_EFCTV_DT', 'ELGBL_ADR_END_DT')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        loc = 'home'
        created_vars = f"""trim(ELGBL_STATE_CD)||trim(ELGBL_CNTY_CD) as ENROLLEES_COUNTY_CD_HOME,
                elgbl_line_1_adr as elgbl_line_1_adr_{loc},
                elgbl_line_2_adr as elgbl_line_2_adr_{loc},
                elgbl_line_3_adr as elgbl_line_3_adr_{loc},
                elgbl_city_name as elgbl_city_name_{loc},
                elgbl_zip_cd as elgbl_zip_cd_{loc},
                elgbl_cnty_cd as elgbl_cnty_cd_{loc},
                lpad(elgbl_state_cd,2,'0') as elgbl_state_cd_{loc},
                elgbl_phne_num as elgbl_phne_num_{loc}"""

        #  Create temp table to determine which beneficiaries have multiple records 
        z = f"""
            create or replace temporary view {self.tab_no}_recCt
            select
                submtg_state_cd,
                msis_ident_num,
                count(msis_ident_num) as recCt
                from {self.tab_no}
                where ELGBL_ADR_TYPE_CD in ('01','1')
                group by submtg_state_cd, msis_ident_num
        """
        self.bsf.append(type(self).__name__, z)

        # title "Number of records per beneficiary in {self.tab_no}"
        # select * from connection to tmsis_passthrough
        #  ( select recCt,count(msis_ident_num) as beneficiaries from {self.tab_no}_recCt asgroup by recCt ) order by recCt

        #  Set aside table data for benes with only one record 
        z = f"""
            create or replace temporary view {self.tab_no}_uniq
            select t1.*,
                {created_vars},
                1 as KEEP_FLAG

                from {self.tab_no} t1
                inner join {self.tab_no}_recCt as t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num  = t2.msis_ident_num
                and t2.recCt=1

                where ELGBL_ADR_TYPE_CD in ('01','1')
        """
        self.bsf.append(type(self).__name__, z)

        # title "Number of beneficiary with unique records in {self.tab_no}"
        # select * from connection to tmsis_passthrough
        #  ( select count(msis_ident_num) as beneficiaries from {self.tab_no}_uniq )

        sort_key = """coalesce(trim(elgbl_line_1_adr),'xx') || coalesce(trim(elgbl_city_name),'xx') || coalesce(trim(elgbl_cnty_cd),'xx') ||
                      coalesce(trim(elgbl_phne_num),'xx') || coalesce(trim(elgbl_state_cd),'xx') || coalesce(trim(elgbl_zip_cd),'xx')"""
        self.MultiIds(self, sort_key, "ELGBL_ADR_TYPE_CD in ('01','1')")

        # title "Number of beneficiares who were processed for duplicates in {self.tab_no}"
        # select * from connection to tmsis_passthrough
        #  ( select count(msis_ident_num) as beneficiaries from {self.tab_no}_multi )

        loc = 'mail'
        created_vars = f"""
            trim(ELGBL_STATE_CD)||trim(ELGBL_CNTY_CD) as ENROLLEES_COUNTY_CD_HOME,
            elgbl_line_1_adr as elgbl_line_1_adr_{loc},
            elgbl_line_2_adr as elgbl_line_2_adr_{loc},
            elgbl_line_3_adr as elgbl_line_3_adr_{loc},
            elgbl_city_name as elgbl_city_name_{loc},
            elgbl_zip_cd as elgbl_zip_cd_{loc},
            elgbl_cnty_cd as elgbl_cnty_cd_{loc},
            elgbl_state_cd as elgbl_state_cd_{loc},
            elgbl_phne_num as elgbl_phne_num_{loc}
        """

        #  Create temp table to determine which beneficiaries have multiple records 
        z = f"""
            create or replace temporary view {self.tab_no}A_recCt
            select
                submtg_state_cd,
                msis_ident_num,
                count(msis_ident_num) as recCt
                from {self.tab_no}
                where ELGBL_ADR_TYPE_CD in ('06','6')
                group by submtg_state_cd, msis_ident_num
        """
        self.bsf.append(type(self).__name__, z)

        # title "Number of records per beneficiary in {self.tab_no}A"
        # select * from connection to tmsis_passthrough
        #  ( select recCt,count(msis_ident_num) as beneficiaries from {self.tab_no}A_recCt group by recCt ) order by recCt

        #  Set aside table data for benes with only one record 

        z = f"""
            create or replace temporary view {self.tab_no}A_uniq
            select t1.*,
                    {created_vars},
                    1 as KEEP_FLAG
                from {self.tab_no} t1
                inner join {self.tab_no}A_recCt  t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num  = t2.msis_ident_num
                and t2.recCt=1

                where ELGBL_ADR_TYPE_CD in ('06','6')
        """
        self.bsf.append(type(self).__name__, z)

        # title "Number of beneficiary with unique records in {self.tab_no}A"
        # select * from connection to tmsis_passthrough
        #  ( select count(msis_ident_num) as beneficiaries from {self.tab_no}A_uniq )

        sort_key = """coalesce(trim(elgbl_line_1_adr),'xx') || coalesce(trim(elgbl_city_name),'xx') || coalesce(trim(elgbl_cnty_cd),'xx') ||
                      coalesce(trim(elgbl_phne_num),'xx') || coalesce(trim(elgbl_state_cd),'xx') || coalesce(trim(elgbl_zip_cd),'xx')"""

        self.MultiIds(self, sort_key, "ELGBL_ADR_TYPE_CD in ('06','6')", 'A')

        # Union together tables for a permanent table 
        z = f"""
            create or replace temporary view {self.tab_no}_uniq_step1
            select * from {self.tab_no}_uniq
            union all
            select * from {self.tab_no}_multi
        """
        self.bsf.append(type(self).__name__, z)
        
        z = f"""
            create or replace temporary view {self.tab_no}_uniq_step2
            select * from {self.tab_no}A_uniq
            union all
            select * from {self.tab_no}A_multi
        """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq
            select
                coalesce(t1.msis_ident_num,t2.msis_ident_num) as msis_ident_num,
                coalesce(t1.submtg_state_cd,t2.submtg_state_cd) as submtg_state_cd,
                t1.ENROLLEES_COUNTY_CD_HOME,
                coalesce(t1.elgbl_adr_efctv_dt,t2.elgbl_adr_efctv_dt) as ELGBL_ADR_EFCTV_DT,
                coalesce(t1.elgbl_adr_end_dt,t2.elgbl_adr_end_dt) as ELGBL_ADR_END_DT,
                coalesce(t1.elgbl_adr_type_cd,t2.elgbl_adr_type_cd) as ELGBL_ADR_TYPE_CD,

                t1.elgbl_line_1_adr_home,
                t1.elgbl_line_2_adr_home,
                t1.elgbl_line_3_adr_home,
                t1.elgbl_city_name_home,
                t1.elgbl_zip_cd_home,
                t1.elgbl_cnty_cd_home,
                t1.elgbl_state_cd_home,
                t1.elgbl_phne_num_home,

                t2.elgbl_line_1_adr_mail,
                t2.elgbl_line_2_adr_mail,
                t2.elgbl_line_3_adr_mail,
                t2.elgbl_city_name_mail,
                t2.elgbl_zip_cd_mail,
                t2.elgbl_cnty_cd_mail,
                t2.elgbl_state_cd_mail,
                t2.elgbl_phne_num_mail

                from {self.tab_no}_uniq_step1 t1
                full join {self.tab_no}_uniq_step2 t2
                on t1.msis_ident_num=t2.msis_ident_num
                and t1.submtg_state_cd=t2.submtg_state_cd
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

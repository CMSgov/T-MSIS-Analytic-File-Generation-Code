from taf.BSF import BSF_Runner



from taf.BSF.ELG import ELG


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class ELG00005(ELG):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00005', 'TMSIS_ELGBLTY_DTRMNT', 'ELGBLTY_DTRMNT_EFCTV_DT', 'ELGBLTY_DTRMNT_END_DT')

    # ---------------------------------------------------------------------------------
    #
    #  1  Full Dual
    #  2  Partial Dual
    #  3  Other Dual
    #  4  Non-Dual
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        ELGBLTY_GRP_CODE = """
            case when length(trim(ELGBLTY_GRP_CD))=1 and ELGBLTY_GRP_CD <> '.'
            then lpad(trim(ELGBLTY_GRP_CD),2,'0')
            else trim(ELGBLTY_GRP_CD) end """

        DUAL_ELGBL_CODE = """
            case when length(trim(DUAL_ELGBL_CD))=1 and DUAL_ELGBL_CD <> '.'
            then lpad(trim(DUAL_ELGBL_CD),2,'0')
            else trim(DUAL_ELGBL_CD) end """

        created_vars = f"""
            {ELGBLTY_GRP_CODE} as ELGBLTY_GRP_CODE,
            {DUAL_ELGBL_CODE} as DUAL_ELGBL_CODE,

            case when LENGTH(trim(CARE_LVL_STUS_CD))<3 and CARE_LVL_STUS_CD <> '.' then lpad(CARE_LVL_STUS_CD,3,'00')
                else CARE_LVL_STUS_CD end as CARE_LVL_STUS_CODE,

            case
                when ({DUAL_ELGBL_CODE}) in ('02','04','08')      then 1
                when ({DUAL_ELGBL_CODE}) in ('01','03','05','06') then 2
                when ({DUAL_ELGBL_CODE}) in ('09','10')           then 3
                when ({DUAL_ELGBL_CODE}) in ('00')                then 4
                else null end as DUAL_ELIGIBLE_FLG,

            case
                when (({ELGBLTY_GRP_CODE}) between '01' and '09') or
                     (({ELGBLTY_GRP_CODE}) between '72' and '75') then 1
                when (({ELGBLTY_GRP_CODE}) between '11' and '19') or
                     (({ELGBLTY_GRP_CODE}) between '20' and '26') then 2
                when (({ELGBLTY_GRP_CODE}) between '27' and '29') or
                     (({ELGBLTY_GRP_CODE}) between '30' and '36') or
                     (({ELGBLTY_GRP_CODE}) = '76')                then 3
                when (({ELGBLTY_GRP_CODE}) between '37' and '39') or
                     (({ELGBLTY_GRP_CODE}) between '40' and '49') or
                     (({ELGBLTY_GRP_CODE}) between '50' and '52') then 4
                when (({ELGBLTY_GRP_CODE}) between '53' and '56') then 5
                when (({ELGBLTY_GRP_CODE}) in('59','60'))         then 6
                when (({ELGBLTY_GRP_CODE}) in('61','62','63')) then 7
                when (({ELGBLTY_GRP_CODE}) in('64','65','66')) then 8
                when (({ELGBLTY_GRP_CODE}) in('67','68')) then 9
                when (({ELGBLTY_GRP_CODE}) in('69','70','71'))    then 10
                else null end as ELIGIBILITY_GROUP_CATEGORY_FLG,

                case when MAS_CD = '.' or ELGBLTY_MDCD_BASIS_CD='.' then '.'
                    else (MAS_CD || ELGBLTY_MDCD_BASIS_CD) end as MASBOE
        """

        #  Create temp table to determine which beneficiaries have multiple records
        z = f"""

            create or replace temporary view {self.tab_no}_recCt as

            select
                submtg_state_cd,
                msis_ident_num,
                count(TMSIS_RUN_ID) as recCt
                from {self.tab_no}
                where PRMRY_ELGBLTY_GRP_IND='1'
                group by submtg_state_cd, msis_ident_num
        """
        self.bsf.append(type(self).__name__, z)

        # title "Number of records per beneficiary in {self.tab_no}"
        # select * from connection to tmsis_passthrough
        #  ( select recCt,count(msis_ident_num) as beneficiaries from {self.tab_no}_recCt asgroup by recCt ) order by recCt

        #  Set aside table data for benes with only one record
        z = f"""
            create or replace temporary view {self.tab_no}_uniq as
            select t1.*,

                {created_vars},

                1 as KEEP_FLAG

                from {self.tab_no} t1
                inner join {self.tab_no}_recCt as t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num  = t2.msis_ident_num
                and t2.recCt=1
                where PRMRY_ELGBLTY_GRP_IND='1'
        """
        self.bsf.append(type(self).__name__, z)

        # title "Number of beneficiary with unique records in {self.tab_no}"
        # select * from connection to tmsis_passthrough
        #  ( select count(msis_ident_num) as beneficiaries from {self.tab_no}_uniq )

        sort_key = """
            coalesce(trim(MSIS_CASE_NUM),'x') || coalesce(trim(elgblty_mdcd_basis_cd),'x')  ||
            coalesce(trim(dual_elgbl_cd),'x')  || coalesce(trim(elgblty_grp_cd),'x')  ||
            coalesce(trim(care_lvl_stus_cd),'x')  ||coalesce( trim(ssdi_ind),'x')  || coalesce(trim(ssi_ind),'x')  ||
            coalesce(trim(ssi_state_splmt_stus_cd),'x') || coalesce(trim(ssi_stus_cd),'x')  ||
            coalesce(trim(state_spec_elgblty_fctr_txt),'x')  || coalesce(trim(birth_cncptn_ind),'x')  ||
            coalesce(trim(mas_cd),'x')  || coalesce(trim(rstrctd_bnfts_cd),'x')  ||
            coalesce(trim(tanf_cash_cd),'x')  || coalesce(trim(prmry_elgblty_grp_ind),'x')"""

        self.MultiIds(created_vars, sort_key, "PRMRY_ELGBLTY_GRP_IND='1'")

        #  Union together tables for a permanent table
        z = f"""
            create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq as

            select *

            from (
            select * from {self.tab_no}_uniq
            union all
            select * from {self.tab_no}_multi)
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

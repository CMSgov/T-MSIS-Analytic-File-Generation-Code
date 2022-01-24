import logging

from datetime import datetime


# -------------------------------------------------------------------------------------
#
#
#
#
# -------------------------------------------------------------------------------------
class BSF_Runner():

    PERFORMANCE = 11

    # ---------------------------------------------------------------------------------
    #
    #   reporting_period = SUBSTR(JOB_PARMS_TXT,1,10) AS RPTPD FORMAT=$10.
    #   e.g. '2020-01-31'
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, reporting_period: str):
        from datetime import date, datetime, timedelta

        self.now = datetime.now()
        self.version = '1.0.1'
        # self.initialize_logger(self.now)

        self.submtg_state_cd = '01'
        self.tmsis_run_id = 4289
        self.DA_RUN_ID = 5678

        self.reporting_period = datetime.strptime(reporting_period, '%Y-%m-%d')

        begmon = self.reporting_period
        begmon = date(begmon.year, begmon.month, 1)
        self.begmon = begmon.strftime('%Y-%m-%d').upper()
        self.st_dt = f'{self.begmon}'

        self.BSF_FILE_DATE = int(self.reporting_period.strftime('%Y%m'))
        self.TAF_FILE_DATE = self.BSF_FILE_DATE

        self.RPT_PRD = f'{self.reporting_period.strftime("%Y-%m-%d")}'
        # self.RPT_OUT = int(self.RPT_PRD)

        if self.now.month == 12:  # December
            last_day = date(self.now.year, self.now.month, 31)
        else:
            last_day = date(self.now.year, self.now.month + 1, 1) - timedelta(days=1)
        self.FILE_DT_END = last_day.strftime('%Y-%m-%d').upper()

        self.sql = []
        self.plan = {}

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def print(self):
        print('Version:\t' + self.version)
        print('-----------------------------------------------------')
        print('')
        print('-----------------------------------------------------')
        print('begmon:\t' + str(self.begmon))
        print('st_dt:\t' + self.st_dt)
        print('BSF_FILE_DATE:\t' + str(self.BSF_FILE_DATE))
        print('TAF_FILE_DATE:\t' + str(self.TAF_FILE_DATE))
        print('RPT_PRD:\t' + str(self.RPT_PRD))
        print('FILE_DT_END:\t' + str(self.FILE_DT_END))

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    @staticmethod
    def compress(string):
        return ' '.join(string.split())

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def log(self, viewname: str, sql=''):
        self.logger.info('\t' + viewname)
        if sql != '':
            self.logger.debug(BSF_Runner.compress(sql.replace('\n', '')))
            # self.sql[viewname] = '\n'.join(sql.split('\n')[2:])

    # --------------------------------------------------------------------
    #
    #
    #
    # --------------------------------------------------------------------
    def initialize_logger(self, now: datetime):

        file_date = now.strftime('%Y-%m-%d-%H-%M-%S')

        logging.addLevelName(BSF_Runner.PERFORMANCE, 'PERFORMANCE')

        def performance(self, message, *args, **kws):
            self.log(BSF_Runner.PERFORMANCE, message, *args, **kws)

        logging.Logger.performance = performance

        p_dir = '/tmp/'
        p_filename = 'custom_log_' + file_date + '.log'
        p_logfile = p_dir + p_filename

        self.logger = logging.getLogger('taf_log')
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(p_logfile, mode='a')
        ch = logging.StreamHandler()
        # ch.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        self.logfile = p_logfile
        self.logfilename = p_filename

        self.logger.debug('TAF log file: ' + p_logfile)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def append(self, segment: str, z: str):

        if segment not in self.plan.keys():
            self.plan[segment] = []

        # self.log(f"{self.tab_no}", z)
        self.plan[segment].append(z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def view_plan(self):

        for segment, chain in self.plan.items():
            for sql in chain:
                print(f"-- {segment}")
                print(sql)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def init(self):
        from taf.BSF.ELG00002 import ELG00002
        from taf.BSF.ELG00003 import ELG00003
        from taf.BSF.ELG00004 import ELG00004
        from taf.BSF.ELG00005 import ELG00005
        from taf.BSF.ELG00006 import ELG00006
        from taf.BSF.ELG00007 import ELG00007
        from taf.BSF.ELG00008 import ELG00008
        from taf.BSF.ELG00009 import ELG00009
        from taf.BSF.ELG00010 import ELG00010
        from taf.BSF.ELG00011 import ELG00011
        from taf.BSF.ELG00012 import ELG00012
        from taf.BSF.ELG00013 import ELG00013
        from taf.BSF.ELG00014 import ELG00014
        from taf.BSF.ELG00015 import ELG00015
        from taf.BSF.ELG00016 import ELG00016
        from taf.BSF.ELG00017 import ELG00017
        from taf.BSF.ELG00018 import ELG00018
        from taf.BSF.ELG00020 import ELG00020
        from taf.BSF.ELG00021 import ELG00021
        from taf.BSF.ELG00022 import ELG00022
        from taf.BSF.TPL00002 import TPL00002

        ELG00002(self).create()
        ELG00003(self).create()
        ELG00004(self).create()
        ELG00005(self).create()
        ELG00006(self).create()
        ELG00007(self).create()
        ELG00008(self).create()
        ELG00009(self).create()
        ELG00010(self).create()
        ELG00011(self).create()
        ELG00012(self).create()
        ELG00013(self).create()
        ELG00014(self).create()
        ELG00015(self).create()
        ELG00016(self).create()
        ELG00017(self).create()
        ELG00018(self).create()
        ELG00020(self).create()
        ELG00021(self).create()
        ELG00022(self).create()
        ELG00005(self).create()
        TPL00002(self).create()

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def run(self):
        print("run")

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
# <http://creativecommons.org/publicdomain/zero/1.0/>elg00005

import json
from operator import truediv
import uuid
import logging
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

import pyodbc
from bcpandas import SqlCreds, to_sql


from .settings import AppSettings
from .pat_flag import PAT_FLAG, DATA_SOURCE_TYPE, PERIL_SUBGROUP, PSOLD_BLENDING, VALIDATE_RULE, COVERAGE_TYPE, DEDDUCT_TYPE, RATING_TYPE

from .psold_rating import PsoldRating
from pat_back.fls_rating import FlsRating
from pat_back.mb_rating import MbRating

class PatJob:
    """Class to represent a PAT analysis"""

    peril_table = {1: "eqdet", 2: "hudet", 3: "todet",
                   4: "fldet", 5: "frdet", 6: "trdet"}
    job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    def __init__(self, job_id:int = 0, param:dict ={}):
        """Class to represent a PAT analysis"""

        self.job_id = 0
        self.data_extracted = False

        try:
            if job_id > 0 :
                with pyodbc.connect(self.job_conn) as conn:
                    df = pd.read_sql(
                        f"select data_extracted, parameters from pat_job where job_id = {job_id} and status='wait_to_start'", conn)
                    if len(df) == 1:
                        self.job_id = job_id
                        self.param = json.loads(df.parameters[0])
                        self.data_extracted = df.data_extracted[0] != 0
            elif param:
                self.job_id = 1
                self.param = param
                self.param['job_guid'] = str(uuid.uuid4())
                if 'valid_rules' not in self.param: self.param['valid_rules'] = -1 
                self.data_extracted = True

            if not self.job_id:
                return

            self.logger = logging.getLogger(f"{self.job_id}")
            self.update_status("started")
            self.logger.info(f"""Start to process job:{self.job_id}""")

            self.data_source_type = DATA_SOURCE_TYPE[self.param['data_source_type']]
            self.catdb = self.param['cat_data'] if 'cat_data' in self.param else None
            self.reference_job = self.param['reference_job'] if 'reference_job' in self.param else None

            self.rating_type = RATING_TYPE[self.param['type_of_rating']]
            self.curve_id = int(self.param['curve_id'])
            self.psold = self.param['psold'] if 'psold' in self.param else None
            self.fls = self.param['fls'] if 'fls' in self.param else None
            self.mb = self.param['mb'] if 'mb' in self.param else None

            self.coverage_type = COVERAGE_TYPE[self.param['coverage_type']]
            self.addt_cvg = float(self.param['additional_coverage'])
            self.ded_type = DEDDUCT_TYPE[self.param['deductible_treatment']]
            self.loss_ratio = float(self.param['loss_alae_ratio'])
            self.valid_rules = VALIDATE_RULE(
                self.param['valid_rules']) if 'valid_rules' in self.param else VALIDATE_RULE(0)

            self.user_name = self.param['user_name'] if 'user_name' in self.param else None
            self.user_email = self.param['user_email'] if 'user_email' in self.param else None

            # no blending weights
            if self.rating_type == RATING_TYPE.PSOLD and PSOLD_BLENDING[self.psold['blending_type']] != PSOLD_BLENDING.no_blending:
                b = np.array(self.psold['blending_weights'])
                if np.all( b <= 0):
                    self.psold['blending_type'] = PSOLD_BLENDING.no_blending.name
                                    
        except:
            self.job_id = 0
            logging.warning('Read job parameter error!')

    def update_status(self, st):
        if self.job_id < 100: return # those are quick jobs and not recored in database

        tm = ''
        if st == 'started':
            tm += f", start_time = '{datetime.utcnow().isoformat()}'"
        elif st == 'finished':
            tm += f", finish_time = '{datetime.utcnow().isoformat()}'"

        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = '{st.replace("'","''")}'
                {tm}           
                where job_id = {self.job_id}""")
            cur.commit()

    def __check_stop(self, stop_cb):
        if stop_cb and stop_cb():
            self.logger.warning("User cancelled the analysis")
            self.update_status('cancelled')
            return True

    def run(self, stop_cb=None):
        self.logger.info("Import data...")
        self.update_status("started")

        if not self.data_extracted:
            self.update_status("extracting_data")
            self.logger.info("Import data...")

            if self.data_source_type == DATA_SOURCE_TYPE.User_Upload:
                self.data_extracted = True
            elif self.data_source_type == DATA_SOURCE_TYPE.Reference_Job:
                self.data_extracted = self.__extract_ref_data(self.reference_job['job_id']) 
            elif self.data_source_type == DATA_SOURCE_TYPE.Cat_Data:
                self.data_extracted = self.__extract_edm_rdm()
            
            if self.data_extracted:
                with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
                    cur.execute(
                        f"""update pat_job set data_extracted = 1 where job_id = {self.job_id}""")
                    cur.commit()
                self.logger.info("Import data...OK")
            else:
                self.logger.info("Import data...Error")
                self.update_status("error")
                return
        if stop_cb and self.__check_stop(stop_cb):
            return

        self.update_status('checking_data')
        self.logger.debug("Check data...")
        self.__check_pseudo_policy()
        self.__check_facultative()
        self.__create_layers()
        self.logger.debug("Check data...OK")
        if stop_cb and self.__check_stop(stop_cb):
            return

        if self.__need_correction():
            if (self.valid_rules & VALIDATE_RULE.ValidContinue) == 0:
                self.logger.error("Need to correct data then run again")
                self.update_status("error")
                return
            else:
                self.logger.warning(
                    "Skip erroneous data and continue (item removed)")
        if stop_cb and self.__check_stop(stop_cb):
            return

        # start calculation
        self.update_status("net_of_fac")
        self.logger.info("Create the net of FAC layer stack ...")
        df_facnet = self.__net_of_fac()
        if len(df_facnet) <= 0:
            self.logger.warning("Nothing to allocate! Finished.")
            self.update_status("finished")
            return
        self.logger.info(
            f"Create the net of FAC layer stack...OK ({len(df_facnet)})")
        if stop_cb and self.__check_stop(stop_cb):
            return

        self.update_status("allocating")
        self.logger.info(f"Allocate premium with {self.rating_type.name}...")
        df_pat = self.allocate_premium(df_facnet)
        self.logger.info(f"Allocate premium with {self.rating_type.name}...OK")
        if stop_cb and self.__check_stop(stop_cb):
            return

        # save results
        self.update_status("upload_results")
        if df_pat is not None and len(df_pat) > 0:
            self.logger.info("Save results to database...")
            df_pat= df_pat[['PseudoPolicyID', 'PseudoLayerID', 'Limit', 'Retention', 'Participation', 
                'RatingGroup', 'LossRatio', 'Premium', 'PolLAS', 'DedLAS']].sort_values([
                    'PseudoPolicyID', 'PseudoLayerID', 'Retention'])
            df_pat.fillna(value=0, inplace=True)

            df_pat['job_id'] = self.job_id
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB,
                             AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_pat, "pat_premium", creds,
                   index=False, if_exists='append')
            self.logger.info("Save results to database...OK")

        self.update_status("finished")
        self.logger.info("Finished!")

    def __extract_ref_data(self, ref_job):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            for t in ['pat_pseudo_policy', 'pat_facultative']:
                cur.execute(
                    f"""delete from {t} where job_id = {self.job_id} and data_type in (0, 1)""")
                cur.commit()

            cur.execute(f"""insert into pat_pseudo_policy
                    select {self.job_id} as job_id, 1 as data_type,
                        PseudoPolicyID, ACCGRPID, OriginalPolicyID, PolRetainedLimit, PolLimit, 
                        PolParticipation, PolRetention, PolPremium, LocationIDStack, 
                        occupancy_scheme, occupancy_code, Building, Contents, BI, AOI, 
                        null RatingGroup, LossRatio, 
                        0 as flag
                    from pat_pseudo_policy 
                    where job_id = {ref_job} and data_type = 1""")
            cur.commit()

            cur.execute(f"""insert into pat_facultative
                        select {self.job_id} as job_id, 1 as data_type,
                        PseudoPolicyID, FacLimit, FacAttachment, FacCeded, FacKey,
                        0 as flag
                    from pat_facultative 
                    where job_id = {ref_job} and data_type = 1""")
            cur.commit()

            return True

    def __extract_edm_rdm(self):
        #clean
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            for t in ['pat_pseudo_policy', 'pat_facultative']:
                cur.execute(
                    f"""delete from {t} where job_id = {self.job_id} and data_type in (0, 1)""")
                cur.commit()

        conn_str = f'''DRIVER={{SQL Server}};Server={self.catdb["server"]};Database={self.catdb['edm']};
            Trusted_Connection=True;MultipleActiveResultSets=true;'''
        with pyodbc.connect(conn_str) as conn:
            self.logger.debug("Verify input data base info...")
            if self.__verify_edm_rdm(conn) != 'ok': return
            self.logger.debug("Verify input data base info...OK")

            suffix = str(uuid.uuid4())[0:8]
            self.logger.debug(f"Temptable suffix: {self.job_id} , {suffix}")

            self.logger.debug("Create temp tables...")
            self.__create_temp_tables(conn, suffix)
            self.logger.debug("Create temp tables...OK")

            self.logger.debug("Extract policy location table...")
            n = self.__extract_pseudo_policy(conn, suffix)
            if n <= 0:
                return False
            self.logger.debug("Extract policy location table...OK ({n})")

            self.logger.debug("Extract fac table...")
            n = self.__extract_facultative(conn, suffix)
            self.logger.debug(f"Extract fac table...OK ({n})")

            with conn.cursor() as cur:
                cur.execute(f"drop table #sqlpremalloc_{suffix}")
                cur.commit()

        return True

    def __verify_edm_rdm(self, conn):
        # Check that PerilID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.catdb['edm']}]..policy a
                    join [{self.catdb['edm']}]..portacct b on a.accgrpid = b.accgrpid
                where b.portinfoid = {self.catdb['portinfoid']} and policytype = {self.catdb['perilid']}""", conn)) <= 0:
            return 'PerilID is not valid!'

        # Check that portfolio ID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.catdb['edm']}]..loccvg 
                    join [{self.catdb['edm']}]..property on loccvg.locid = property.locid 
                    join [{self.catdb['edm']}]..portacct on property.accgrpid = portacct.accgrpid 
                where loccvg.peril = {self.catdb['perilid']} and portacct.portinfoid = {self.catdb['portinfoid']}""", conn)) <= 0:
            return 'portfolio ID is not valid!'

        # Check that analysis ID is valid
        if 'rdm' in self.catdb and self.catdb['rdm'] and 'analysisid' in self.catdb and self.catdb['analysisid']:
            if len(pd.read_sql_query(f"""select top 1 1
            FROM [{self.catdb['edm']}]..loccvg 
                join [{self.catdb['edm']}]..property on loccvg.locid = property.locid 
                join [{self.catdb['edm']}]..portacct on property.accgrpid = portacct.accgrpid 
                join [{self.catdb['edm']}]..policy on property.accgrpid = policy.accgrpid 
                join [{self.catdb['rdm']}]..rdm_policy on policy.policyid = rdm_policy.id 
            where portacct.portinfoid = {self.catdb['portinfoid']} and loccvg.peril = {self.catdb['perilid']}
            and rdm_policy.ANLSID = {self.catdb['analysisid']}""", conn)) <= 0:
                print('portfolio ID is not valid!')

        return 'ok'

    def __create_temp_tables(self, conn, suffix: str):
        with conn.cursor() as cur:
            # location_standard
            cur.execute(f"""select p.accgrpid, p.locid, sum(loccvg.valueamt) as tiv, 
                    sum(case when losstype = 1 then valueamt else 0 end) as bldgval,
                    sum(case when losstype = 2 then valueamt else 0 end) as contval,
                    sum(case when losstype = 3 then valueamt else 0 end) as bival,
                    max(p.occscheme) as occupancy_scheme, 
                    max(p.occtype) as occupancy_code
                    into #location_standard_{suffix}
                from portacct as pa 
                    join property as p on pa.accgrpid = p.accgrpid
                    join loccvg on p.locid = loccvg.locid
                where pa.portinfoid = {self.catdb['portinfoid']} and loccvg.peril = {self.catdb['perilid']}
                group by p.accgrpid, p.locid""")
            cur.commit()

            # policy_standard
            cur.execute(f"""select p.accgrpid, policyid, policynum,
                    blanlimamt as origblanlimamt, partof as origpartof, accttiv,
                    case when blanlimamt <= 0 then accttiv
                        when blanlimamt > 1 then blanlimamt 
                        else blanlimamt * (case when partof > 1 then partof else accttiv end)			
                    end as blanlimamt,																	

                    case when partof > 1 then partof
                        when blanlimamt > 1 then blanlimamt/(case when partof > 0 then partof else 1 end)
                        else accttiv
                    end as partof,

                    case when blanlimamt > 0 and blanlimamt <= 1 then blanlimamt
                        when partof > 0 and partof <= 1 then partof
                        when blanlimamt > 1 then 
                            (case when partof >= blanlimamt then blanlimamt / partof else 1 end)
                        else 1 end as polparticipation,
                    p.undcovamt,
                    case when p.blandedamt > p.mindedamt then p.blandedamt else p.mindedamt end as polded, 
                    blanpreamt
                    into #policy_standard_{suffix}
                from portacct as pa 
                    join policy as p on pa.accgrpid = p.accgrpid 
                    join (
                            select accgrpid, sum(tiv) as accttiv 
                            from #location_standard_{suffix} 
                            group by accgrpid
                        ) accttiv on p.accgrpid = accttiv.accgrpid
                    where p.policytype = {self.catdb['perilid']}
                        and pa.portinfoid = {self.catdb['portinfoid']}""")
            cur.commit()

            # location policy
            if 'rdm' in self.catdb and self.catdb['rdm'] and 'analysisid' in self.catdb and self.catdb['analysisid']:
                # with spider
                # policy_loc_conditions
                cur.execute(f"""select p.accgrpid, p.policyid, lc.locid, pc.conditiontype
                        into #policy_loc_conditions_{suffix} 
                    from #policy_standard_{suffix} as p 
                        join policyconditions as pc on p.policyid = pc.policyid 
                        join locconditions as lc on pc.conditionid = lc.conditionid
                    where conditiontype = 2 and included in (1,2) 
                        and (pc.limit < p.partof + p.undcovamt or pc.deductible > 0)
                    group by p.accgrpid, p.policyid, lc.locid, pc.conditiontype""")
                cur.commit()

                # additional policy_loc_conditions
                cur.execute(f"""with incl_locs as 
                    (select p.accgrpid, p.policyid,lc.locid, pc.conditiontype
                    from #policy_standard_{suffix} as p 
                        join policyconditions as pc on p.policyid = pc.policyid 
                        join locconditions as lc on pc.conditionid = lc.conditionid
                    where conditiontype = 1 and included in (1,2)
                    group by p.accgrpid, p.policyid,lc.locid, pc.conditiontype),

                    all_locs as 
                    (select polexcl.accgrpid, polexcl.policyid, alllocs.locid
                    from (select accgrpid, policyid
                        from incl_locs
                        group by accgrpid, policyid) as polexcl 
                            join #location_standard_{suffix} as alllocs 
                                on polexcl.accgrpid = alllocs.accgrpid 
                        where alllocs.tiv > 0)
                    insert into #policy_loc_conditions_{suffix} 
                    select all_locs.accgrpid, all_locs.policyid, all_locs.locid, 1 as conditiontype
                    from all_locs 
                        join policy as p on all_locs.policyid = p.policyid 
                        left join incl_locs on all_locs.policyid = incl_locs.policyid and all_locs.locid = incl_locs.locid
                    where incl_locs.locid is null""")
                cur.commit()

                # sqlpremalloc
                cur.execute(f"""with locpoltotals as (
                    select a.id as policyid, res1value as locid,
                        sum(case when perspcode = 'GU' then perspvalue else 0  end) as grounduploss,
                        sum(case when perspcode = 'CL' then perspvalue else 0  end) as clientloss,
                        sum(case when perspcode = 'UC' then perspvalue else 0  end) as undcovloss,
                        sum(case when perspcode = 'OL' then perspvalue else 0  end) as overlimitloss,
                        sum(case when perspcode = 'OI' then perspvalue else 0  end) as otherinsurerloss,
                        sum(case when perspcode = 'GR' then perspvalue else 0  end) as grossloss
                        --sum(case when perspcode = 'SS' then perspvalue else 0  end) as surplusshareloss,
                        --sum(case when perspcode = 'FA' then perspvalue else 0  end) as facloss,
                        --sum(case when perspcode = 'RL' then perspvalue else 0  end) as netprecatloss
                    from {self.catdb['rdm']}..rdm_policy a 
                        join {self.catdb['rdm']}..rdm_eventareadetails b on a.eventid = b.eventid 
                        join #policy_standard_{suffix} as p on a.id = p.policyid
                    where anlsid = {self.catdb['analysisid']}
                    group by a.id, res1value
                    having sum(case when perspcode = 'GU' then perspvalue else 0 end) * 
                        {self.addt_cvg + 1} > max(p.undcovamt) )

                    select l.locid, p.accgrpid, p.policyid, p.policynum,
                        p.blanlimamt as orig_blanlim,
                        p.partof as orig_partof,
                        p.undcovamt as orig_undcovamt,
                        case when p.blanlimamt <=0 then 0 else p.blanlimamt / p.partof end as orig_participation,
                        grounduploss, clientloss, undcovloss, overlimitloss, otherinsurerloss, grossloss,
                        case when grossloss + otherinsurerloss <= 0 then p.blanlimamt 
                            when overlimitloss > 1 then (p.blanlimamt / p.partof) * (grossloss + otherinsurerloss) 
                            else p.blanlimamt end as rev_blanlimamt,
                        case when grossloss + otherinsurerloss <= 0 then p.partof 
                            when overlimitloss > 1 then grossloss + otherinsurerloss 
                            else p.partof end as rev_partof,	
                        case when p.partof <= 0 then 1 else p.blanlimamt / p.partof end as participation,
                        clientloss as deductible,
                        case when cond.conditiontype = 2 then undcovloss else p.undcovamt end as undcovamt, 
                        grounduploss as origtiv,
                        case when grossloss + otherinsurerloss = 0 then clientloss + undcovloss else grounduploss end as rev_tiv,
                        case when grounduploss < l.tiv then grounduploss * (bldgval/l.tiv) else bldgval end as bldgval,
                        case when grounduploss < l.tiv then grounduploss * (contval/l.tiv) else contval end as contval,
                        case when grounduploss < l.tiv then grounduploss * (bival/l.tiv) else bival end as bival,
                        l.occupancy_scheme, l.occupancy_code, blanpreamt
                        into #sqlpremalloc_{suffix}
                    from #policy_standard_{suffix} p
                        join #location_standard_{suffix} as l on p.accgrpid = l.accgrpid
                        left join locpoltotals sp on sp.locid = l.locid and sp.policyid = p.policyid
                        left join #policy_loc_conditions_{suffix} as cond on p.policyid = cond.policyid 
                            and l.locid = cond.locid and cond.conditiontype != 1
                    order by p.accgrpid, sp.locid, sp.undcovloss;
                    
                    drop table #policy_standard_{suffix};
                    drop table #location_standard_{suffix};
                    drop table #policy_loc_conditions_{suffix}""")
                cur.commit()

            else:  # without spider
                cur.execute(f"""select l.locid, p.accgrpid, 
                        p.policyid, p.policynum, p.blanlimamt as orig_blanlimamt, 
                        p.partof as orig_partof, p.undcovamt as orig_undcovamt,
                        p.polparticipation as orig_participation, 
                        l.tiv as grounduploss, p.polded as clientloss, p.undcovamt as undcovloss,
                        case when l.tiv < p.undcovamt + p.partof + p.polded then 0
                            else l.tiv - (p.undcovamt + p.partof + p.polded) end as overlimitloss,
                        case when l.tiv < p.undcovamt + p.polded then 0
                            when l.tiv < p.undcovamt + p.partof + p.polded and l.tiv > p.undcovamt + p.polded 
                                then (l.tiv - (p.undcovamt + p.polded)) * (1-p.polparticipation)
                            else p.partof * (1 - p.polparticipation) end as otherinsurerloss,
                        case when l.tiv < p.undcovamt + p.polded then 0 
                            when l.tiv < p.undcovamt + p.partof + p.polded and l.tiv > p.undcovamt + p.polded 
                                then (l.tiv - (p.undcovamt + p.polded)) * (p.polparticipation)
                            else p.blanlimamt end as grossloss,
                        case when l.tiv * {self.addt_cvg + 1} <= p.undcovamt then null else p.blanlimamt end as rev_blanlimamt, 
                        case when l.tiv * {self.addt_cvg + 1} <= p.undcovamt then null else p.partof end as rev_partof, 
                        p.polparticipation as participation, p.polded as deductible, p.undcovamt as undcovamt,
                        l.tiv as origtiv, l.tiv as rev_tiv, bldgval, contval, bival, 
                        l.occupancy_scheme, l.occupancy_code,p.blanpreamt
                        into #sqlpremalloc_{suffix}
                        from #policy_standard_{suffix} as p
                            join #location_standard_{suffix} as l on p.accgrpid = l.accgrpid;
                        --where l.tiv * {self.addt_cvg + 1} > p.undcovamt;
                        
                        drop table #policy_standard_{suffix};
                        drop table #location_standard_{suffix};""")
                cur.commit()

            cur.execute(f"""update #sqlpremalloc_{suffix}
                set bldgval = rev_tiv * (bldgval / origtiv), 
                    contval = rev_tiv * (contval / origtiv), 
                    bival = rev_tiv * (bival / origtiv)
                where origtiv > rev_tiv + 1""")
            cur.commit()

    def __extract_pseudo_policy(self, conn, suffix: str):
        retained_lmt = "(rev_partof - deductible) * rev_blanlimamt / rev_partof" if self.ded_type == DEDDUCT_TYPE.Erodes_Limit else "rev_blanlimamt"
        gross_lmt = "rev_partof - deductible" if self.ded_type == DEDDUCT_TYPE.Erodes_Limit else "rev_partof"
        df_polloc = pd.read_sql_query(f"""select policyid as OriginalPolicyID, ACCGRPID,
                    concat(policyid, '_', locid) as PseudoPolicyID, 
                    {retained_lmt} as PolRetainedLimit, 
                    round({gross_lmt}, 2) as PolLimit, 
                    participation as PolParticipation, 
                    round(deductible + undcovamt, 2) as PolRetention, 
                    blanpreamt as PolPremium,
                    locid as LocationIDStack,
                    bldgval as Building, contval as Contents, bival as BI,
                    occupancy_scheme, occupancy_code 
                from #sqlpremalloc_{suffix}""", conn)

        # Save to DB
        if len(df_polloc) > 0:
            df_polloc['job_id'] = self.job_id
            df_polloc['data_type'] = int(1)
            df_polloc['flag'] = int(0)
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB,
                             AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_polloc, "pat_pseudo_policy", creds,
                   index=False, if_exists='append')

        return len(df_polloc)

    def __extract_facultative(self, conn, suffix: str):
        df_fac = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    round(case when excessamt + layeramt > rev_blanlimamt then rev_blanlimamt - excessamt 
                        else layeramt end, 2) as FacLimit, 
                    excessamt as FacAttachment, pcntreins/100 as FacCeded
                from #sqlpremalloc_{suffix} as locpol 
                    join reinsinf on locpol.policyid = reinsinf.exposureid 
                where reinsinf.exposrtype = 'pol' and excessamt < rev_blanlimamt 
                    and layeramt > 0 and pcntreins > 0 
                order by locpol.accgrpid, locpol.locid, locpol.undcovamt, reinsinf.excessamt, FacLimit, pcntreins""", conn)

        # Save to DB
        if len(df_fac) > 0:
            # keep the original key for identifier
            df_fac['FacKey'] = np.arange(1, len(df_fac) + 1, dtype=int)

            df_fac['job_id'] = self.job_id
            df_fac['data_type'] = int(1)
            df_fac['flag'] = int(0)
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB,
                             AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_fac, "pat_facultative", creds,
                   index=False, if_exists='append')

        return len(df_fac)

    def __check_pseudo_policy(self):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""delete from pat_pseudo_policy where job_id = {self.job_id} and data_type = 0""")
            cur.commit()

            cur.execute(
                f"""select min(PSOLD_RG) as min_rg, max(PSOLD_RG) as max_rg from psold_mapping""")
            row = cur.fetchone()
            min_psold_rg, max_psold_rg = row

            aoi = []
            if self.coverage_type & COVERAGE_TYPE.Building:
                aoi.append("COALESCE(b.Building, a.Building)")
            if self.coverage_type & COVERAGE_TYPE.Contents:
                aoi.append("COALESCE(b.Contents, a.Contents)")
            if self.coverage_type & COVERAGE_TYPE.BI:
                aoi.append("COALESCE(b.BI, a.BI)")
            aoi = " + ".join(aoi)

            # rating group
            rg_tab = ''
            rg_col = ''
            rg_flg = ''
            if self.rating_type != RATING_TYPE.PSOLD:
                rg_col = "b.RatingGroup"
            else:
                rg_tab = """left join psold_mapping c 
                    on COALESCE(b.occupancy_scheme, a.occupancy_scheme) = c.OCCSCHEME 
                    and COALESCE(b.occupancy_code, a.occupancy_code) =  c.OCCTYPE
                    """
                rg_col = f"""case when COALESCE(b.RatingGroup, c.PSOLD_RG) is null 
                            or COALESCE(b.RatingGroup, c.PSOLD_RG) < {min_psold_rg} 
                            or COALESCE(b.RatingGroup, c.PSOLD_RG) > {max_psold_rg} then null
                            else COALESCE(b.RatingGroup, c.PSOLD_RG) end"""
                
                if PSOLD_BLENDING[self.psold['blending_type']] == PSOLD_BLENDING.no_blending:
                    rg_flg = f"""+ (case when COALESCE(b.RatingGroup, c.PSOLD_RG) is null 
                                or COALESCE(b.RatingGroup, c.PSOLD_RG) < {min_psold_rg} 
                                or COALESCE(b.RatingGroup, c.PSOLD_RG) > {max_psold_rg} 
                                then {PAT_FLAG.FlagLocRG} else 0 end)"""

            cur.execute(f"""insert into pat_pseudo_policy 
                            select a.job_id, 0 as data_type, 
                                a.PseudoPolicyID, 
                                COALESCE(b.ACCGRPID, a.ACCGRPID) as ACCGRPID,
                                COALESCE(b.OriginalPolicyID, a.OriginalPolicyID) as OriginalPolicyID,
                                COALESCE(b.PolRetainedLimit, a.PolRetainedLimit) as PolRetainedLimit,
                                COALESCE(b.PolLimit, a.PolLimit) as PolLimit,
                                (case when round(COALESCE(b.PolRetainedLimit, a.PolRetainedLimit)
                                        - COALESCE(b.PolLimit, a.PolLimit) 
                                        * COALESCE(b.PolParticipation, a.PolParticipation), 1) <= 2
                                        and COALESCE(b.PolLimit, a.PolLimit) > 0
                                    then COALESCE(b.PolRetainedLimit, a.PolRetainedLimit) / COALESCE(b.PolLimit, a.PolLimit)
                                    else COALESCE(b.PolParticipation, a.PolParticipation) end) as PolParticipation,
                                COALESCE(b.PolRetention, a.PolRetention) as PolRetention,
                                COALESCE(b.PolPremium, a.PolPremium) as PolPremium,
                                
                                COALESCE(b.LocationIDStack, a.LocationIDStack) as LocationIDStack,
                                COALESCE(b.occupancy_scheme, a.occupancy_scheme) as occupancy_scheme,
                                COALESCE(b.occupancy_code, a.occupancy_code) as occupancy_code,
                                COALESCE(b.Building, a.Building) as Building,
                                COALESCE(b.Contents, a.Contents) as Contents,
                                COALESCE(b.BI, a.BI) as BI,
                                COALESCE(b.AOI, {aoi}) as AOI,
                                {rg_col} as RatingGroup,
                                COALESCE(b.LossRatio, a.LossRatio) as LossRatio,
                                
                                (case when COALESCE(b.PolLimit, a.PolLimit) is null 
                                            or COALESCE(b.PolRetention, a.PolRetention) is null 
                                            or COALESCE(b.PolRetainedLimit, a.PolRetainedLimit) is null 
                                            or COALESCE(b.PolParticipation, a.PolParticipation) is null 
                                            or COALESCE(b.PolPremium, a.PolPremium) is null  
                                            or COALESCE(b.PolLimit, a.PolLimit) < 0 
                                            or COALESCE(b.PolRetention, a.PolRetention) < 0 
                                            or COALESCE(b.PolRetainedLimit, a.PolRetainedLimit) < 0 
                                            or COALESCE(b.PolParticipation, a.PolParticipation) < 0 
                                            or COALESCE(b.PolPremium, a.PolPremium) < 0 
                                        then {PAT_FLAG.FlagPolNA}
                                        else 0 end) 
                                    + (case when round(COALESCE(b.PolParticipation, a.PolParticipation), 2) > 1 then {PAT_FLAG.FlagPolParticipation}
                                        else 0 end)
                                    + (case when round(
                                            COALESCE(b.PolRetainedLimit, a.PolRetainedLimit) 
                                            - COALESCE(b.PolLimit, a.PolLimit) 
                                            * COALESCE(b.PolParticipation, a.PolParticipation), 1) > 2 
                                        then {PAT_FLAG.FlagPolLimitParticipation} else 0 end) 

                                    + (case when COALESCE(b.AOI, {aoi}) < 0 then {PAT_FLAG.FlagLocNA} else 0 end)
                                    {rg_flg}  as flag                                
                            from pat_pseudo_policy a 
                                left join (select * from pat_pseudo_policy where job_id = {self.job_id} and data_type = 2) b 
                                    on a.PseudoPolicyID = b.PseudoPolicyID
                                {rg_tab}
                            where a.job_id = {self.job_id} and a.data_type = 1;""")
            cur.commit()
            if cur.rowcount<=0: return

            # Apply rule
            if self.valid_rules & VALIDATE_RULE.ValidAoi:
                cur.execute(f"""select LocationIDStack, max(aoi) as max_aoi
                                into #tmp_f
                            from pat_pseudo_policy
                            where job_id = {self.job_id} and data_type = 0 and LocationIDStack is not null
                            group by LocationIDStack
                            having count(distinct round(aoi, 0)) > 1;
                            update a set a.aoi = b.max_aoi
                            from pat_pseudo_policy a join #tmp_f b
                                on a.LocationIDStack = b.LocationIDStack
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""")
                cur.commit()

            # Policy Location record duplications
            cur.execute(f"""select PseudoPolicyID, {PAT_FLAG.FlagPolLocDupe} as flag
                                into #tmp_f
                            from pat_pseudo_policy 
                            where job_id = {self.job_id} and data_type = 0
                            group by PseudoPolicyID having count(*) > 1;
                            update a set a.flag = a.flag + b.flag
                            from pat_pseudo_policy a join #tmp_f b
                                on a.PseudoPolicyID = b.PseudoPolicyID
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""")
            cur.commit()

            # duplicate LocationIDStack
            cur.execute(f"""select LocationIDStack, {PAT_FLAG.FlagLocIDDupe} as flag
                                into #tmp_f
                            from pat_pseudo_policy
                            where job_id = {self.job_id} and data_type = 0 and LocationIDStack is not null
                            group by LocationIDStack
                            having count(distinct round(aoi,0)) > 1;
                            update a set a.flag = a.flag + b.flag
                            from pat_pseudo_policy a join #tmp_f b
                                on a.LocationIDStack = b.LocationIDStack
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""")
            cur.commit()

    def __check_facultative(self):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""delete from pat_facultative where job_id = {self.job_id} and data_type = 0""")
            cur.commit()

            cur.execute(f"""insert into pat_facultative 
                            select a.job_id, 0 as data_type, a.PseudoPolicyID,  
                                COALESCE(b.FacLimit, a.FacLimit) as FacLimit,
                                COALESCE(b.FacAttachment, a.FacAttachment) as FacAttachment,
                                COALESCE(b.FacCeded, a.FacCeded) as FacCeded,
                                a.FacKey,
                                (case when COALESCE(b.FacLimit, a.FacLimit) is null 
                                        or COALESCE(b.FacAttachment, a.FacAttachment) is null 
                                        or COALESCE(b.FacCeded, a.FacCeded) is null 
                                        or COALESCE(b.FacLimit, a.FacLimit) < 0
                                        or COALESCE(b.FacAttachment, a.FacAttachment) < 0 
                                        or COALESCE(b.FacCeded, a.FacCeded) < 0  
                                    then {PAT_FLAG.FlagFacNA} else 0 end) as flag                                
                            from pat_facultative a 
                                left join (select * from pat_facultative where job_id = {self.job_id} and data_type = 2) b 
                                    on a.PseudoPolicyID = b.PseudoPolicyID and a.FacKey = b.FacKey
                            where a.job_id = {self.job_id} and a.data_type = 1;""")
            cur.commit()
            if cur.rowcount<=0: return

           # Orphan Fac Records
            cur.execute(f"""update a set a.flag = a.flag | {PAT_FLAG.FlagFacOrphan}
                from pat_facultative a 
                    left join (select distinct PseudoPolicyID from pat_pseudo_policy where job_id = {self.job_id} and data_type = 0) b
                        on a.PseudoPolicyID = b.PseudoPolicyID
                where a.job_id = {self.job_id} and a.data_type = 0 and b.PseudoPolicyID is null""")
            cur.commit()

            # FacNet combined specific checks
            f = PAT_FLAG.FlagPolNA | PAT_FLAG.FlagFacNA
            cur.execute(f"""with cte as (
                    select a.PolParticipation, a.PolRetention,
                        PolLimit + PolRetention as PolTopLine,
                        (case when b.FacKey is null then 0 else b.FacLimit end) / PolParticipation as FacGupLimit,
                        case when b.FacKey is null then 0 else b.FacAttachment end as FacAttachment,
                        b.FacKey
                    from pat_pseudo_policy a 
                        left join pat_facultative b on a.job_id = b.job_id and a.data_type = b.data_type
                            and a.PseudoPolicyID = b.PseudoPolicyID and (b.flag & {f.value}) = 0
                    where a.job_id = {self.job_id} and a.data_type = 0 and (a.flag & {f.value}) = 0 
                ),
                cte1 as (
                    select FacKey, PolTopLine,
                        FacAttachment / PolParticipation + PolRetention as FacGupAttachment,
                        FacGupLimit + FacAttachment / PolParticipation + PolRetention as FacGupTopLine
                    from cte
                )
                select distinct FacKey 
                    into #facover
                from cte1 
                where FacGupAttachment - PolTopLine > 1 or FacGupTopLine - PolTopLine > 1;
                update a set a.flag = a.flag | {PAT_FLAG.FlagFacOverexposed}
                from pat_facultative a
                    join #facover b on a.FacKey = b.FacKey
                where job_id = {self.job_id} and data_type = 0;
                drop table #facover""")
            cur.commit()
    
    def __create_layers(self):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""delete from pat_layers where job_id = {self.job_id}""")
            cur.commit()
            cur.execute(f"""select distinct PseudoPolicyID into #pol1
                        from pat_pseudo_policy 
                        where job_id = {self.job_id} and data_type = 0 and flag = 0;
                        select distinct PseudoPolicyID into #pol2 from pat_facultative 
                        where job_id = {self.job_id} and data_type = 0 and flag != 0;
                    with good_p as (
                        select #pol1.PseudoPolicyID 
                        from #pol1 
                            left join #pol2 on #pol1.PseudoPolicyID = #pol2.PseudoPolicyID 
                        where #pol2.PseudoPolicyID is null
                    )
                    select OriginalPolicyID, a.PseudoPolicyID, PolRetainedLimit, PolLimit, 
                            case when PolParticipation > 1 then 1 else PolParticipation end as PolParticipation, 
                            PolRetention, PolPremium,
                            PolLimit + PolRetention as PolTopLine
                        into #dfPolUse
                        from pat_pseudo_policy a 
                            join good_p b on a.PseudoPolicyID = b.PseudoPolicyID 
                        where a.job_id = {self.job_id} and a.data_type = 0;
                        drop table #pol1;
                        drop table #pol2;""")
            cur.commit()
            if cur.rowcount<=0: return

            cur.execute(f"""select a.PseudoPolicyID, PolRetention,PolTopLine, FacCeded,
                        FacLimit / PolParticipation as FacGupLimit,
                        FacAttachment / PolParticipation + PolRetention as FacGupAttachment,
                        (FacLimit + FacAttachment) / PolParticipation + PolRetention as FacGupTopLine
                    into #dfPolFac
                    from #dfPolUse a 
                        join pat_facultative b on a.PseudoPolicyID = b.PseudoPolicyID 
                    where b.job_id = {self.job_id} and b.data_type = 0""")
            cur.commit()

            # Apply rule
            ceded = "sum(case when Ceded is null or Ceded < 0 then 0 else Ceded end)"
            if self.valid_rules & VALIDATE_RULE.ValidFac100:
                ceded = f"case when {ceded} > 1e-6 then 1 else {ceded} end"

            cur.execute(f"""with cte1 as 
                        (select PseudoPolicyID,PolRetention as LayerLow from #dfPolUse
                        union
                        select PseudoPolicyID, PolTopLine as LayerLow from #dfPolUse
                        union
                        select PseudoPolicyID, FacGupAttachment as LayerLow from #dfPolFac
                        union
                        select PseudoPolicyID, FacGupTopLine as LayerLow from #dfPolFac
                        ),
                    cte2 as 
                        (select ROW_NUMBER() over (order by PseudoPolicyID, LayerLow) as LayerKey, PseudoPolicyID, LayerLow from cte1),
                    cte3 as
                        (select a.PseudoPolicyID, a.LayerKey, a.LayerLow, b.LayerLow as LayerHigh
                            from cte2 a 
                            join cte2 b on a.PseudoPolicyID = b.PseudoPolicyID and a.LayerKey = b.LayerKey - 1
                            where b.LayerLow - a.LayerLow > 1),
                    cte4 as
                        (select a.*, b.PolParticipation as Participation, 
                            case when c.FacCeded is null or c.FacCeded < 0 then 0 else c.FacCeded end as Ceded
                        from cte3 a
                            Left Join #dfPolUse b ON a.PseudoPolicyID = b.PseudoPolicyID
                            Left Join #dfPolFac c ON a.PseudoPolicyID = c.PseudoPolicyID 
                                and a.LayerLow >= c.FacGupAttachment and a.LayerHigh <= FacGupTopLine
                    )
                    insert into pat_layers
                    select {self.job_id} as job_id, PseudoPolicyID,
                        row_number() OVER (PARTITION BY PseudoPolicyID ORDER BY LayerLow) as LayerID,
                        LayerLow,LayerHigh, 
                        {ceded} as Ceded
                    from cte4
                    group by PseudoPolicyID, LayerLow, LayerHigh;
                    drop table #dfPolUse;
                    drop table #dfPolFac;""")
            cur.commit()

            # Fac/Pol exceed 100%
            cur.execute(f"""update a set a.flag = a.flag | {PAT_FLAG.FlagCeded100}
                            from pat_facultative a 
                                join (select distinct PseudoPolicyID 
                                    from pat_layers where Ceded > 1
                                    ) b on a.PseudoPolicyID = b.PseudoPolicyID
                            where job_id = {self.job_id} and data_type = 0""")
            cur.commit()        

    def __need_correction(self):
        with pyodbc.connect(self.job_conn) as conn:
            for t in ['pat_pseudo_policy', 'pat_facultative']:
                df = pd.read_sql_query(f"""select count(*) as n from [{t}] where job_id = {self.job_id} 
                        and data_type = 0 and flag != 0""", conn)
                if df is not None and len(df) > 0 and df.n[0] > 0:
                    return True

        return False

    def __net_of_fac(self):
        with pyodbc.connect(self.job_conn) as conn:
            df_facnet = pd.read_sql_query(f"""select b.OriginalPolicyID as PolicyID, a.PseudoPolicyID, a.LayerID as PseudoLayerID,
                    a.LayerHigh - a.LayerLow as Limit, a.LayerLow as Retention, 
                    b.PolPremium as PolPrem, 
                    b.polParticipation * (1 - a.Ceded) as Participation,
                    b.AOI as TIV, b.LocationIDStack as Stack, b.RatingGroup, b.LossRatio
                from pat_layers a
                    join pat_pseudo_policy b on a.PseudoPolicyID = b.PseudoPolicyID 
                        and b.job_id ={self.job_id} and b.data_type = 0 
                where b.polParticipation > 0 and Ceded >= 0 and Ceded < 1 and AOI > 0 
                order by a.PseudoPolicyID, LayerID""", conn)

            return df_facnet

    def allocate_premium(self, DT) -> DataFrame:
        if self.rating_type == RATING_TYPE.PSOLD:
            DT = self.__calculate_las_psold(DT)
        elif self.rating_type == RATING_TYPE.FLS:
            DT = self.__calculate_las_fls(DT)
        elif self.rating_type == RATING_TYPE.MB:
            DT = self.__calculate_las_mb(DT)
        
        DT.fillna({'LossRatio':self.loss_ratio, 'Participation': 1},inplace=True)
        if 'PolLAS' in DT and 'DedLAS' in DT:
            DT['Premium'] = (DT.PolLAS-DT.DedLAS) * DT.LossRatio * DT.Participation
            sumLAS = DT.groupby('PolicyID').agg(
                {'Premium': 'sum'}).rename(columns={'Premium': 'sumLAS'})
            DT = DT.merge(sumLAS, on='PolicyID')
            DT['Premium'] *= DT['PolPrem'] / DT['sumLAS']

            return DT

    def __calculate_las_psold(self, DT):
        df_wts, df_hpr, def_rtg = None, None, None
        blend = PSOLD_BLENDING[self.psold['blending_type']]
        with pyodbc.connect(self.job_conn) as conn:
            aoi_split = pd.read_sql_query(
                f"""select * from psold_aoi order by AOI""", conn).AOI.to_numpy()
            df_psold = pd.read_sql_query(f"""select * from psold_curves  
                where ID = '{self.curve_id}' 
                    and CurveType ='{self.psold['curve_persp']}' 
                    and COVG = {COVERAGE_TYPE[self.psold['curve_coverage']] } 
                    and SUBGRP = {PERIL_SUBGROUP[self.psold['peril_subline']]}
                """, conn).drop(columns=['ID', 'CurveType', 'COVG', 'SUBGRP'])
            if df_psold is None: return

            if blend != PSOLD_BLENDING.no_blending:
                def_rtg = None
                b = np.array(self.psold['blending_weights'])
                if np.sum ( b > 0) == 1:
                    def_rtg = np.argmax( b > 0) + 1
                else: 
                    df_wts = pd.read_sql_query(f"""select RG, HPRTable from psold_weight order by rg""", conn)
                    df_wts['PremiumWeight'] = b

                    if self.psold['hpr_blending']:
                        df_hpr = pd.read_sql_query(f"""select Limit, Weight from psold_hpr_weight order by Limit""", conn)
                    else:
                        df_wts.drop(columns=['HPRTable'])
                
                if blend == PSOLD_BLENDING.all_blending:
                    DT['RatingGroup'] = np.nan

        psold = PsoldRating(self.curve_id, df_psold, aoi_split, **self.psold)
        avg_acc = datetime.strptime(self.param['psold']['average_accident_date'], '%m/%d/%Y') if 'average_accident_date' in self.param['psold'] else datetime.today()
        return psold.calculate_las(DT, def_rtg, df_wts, df_hpr,
                    blend_type = blend,
                    ded_type = self.ded_type.name,
                    avg_acc_date = avg_acc,
                    addt_cvg=self.addt_cvg)
    
    def __calculate_las_fls(self, DT):
        df_fls = None
        with pyodbc.connect(self.job_conn) as conn:
            df_fls = pd.read_sql_query(f"""select ID, mu, w, tau, theta, beta, cap, uTgammaMean, limMean
                from fls_curves order by ID""", conn).set_index('ID')
            if df_fls is None: return

        if self.fls and self.curve_id == FlsRating.user_defined:
            if self.curve_id in df_fls.index:
                df_fls.loc[self.curve_id] = df_fls.loc[self.curve_id].to_dict() | self.fls
            else:
                df_fls.loc[self.curve_id] = self.fls

        fls = FlsRating(self.curve_id, df_fls)
        return fls.calculate_las(DT, 
            ded_type = self.ded_type.name, 
            addt_cvg = self.addt_cvg)

    def __calculate_las_mb(self, DT):
        df_mb = None
        with pyodbc.connect(self.job_conn) as conn:
            df_mb = pd.read_sql_query(f"""select ID, b, g, cap from mb_curves 
                order by ID""", conn).set_index('ID')
            if df_mb is None: return

        if self.mb and self.curve_id == MbRating.user_defined:
            df_mb.loc[self.curve_id] = df_mb.loc[self.curve_id].to_dict() | self.mb

        mb = MbRating(self.curve_id, df_mb)
        return mb.calculate_las(DT, 
            ded_type = self.ded_type.name, 
            addt_cvg = self.addt_cvg, 
            custom_type = int(self.mb['custom_type']) if 'custom_type' in self.mb else 1)
        
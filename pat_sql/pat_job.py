
import os
import json
import logging
import threading
from datetime import datetime

import numpy as np
import pandas as pd

import pyodbc
from bcpandas import SqlCreds, to_sql

from common import AppSettings, PatFlag

class PatJob:
    """Class to repreet a PAT analysis"""

    peril_table = {1: "eqdet", 2: "hudet", 3: "todet",
                   4: "fldet", 5: "frdet", 6: "trdet"}
    job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    def __init__(self, job_para, data_file = None):
        self.para = job_para
        self.job_guid = self.para['job_guid']
        self.job_id =0 
        
        self.job_name = self.para['job_name']
        self.user_name = self.para['user_name'] if 'user_name' in job_para else None
        self.user_email = self.para['user_email'] if 'user_email' in job_para else None
        
        self.iCovg = 2 if self.para['coverage'] == "Building + Contents + Time Element" else 1
        self.iSubGrp = {
                "Fire":1,
                "Wind":2,
                "Special_Cause_of_Loss": 3,
                "All_Perils": 4
                }[self.para['peril_subline']] 
        self.AddtlCovg = float(self.para['additional_coverage'])
        self.dCurrencyAdj = 1.0
        self.iDedType = 1 if self.para['deductible_treatment'] == "Retains Limit" else 2
        self.dSubjPrem = float(self.para['subject_premium'])
        self.loss_ratio = float(self.para['loss_alae_ratio'])
        self.dtAveAccDate = datetime.strptime(self.para['average_accident_date'], '%m/%d/%Y')
        self.gdtPSTrendFrom = datetime(2015,12,31)
        self.gdPSTrendFactor = float(self.para['trend_factor'])
        self.gdReinsuranceLimit = 1000000 # global reinsurance limit
        self.gdReinsuranceRetention = 1000000 # global reinsurance retention

        #
        self.job_id = self.__register_job()
        if self.job_id > 0:
            self.__setup_logging(f"{self.job_id}")

    def __register_job(self):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            dt = datetime.utcnow().isoformat()
            n = cur.execute(f"""if not exists(select 1 from pat_job where job_guid = '{self.job_guid}')
                                begin 
                                insert into pat_job values (next value for pat_analysis_id_seq, '{self.job_guid}', '{self.job_name}',
                                    '{dt}', '{dt}', 'received',
                                    '{self.user_name}','{self.user_email}',
                                    '{json.dumps(self.para).replace("'", "''")}')
                                end""").rowcount
            cur.commit()
            
            if n==1:
                df = pd.read_sql_query(f"""select job_id from pat_job where job_guid = '{self.job_guid}'""", conn)
                if len(df)>0:
                    return df.job_id[0]
        return 0

    def __setup_logging(self, name):
        self.log_file = os.path.join(AppSettings.PAT_LOG, "pat.log")
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s [%(name)s] (%(levelname)s): %(message)s")

        oh = logging.StreamHandler()
        oh.setLevel(logging.INFO)
        oh.setFormatter(formatter)
        self.logger.addHandler(oh)

        try:
            fh = logging.FileHandler(self.log_file)
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)
        except:
            pass    

        self.logger.info(f"Start processing job: {self.job_guid}")
        self.logger.info(f"parameter: {json.dumps(self.para, sort_keys=True, indent=4)}")

    def __update_status(self, st):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = '{st.replace("'","''")}', 
                update_time = '{datetime.utcnow().isoformat()}'
                where job_id = {self.job_id}""").rowcount
            cur.commit()

    @classmethod
    def get_job_list(cls):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select job_id job_id, job_guid, job_name, receive_time, update_time, status, user_name, user_email
            from pat_job order by update_time desc, job_id desc""", conn)

            return df.to_json()

    @classmethod
    def get_job_para(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select parameters from pat_job where job_id = {job_id}""", conn)
            if df is not None and len(df) > 0:
                return df.parameters[0]

    @classmethod
    def get_summary(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select count(*) as cnt_policy from pat_policy where job_id = {job_id}""", conn)
            df['cnt_location'] = pd.read_sql_query(f"""select count(*) from pat_location where job_id = {job_id}""", conn)
            df['cnt_fac'] = pd.read_sql_query(f"""select count(*) from pat_facultative where job_id = {job_id}""", conn)
            return df.to_json()

    @classmethod
    def get_results(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select Limit, Retention, Premium, Participation, AOI, LocationIDStack, 
                                            RatingGroup, OriginalPolicyID, PseudoPolicyID, PseudoLayerID, PolLAS, DedLAS
                                       from pat_premium where job_id = {job_id}""", conn)
            return df.to_json()

    @classmethod
    def delete(cls, *job_lst):
        if len(job_lst) >0 :  
            lst= [f"{a}" for a in job_lst]
            with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
                for t in ['pat_job','pat_policy','pat_location', 'pat_facultative', 'pat_premium']:
                    cur.execute(f"""delete from [{t}] where job_id in ({','.join(lst)})""")
                
                cur.commit()
        
        return cls.get_job_list()
   
    @classmethod
    def get_validation_data(self, job_id):
        with pyodbc.connect(self.job_conn) as conn:
            df1 = pd.read_sql_query(f"""
                select * from pat_policy where job_id = {job_id} and flag != 0
                order by PseudoPolicyID""",conn)
            if len(df1) > 0:
                df = df1[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
                df1 = df1.merge(df, on ='flag')
            
            df2 = pd.read_sql_query(f"""
                select * from pat_location where job_id = {job_id} and flag != 0
                order by PseudoPolicyID""",conn)
            if len(df2) > 0:
                df = df2[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
                df2 = df2.merge(df, on ='flag')

            df3 = pd.read_sql_query(f"""
                select * from pat_facultative where job_id = {job_id} and flag != 0
                order by PseudoPolicyID""",conn)
            if len(df3) > 0:
                df = df3[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
                df3 = df3.merge(df, on ='flag')

        return (df1, df2, df3)

    # deamon  
    def process_job_async(self):
        d = threading.Thread(name='pat-daemon', target=self.perform_analysis)
        d.setDaemon(True)
        d.start()

        return True
    
    def perform_analysis(self):
        self.logger.info("Import data...")
        self.__extract_edm_rdm()
        self.logger.info("Import data...OK")
        self.__update_status("data_extracted")

        if self.__need_correction():
            if 'error_action' in self.para and self.para['error_action'] == 'stop':
                self.logger.error("Need to correct data tehn run again")
                return
            else:
                self.logger.warning("Skip erroneous data and continue")
        
        # start calculation
        self.logger.info("create the net of FAC layer stack ...")
        df_facnet = self.__net_of_fac()
        self.logger.info("create the net of FAC layer stack...OK")
        self.__update_status("net_of_fac")

        self.logger.info("Allocate premium with PSOLD...")
        df_pat = self.__allocate_with_psold(df_facnet)
        self.logger.info("Allocate premium with PSOLD...OK")
        self.__update_status("allocated")

        # save results
        if df_pat is not None and len(df_pat) > 0:
            self.logger.info("Save results to database...")
            df_pat.fillna(value=0, inplace=True)
            df_pat['job_id'] = self.job_id
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_pat, "pat_premium", creds, index=False, if_exists='append')
            self.logger.info("Save results to database...OK")
        
        self.__update_status("finished")
        self.logger.info("Finished!")

    def __extract_edm_rdm(self):
        conn_str = f'''DRIVER={{SQL Server}};Server={self.para["server"]};Database={self.para["edm_database"]};
            Trusted_Connection=True;MultipleActiveResultSets=true;'''
        with pyodbc.connect(conn_str) as conn:
            self.logger.debug("Verify input data base info...")
            if self.__verify_edm_rdm(conn) != 'ok': return
            self.logger.debug("Verify input data base info...OK")

            self.logger.debug("Create temp tables...")
            self.__create_temp_tables(conn)
            self.logger.debug("Create temp tables...OK")
            
            self.logger.debug("Extract policy table...")
            self.__extract_policy(conn)
            self.logger.debug("Extract policy table...OK")
            
            self.logger.debug("Extract location table...")
            self.__extract_location(conn)
            self.logger.debug("Extract location table...OK")
            
            self.logger.debug("Extract fac table...")
            self.__extract_fac(conn)
            self.logger.debug("Extract fac table...OK")

            self.__delete_temp_tables(conn)

            self.logger.debug("Some extra data checking...")
            self.__extra_data_check()
            self.logger.debug("Some extra data checking...OK")

    def __verify_edm_rdm(self, conn):
        # Check that PerilID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.para['edm_database']}]..policy a
                    join [{self.para['edm_database']}]..portacct b on a.accgrpid = b.accgrpid
                where b.portinfoid = {self.para['portinfoid']} and policytype = {self.para['perilid']}""", conn)) <= 0:
            return 'PerilID is not valid!'

        # Check that portfolio ID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.para['edm_database']}]..loccvg 
                    inner join [{self.para['edm_database']}]..property on loccvg.locid = property.locid 
                    inner join [{self.para['edm_database']}]..portacct on property.accgrpid = portacct.accgrpid 
                where loccvg.peril = {self.para['perilid']} and portacct.portinfoid = {self.para['portinfoid']}""", conn)) <= 0:
            return 'portfolio ID is not valid!'

        # Check that analysis ID is valid
        if 'rdm_database' in self.para and 'analysisid' in self.para:
            if len(pd.read_sql_query(f"""select top 1 1
            FROM [{self.para['edm_database']}]..loccvg 
                inner join [{self.para['edm_database']}]..property on loccvg.locid = property.locid 
                inner join [{self.para['edm_database']}]..portacct on property.accgrpid = portacct.accgrpid 
                inner join [{self.para['edm_database']}]..policy on property.accgrpid = policy.accgrpid 
                inner join [{self.para['rdm_database']}]..rdm_policy on policy.policyid = rdm_policy.id 
            where portacct.portinfoid = {self.para['portinfoid']} and loccvg.peril = {self.para['perilid']}
            and rdm_policy.ANLSID = {self.para['analysisid']}""", conn)) <= 0:
                print('portfolio ID is not valid!')

        return 'ok'
    
    def __create_temp_tables(self, conn):
        with conn.cursor() as cur:
            # policy_standard
            cur.execute(f"""select p.accgrpid, policyid, policynum, policytype,
                    blanlimamt as origblanlimamt, partof as origpartof, accttiv,
                    case when blanlimamt > 1 then blanlimamt 
                        when blanlimamt > 0 and blanlimamt <= 1 and partof >= 1 then blanlimamt*partof
                        when blanlimamt > 0 and blanlimamt <= 1 and partof = 0 then blanlimamt * accttiv
                        when blanlimamt = 0 then accttiv
                    end as blanlimamt, 

                    case when partof > 1 then partof
                        when partof > 0 and partof <= 1 and blanlimamt >= 1 then blanlimamt/partof
                        when partof = 0 and blanlimamt <= 1 then accttiv
                        when partof = 0 and blanlimamt > 1 then blanlimamt
                        when blanlimamt = 0 then accttiv
                    end as partof,

                    case when blanlimamt = 0 and partof > 0 and partof <= 1 then partof
                        when blanlimamt = 0 then 1
                        when blanlimamt > 0 and blanlimamt <= 1 then blanlimamt
                        else (case 
                            when (partof > 0 and partof <= 1) then partof
                            when (partof > 1 and blanlimamt > 1 and partof >= blanlimamt) then (blanlimamt/partof)
                            when (partof > 1 and blanlimamt > 1 and partof < blanlimamt) then 1
                            when partof = 0 then 1
                            else partof 
                        end) 
                    end as polparticipation,
                    p.undcovamt,
                    case when p.blandedamt > p.mindedamt then p.blandedamt else p.mindedamt end as polded, blanpreamt
                    into #policy_standard
                from portacct as pa 
                    inner join policy as p on pa.accgrpid = p.accgrpid 
                    inner join
                        (select accgrpid, sum(tiv) as accttiv,
                            sum(case when sitelimamt > 0 and sitelimamt < tiv then sitelimamt else tiv end) as acctsitelim
                        from
                            (select property.accgrpid, property.locid, sum(loccvg.valueamt) as tiv, sitelimamt
                            from property
                                inner join loccvg on property.locid = loccvg.locid
                                left outer join {self.peril_table[self.para['perilid']]} as det on property.locid = det.locid
                            where loccvg.peril = {self.para['perilid']}
                            group by property.accgrpid, property.locid, sitelimamt
                            ) as accttiv1 
                        group by accgrpid
                        ) as accttiv on p.accgrpid = accttiv.accgrpid
                    where p.policytype = {self.para['perilid']}
                        and pa.portinfoid = {self.para['portinfoid']}""")

            # policy_loc_conditions
            cur.execute(f"""select p.accgrpid, p.policyid, p.policytype, p.blanlimamt, p.partof, 
                    p.undcovamt, polded,lc.locid, pc.conditiontype
                    into #policy_loc_conditions 
                from #policy_standard as p 
                    inner join policyconditions as pc on p.policyid = pc.policyid 
                    inner join locconditions as lc on pc.conditionid = lc.conditionid
                where conditiontype = 2 and included in (1,2) and p.policytype = {self.para['perilid']}
                    and (pc.limit < p.partof + p.undcovamt or pc.deductible > 0)
                group by p.accgrpid, p.policyid, p.policytype, p.blanlimamt, 
                    p.partof, p.undcovamt, p.polded, lc.locid, pc.conditiontype""")

            # additional policy_loc_conditions
            cur.execute(f"""with incl_locs as 
                (select p.accgrpid, p.policyid, p.policytype, p.blanlimamt, p.partof, 
                    p.undcovamt, p.polded, lc.locid, pc.conditiontype
                from #policy_standard as p 
                    inner join policyconditions as pc on p.policyid = pc.policyid 
                    inner join locconditions as lc on pc.conditionid = lc.conditionid
                where conditiontype = 1 and included in (1,2) and p.policytype = {self.para['perilid']}
                group by p.accgrpid, p.policyid, p.policytype, p.blanlimamt, p.partof, p.undcovamt, 
                    p.polded, lc.locid, pc.conditiontype),

                all_locs as 
                (select polexcl.accgrpid, polexcl.policyid, alllocs.locid
                from (select accgrpid, policyid
                    from incl_locs
                    group by accgrpid, policyid) as polexcl 
                inner join (select l.accgrpid, l.locid
                    from loc as l 
                        inner join loccvg as lc on l.locid = lc.locid 
                    where lc.valueamt > 0 and lc.peril = {self.para['perilid']}
                    group by l.accgrpid, l.locid) as alllocs on polexcl.accgrpid = alllocs.accgrpid)

                insert into #policy_loc_conditions 
                select all_locs.accgrpid, all_locs.policyid, p.policytype, p.blanlimamt, p.partof, p.undcovamt, 
                    case when p.blandedamt > 0 then p.blandedamt else p.mindedamt end as polded,
                    all_locs.locid, 1 as conditiontype
                from all_locs 
                    inner join policy as p on all_locs.policyid = p.policyid 
                    left join incl_locs on all_locs.policyid = incl_locs.policyid and all_locs.locid = incl_locs.locid
                where incl_locs.locid is null""")

            # sqlpremalloc
            if 'rdm_database' in self.para and 'analysisid' in self.para:
                cur.execute(f"""with locpoltotals as (
                    select a.id as policyid, p.blanlimamt, p.partof, 
                        p.undcovamt, p.polded, res1value as locid, a.eventid,
                        sum(case when perspcode = 'gu' then perspvalue else 0  end) as grounduploss,
                        sum(case when perspcode = 'cl' then perspvalue else 0  end) as clientloss,
                        sum(case when perspcode = 'uc' then perspvalue else 0  end) as undcovloss,
                        sum(case when perspcode = 'ol' then perspvalue else 0  end) as overlimitloss,
                        sum(case when perspcode = 'oi' then perspvalue else 0  end) as otherinsurerloss,
                        sum(case when perspcode = 'gr' then perspvalue else 0  end) as grossloss,
                        sum(case when perspcode = 'ss' then perspvalue else 0  end) as surplusshareloss,
                        sum(case when perspcode = 'fa' then perspvalue else 0  end) as facloss,
                        sum(case when perspcode = 'rl' then perspvalue else 0  end) as netprecatloss
                    from {self.para['rdm_database']}..rdm_policy a 
                        inner join {self.para['rdm_database']}..rdm_eventareadetails b on a.eventid = b.eventid 
                        inner join #policy_standard as p on a.id = p.policyid
                    where anlsid = {self.para['analysisid']}
                    group by a.id, res1value, a.eventid, p.blanlimamt, p.partof, p.undcovamt, p.polded
                    having sum(case when perspcode = 'gu' then perspvalue else 0 end) * 
                        ({self.para['additional_coverage']} + 1) > p.undcovamt)

                    select case when cond.conditiontype is null then 0 else cond.conditiontype end as condition,
                        a.locid, b.accgrpid, c.accgrpname, a.policyid, b.policynum,
                        b.blanlimamt as orig_blanlim,
                        b.partof as orig_partof,
                        b.undcovamt as orig_undcovamt,
                        b.blanlimamt / b.partof as orig_participation,
                        a.grounduploss, clientloss, undcovloss, overlimitloss, otherinsurerloss, grossloss,
                        case when grossloss + otherinsurerloss = 0 then a.blanlimamt else
                            case when overlimitloss > 1 then (a.blanlimamt / a.partof) * (grossloss + otherinsurerloss) else a.blanlimamt end end as  rev_blanlimamt,
                        case when grossloss + otherinsurerloss = 0 then a.partof else 
                            case when overlimitloss > 1 then grossloss + otherinsurerloss else a.partof end end as rev_partof,	
                        case when a.partof = 0 then 1 else a.blanlimamt / a.partof end as participation,
                        clientloss as deductible,
                        case when cond.conditiontype = 2 then undcovloss else b.undcovamt end as undcovamt, grounduploss as origtiv,
                        case when grossloss + otherinsurerloss = 0 then clientloss + undcovloss else grounduploss end as rev_tiv,
                        case when grounduploss < totaltiv then grounduploss * (bldgval/totaltiv) else bldgval end as bldgval,
                        case when grounduploss < totaltiv then grounduploss * (contval/totaltiv) else contval end as contval,
                        case when grounduploss < totaltiv then grounduploss * (bival/totaltiv) else bival end as bival,
                        a.grossloss as spidergel, blanpreamt
                        into #sqlpremalloc
                    from locpoltotals a 
                        inner join #policy_standard b on a.policyid = b.policyid
                        inner join accgrp as c on b.accgrpid = c.accgrpid 
                        inner join 
                            (select locid, peril,
                                sum(case when losstype = 1 then valueamt else 0  end) as bldgval,
                                sum(case when losstype = 2 then valueamt else 0  end) as contval,
                                sum(case when losstype = 3 then valueamt else 0  end) as bival,
                                sum(valueamt) as totaltiv
                            from loccvg								
                            where peril = {self.para['perilid']}
                            group by locid, peril) as d on a.locid = d.locid 
                        left join #policy_loc_conditions as cond on a.policyid = cond.policyid and a.locid = cond.locid
                    where case when cond.conditiontype is null then 0 else cond.conditiontype end <> 1
                    order by b.accgrpid, a.locid, a.undcovloss""")
            else:
                cur.execute(f"""select 0 as conditionid, l.locid, a.accgrpid, a.accgrpname, 
                        p.policyid, p.policynum, p.blanlimamt as orig_blanlimamt, 
                        p.partof as orig_partof, p.undcovamt as orig_undcovamt,
                        p.polparticipation as orig_participation, l.tiv as grounduploss, 
                        p.polded as clientloss, p.undcovamt as undcovloss,
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
                        p.blanlimamt as rev_blanlimamt, p.partof as rev_partof, 
                        p.polparticipation as participation, p.polded as deductible, p.undcovamt as undcovamt,
                        l.tiv as origtiv, l.tiv as rev_tiv, bldgval, contval, bival, 
                        case when l.tiv < p.undcovamt + p.polded then 0
                            when l.tiv < p.undcovamt + p.partof + p.polded and l.tiv > p.undcovamt + p.polded 
                                then (l.tiv - (p.undcovamt + p.polded)) * (p.polparticipation)
                            else p.blanlimamt end as spidergel, p.blanpreamt
                        into #sqlpremalloc
                        from accgrp as a 
                            inner join policy_standard as p on a.accgrpid = p.accgrpid 
                            inner join policy as pol on p.policyid = pol.policyid 
                            inner join 
                                (select loc.accgrpid, loc.locid, sum(valueamt) as tiv,
                                    sum(case when losstype = 1 then valueamt else 0 end) as bldgval,
                                    sum(case when losstype = 2 then valueamt else 0 end) as contval,
                                    sum(case when losstype = 3 then valueamt else 0 end) as bival
                                from loc 
                                    inner join loccvg on loc.locid = loccvg.locid 
                                where peril = {self.para['perilid']}
                                group by loc.accgrpid, loc.locid
                                ) as l on a.accgrpid = l.accgrpid
                        where l.tiv * ({self.para['additional_coverage']} + 1) > p.undcovamt""")

            cur.execute(f"""update #sqlpremalloc
                set bldgval = rev_tiv * (bldgval / origtiv), 
                    contval = rev_tiv * (contval / origtiv), 
                    bival = rev_tiv * (bival / origtiv)
                where origtiv > rev_tiv + 1""")

            cur.commit()

    def __extract_policy(self, conn):
        retained_lmt = "(locpol.rev_partof - locpol.deductible) * locpol.rev_blanlimamt / locpol.rev_partof" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "locpol.rev_blanlimamt"
        gross_lmt = "locpol.rev_partof - locpol.deductible" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "locpol.rev_partof"
        df_pol = pd.read_sql_query(f"""select locpol.policyid as OriginalPolicyID, 
                    concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    {retained_lmt} as PolRetainedLimit, 
                    round({gross_lmt}, 2) as PolLimit, 
                    locpol.participation as PolParticipation, 
                    round(locpol.deductible + locpol.undcovamt, 2) as PolRetention, 
                    policy.blanpreamt as PolPremium
                from #sqlpremalloc as locpol 
                    inner join policy on locpol.policyid = policy.policyid""", conn)

        df_pol['job_id'] = self.job_id
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        to_sql(df_pol, "pat_policy", creds, index=False, if_exists='append')

        # normalize policy
        with pyodbc.connect(self.job_conn) as j_conn, j_conn.cursor() as cur:
            cur.execute(f"""update pat_policy set flag = 0 where job_id = {self.job_id}""")
            cur.execute(f"""update pat_policy set PolParticipation = PolRetainedLimit / PolLimit
                            where job_id  = {self.job_id}
                                and round(PolRetainedLimit - PolLimit * PolParticipation, 1) <= 2""")
            
            # Duplicate Policies 
            cur.execute(f"""update pat_policy set flag = flag + {PatFlag.FlagPolDupe}
                            where job_id  = {self.job_id}
                                and PseudoPolicyID in 
                                (select PseudoPolicyID from pat_policy 
                                 where job_id  = {self.job_id}
                                 group by PseudoPolicyID having count(*) > 1)""")

            # Policies with nonNumeric Fields
            cur.execute(f"""update pat_policy set flag = flag + {PatFlag.FlagPolNA}
                            where job_id  = {self.job_id}
                                and (PolLimit is null or PolRetention is null or PolRetainedLimit is null or
                                    PolParticipation is null or PolPremium is null)""")
            
            # Policies with negative fields
            cur.execute(f"""update pat_policy set flag = flag + {PatFlag.FlagPolNeg}
                            where job_id  = {self.job_id}
                                and (PolLimit < 0 or PolRetention < 0 or PolRetainedLimit < 0 or
                                    PolParticipation < 0 or PolPremium < 0)""")

            # Policies with inconsistant limit/participation alegebra
            cur.execute(f"""update pat_policy set flag = flag + {PatFlag.FlagPolLimitParticipation}
                            where job_id  = {self.job_id}
                                and round(PolRetainedLimit - PolLimit * PolParticipation, 1) > 2""") #? check above for same formula 

            # Policies with excess participation
            cur.execute(f"""update pat_policy set flag = flag + {PatFlag.FlagPolParticipation}
                            where job_id  = {self.job_id} 
                                and round(PolParticipation, 2) > 1""")

            cur.commit()

    def __extract_location(self, conn):
        aoi = "locpol.bldgval + locpol.contval + locpol.bival" if self.para[
            'coverage'] == "Building + Contents + Time Element" else "locpol.bldgval + locpol.contval"
        df_loc = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    locpol.locid as LocationIDStack,
                    {aoi} as AOI, 
                    loc.occscheme as occupancy_scheme, loc.occtype as occupancy_code 
                from #sqlpremalloc as locpol 
                    inner join loc on locpol.locid = loc.locid""", conn)
        
        df_loc['job_id'] = self.job_id
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        to_sql(df_loc, "pat_location", creds, index=False, if_exists='append')

        # normalize location
        with pyodbc.connect(self.job_conn) as j_conn, j_conn.cursor() as cur:
            df = pd.read_sql_query(f"""select min(PSOLD_RG) as min_rg, max(PSOLD_RG) as max_rg from psold_mapping""", j_conn)
            min_psold_rg = df.min_rg[0]
            max_psold_rg = df.max_rg[0]
           
            cur.execute(f"""update pat_location set flag = 0, RatingGroup = 0 where job_id  = {self.job_id}""")

            # Attach PSOLD mapping
            cur.execute(f"""update t set t.RatingGroup = m.PSOLD_RG
                                from pat_location t 
                                join psold_mapping m on t.occupancy_scheme = m.occscheme
                                    and t.occupancy_code = m.occtype --date constraint 
                                where job_id  = {self.job_id}""")

            # Location record duplications
            cur.execute(f"""update pat_location set flag = flag + {PatFlag.FlagLocDupe}
                            where job_id  = {self.job_id}
                                and PseudoPolicyID in 
                                (select PseudoPolicyID from pat_location 
                                 where job_id  = {self.job_id}
                                 group by PseudoPolicyID having count(*) > 1)""")
            
            # duplicate LocationIDStack
            cur.execute(f"""update pat_location set flag = flag + {PatFlag.FlagLocIDDupe}
                            where job_id  = {self.job_id}
                                and LocationIDStack in 
                                    (select LocationIDStack from 
                                        (select distinct LocationIDStack, round(aoi, 1) as AOI
                                         from pat_location
                                         where job_id = {self.job_id} and LocationIDStack is not null
                                        ) a
                                     group by LocationIDStack
                                     having count(*) > 1
                                    )""")

            # Location record non numeric entry
            cur.execute(f"""update pat_location set flag = flag + {PatFlag.FlagLocNA}
                            where job_id  = {self.job_id} 
                                and (AOI is null or RatingGroup is null or RatingGroup = 0)""")
            
            # Location record negative field
            cur.execute(f"""update pat_location set flag = flag + {PatFlag.FlagLocNeg}
                            where job_id  = {self.job_id} 
                                and (AOI <0 or RatingGroup < 0)""")

            # Location record rating group out of range
            cur.execute(f"""update pat_location set flag = flag + {PatFlag.FlagLocRG}
                            where job_id  = {self.job_id} 
                                and (RatingGroup < {min_psold_rg} or RatingGroup > {max_psold_rg})""")

            cur.commit()

    def __extract_fac(self, conn):
        df_fac = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    round(case when excessamt + layeramt > rev_blanlimamt then rev_blanlimamt - excessamt 
                        else layeramt end, 2) as FacLimit, 
                    excessamt as FacAttachment, pcntreins/100 as FacCeded
                from #sqlpremalloc as locpol 
                    inner join reinsinf on locpol.policyid = reinsinf.exposureid 
                where reinsinf.exposrtype = 'pol' and excessamt < rev_blanlimamt 
                    and layeramt > 0 and pcntreins > 0 
                order by locpol.accgrpid, locpol.locid, locpol.undcovamt, reinsinf.excessamt, FacLimit, pcntreins""", conn)

        # keep the original key (why?)
        df_fac['FacKey'] = np.arange(1, len(df_fac)+1, dtype=int)
        df_fac['job_id'] = self.job_id
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        to_sql(df_fac, "pat_facultative", creds, index=False, if_exists='append')

        # normalize location
        with pyodbc.connect(self.job_conn) as j_conn, j_conn.cursor() as cur:
            cur.execute(f"""update pat_facultative set flag = 0 where job_id  = {self.job_id}""")

            # Fac records with NA entries
            cur.execute(f"""update pat_facultative set flag = flag + {PatFlag.FlagFacNA}
                            where job_id  = {self.job_id} 
                                and (FacLimit is null or FacAttachment is null or FacCeded is null)""")

            # Fac records with negative entries
            cur.execute(f"""update pat_facultative set flag = flag + {PatFlag.FlagFacNeg}
                            where job_id  = {self.job_id} 
                                and (FacLimit < 0 or FacAttachment < 0 or FacCeded < 0)""")

            cur.commit()
        
    def __delete_temp_tables(self, conn):
        with conn.cursor() as cur:
            cur.execute("drop table #policy_standard")
            cur.execute("drop table #policy_loc_conditions")
            # cur.execute("drop table #locpoltotals")
            cur.execute("drop table #sqlpremalloc")
            cur.commit()

    def __extra_data_check(self):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            # Policies with no Locations
            cur.execute(f"""update pat_policy set flag = flag + {PatFlag.FlagPolNoLoc}
                            where job_id  = {self.job_id} 
                                and PseudoPolicyID not in 
                                    (select distinct PseudoPolicyID from pat_location where job_id = {self.job_id})""")

            # Location records with no policy
            cur.execute(f"""update pat_location set flag = flag + {PatFlag.FlagLocOrphan}
                            where job_id  = {self.job_id} 
                                and PseudoPolicyID not in 
                                    (select distinct PseudoPolicyID from pat_policy where job_id = {self.job_id})""")

            # Orphan Fac Records
            cur.execute(f"""update pat_facultative set flag = flag + {PatFlag.FlagFacOrphan}
                            where job_id  = {self.job_id} 
                                and PseudoPolicyID not in 
                                    (select distinct PseudoPolicyID from pat_policy where job_id = {self.job_id})""")

                    
            # FacNet combined specific checks
            f = PatFlag.FlagPolNA |PatFlag.FlagPolNeg | PatFlag.FlagFacNA | PatFlag.FlagFacNeg
            cur.execute(f"""with cte as (
                    select a.PolParticipation, a.PolRetention,
                        PolLimit + PolRetention as PolTopLine,
                        (case when b.FacKey is null then 0 else b.FacLimit end) / PolParticipation as FacGupLimit,
                        case when b.FacKey is null then 0 else b.FacAttachment end as FacAttachment,
                        b.FacKey
                    from pat_policy a 
                        left join pat_facultative b on a.job_id =b.job_id
                            and a.PseudoPolicyID = b.PseudoPolicyID
                    where a.job_id  = {self.job_id} and (a.flag & {f.value}) = 0 and (b.flag & {f.value}) = 0
                ),
                cte1 as (
                    select FacKey, PolTopLine,
                        FacAttachment / PolParticipation + PolRetention as FacGupAttachment,
                        FacGupLimit + FacAttachment / PolParticipation + PolRetention as FacGupTopLine
                    from cte
                )
                update pat_facultative set flag = flag + {PatFlag.FlagFacOverexposed}
                where job_id  = {self.job_id} and FacKey in (
                        select distinct FacKey 
                        from cte1
                        where FacGupAttachment - PolTopLine > 1 or FacGupTopLine - PolTopLine > 1
                    )""")

            cur.commit()

    def __need_correction(self):
        with pyodbc.connect(self.job_conn) as conn:
            for t in ['pat_policy','pat_location', 'pat_facultative']:
                df = pd.read_sql_query(f"""select count(*) from [{t}] where job_id = {self.job_id} and flag != 0""", conn)
                if df is not None and len(df)>0: 
                    return True
            
        return False
    
    #?
    def apply_user_correction(self):
        # use user input (pol.loc, fac) to replace the ones need to be correct, re-check data
        #for testing read from file

        modified = False
        # pol
        fn = r'C:\_Working\PAT_20201019\__temp\pol_correction.csv'
        if os.path.isfile(fn):  
            corr =  pd.read_csv(fn).set_index('PseudoPolicyID')
            if len(corr) > 0:
                df = self.df_pol.set_index('PseudoPolicyID')
                df.update(corr)
                self.df_pol = df.reset_index()
                modified = True

        # loc
        fn = r'C:\_Working\PAT_20201019\__temp\loc_correction.csv'
        if os.path.isfile(fn):  
            corr =  pd.read_csv(fn).set_index('PseudoPolicyID')
            if len(corr) > 0:
                df = df_loc.set_index('PseudoPolicyID')
                df.update(corr)
                df_loc = df.reset_index()
                modified = True
        
        # fac
        fn = r'C:\_Working\PAT_20201019\__temp\fac_correction.csv'
        if os.path.isfile(fn):  
            corr =  pd.read_csv(fn).set_index(['PseudoPolicyID', 'FacKey'])
            if len(corr) > 0:
                df = df_loc.set_index(['PseudoPolicyID', 'FacKey'])
                df.update(corr)
                df_loc = df.reset_index()
                modified = True

        if modified:
            self.check_data()
            self.net_of_fac()
   
    def __net_of_fac(self):
        with pyodbc.connect(self.job_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""with good_p as
                    (select distinct a.PseudoPolicyID
                    from pat_policy a
                        join pat_location b on a.job_id = b.job_id and a.PseudoPolicyID =b.PseudoPolicyID
                        left join 
                            (select distinct PseudoPolicyID from pat_facultative where job_id = {self.job_id} and flag != 0)
                            c on a.PseudoPolicyID = c.PseudoPolicyID
                        where a.job_id = {self.job_id} and a.flag = 0 and b.flag = 0 and c.PseudoPolicyID is null
                    )
                    select OriginalPolicyID, a.PseudoPolicyID, PolRetainedLimit, PolLimit, 
                        case when PolParticipation > 1 then 1 else PolParticipation end as PolParticipation, 
                        PolRetention, PolPremium,
                        PolLimit + PolRetention as PolTopLine
                    into #dfPolUse
                    from pat_policy a 
                        join good_p b on a.PseudoPolicyID = b.PseudoPolicyID 
                    where a.job_id = {self.job_id}""")

                cur.execute(f"""select a.PseudoPolicyID, PolRetention,PolTopLine, FacCeded,
                        FacLimit / PolParticipation as FacGupLimit,
                        FacAttachment / PolParticipation + PolRetention as FacGupAttachment,
                        (FacLimit + FacAttachment) / PolParticipation + PolRetention as FacGupTopLine
                    into #dfPolFac
                    from #dfPolUse a 
                        join pat_facultative b on a.PseudoPolicyID = b.PseudoPolicyID 
                    where b.job_id = {self.job_id}""")
                
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
                        (select a.*, b.OriginalPolicyID, b.PolParticipation as Participation, b.PolPremium, 
                            case when c.FacCeded is null then 0 else c.FacCeded end as Ceded
                        from cte3 a
                            Left Join #dfPolUse b ON a.PseudoPolicyID = b.PseudoPolicyID
                            Left Join #dfPolFac c ON a.PseudoPolicyID = c.PseudoPolicyID 
                                and a.LayerLow >= c.FacGupAttachment and a.LayerHigh <= FacGupTopLine
                    )
                    select OriginalPolicyID,PseudoPolicyID,LayerLow,LayerHigh,Participation,max(PolPremium) as PolPremium,
                        sum(case when Ceded is null then 0 else Ceded end) as Ceded,
                        row_number() OVER (PARTITION BY PseudoPolicyID ORDER BY LayerLow) as LayerID
                        into #dfLayers
                    from cte4
                    group by PseudoPolicyID,OriginalPolicyID, LayerLow,LayerHigh,Participation""")

                cur.execute(f"""select distinct PseudoPolicyID into #ceded100 
                                from #dfLayers where round(Ceded, 4) >1;
                            update pat_facultative set flag = flag + {PatFlag.FlagCeded100}
                            where job_id = {self.job_id} and PseudoPolicyID in 
                                (select PseudoPolicyID from #ceded100);
                            """)

                cur.commit()
            
            df_facnet = pd.read_sql_query(f"""select OriginalPolicyID, a.PseudoPolicyID, LayerID as PseudoLayerID,
                    LayerHigh - LayerLow as Limit, LayerLow as Retention, 
                    PolPremium as OriginalPremium, 
                    case when Ceded > 1 then 0 else Participation * (1 - Ceded) end as Participation,
                    b.AOI, b.LocationIDStack, b.RatingGroup
                from #dfLayers a
                    left join pat_location b on a.PseudoPolicyID = b.PseudoPolicyID
                where b.job_id ={self.job_id} and a.PseudoPolicyID not in 
                    (select PseudoPolicyID from #ceded100)
                order by a.PseudoPolicyID, LayerID, LayerLow, LayerHigh
                """,conn)

            with conn.cursor() as cur:
                cur.execute(f"""drop table #dfPolUse;
                                drop table #dfPolFac;
                                drop table #dfLayers;
                                drop table #ceded100""")
                
                cur.commit()
        
            return df_facnet
    
    def __allocate_with_psold(self, df_facnet):     
        dfFacNetLast = df_facnet.groupby('PseudoPolicyID').agg({'PseudoLayerID':max}).rename(columns={'PseudoLayerID':'LayerPosition'})
        # dfLocation2
        dfLoc = df_facnet.merge(dfFacNetLast, left_on='PseudoPolicyID', right_index=True)

        with pyodbc.connect(self.job_conn) as conn:
            dfWeights = pd.read_sql_query(f"""select * from psold_weight""", conn)
            AOI_split = pd.read_sql_query(f"""select * from psold_aoi order by AOI""", conn).AOI.to_numpy() 
            guNewPSTable = pd.read_sql_query(f"""select * from psold_gu_2016 order by COVG, SUBGRP, RG, EG""", conn)

        gdMu = [1000 * (np.sqrt(10)**(i-1)) for i in range(1,12)]
        dfWeights = self.__calc_weights(dfWeights, guNewPSTable, gdMu, AOI_split)

        # dfLocation3
        dfLoc = self.__calc_detail_info(dfLoc, AOI_split)
        
        # dfLocation4
        dfLoc = self.__stack_check(dfLoc)
        
        iUniqueRG = dfLoc[['RatingGroup']].dropna().drop_duplicates().iloc[:,0].to_numpy()
        iUniqueAOI = dfLoc[['iAOI']].dropna().drop_duplicates().iloc[:,0].to_numpy()
        dfUniqueDetail = guNewPSTable[np.all([guNewPSTable.RG.isin(iUniqueRG),  
            guNewPSTable.EG.isin(iUniqueAOI) ,
            guNewPSTable.SUBGRP == self.iSubGrp,
            guNewPSTable.COVG == self.iCovg], axis= 0)][["RG","EG","AOCC"] + [f"G{i+1}" for i in range(11)] +
                [f"R{i+1}" for i in range(11)]]
        
        # dfLocation5
        dfLoc = self.__calc_las(dfLoc, gdMu, dfWeights, dfUniqueDetail)

        # dfLocation6
        dfLoc['sumLAS'] = (dfLoc.PolLAS-dfLoc.DedLAS)*self.loss_ratio*dfLoc.Participation
        dfLoc['sumLAS'] = dfLoc['sumLAS'].fillna(value=0)
        dfLoc['sumLAS'] = dfLoc['sumLAS'].groupby(dfLoc.OriginalPolicyID).transform('sum')
        dfLoc['premLAS'] = dfLoc.OriginalPremium / dfLoc.sumLAS
        dfLoc['Premium'] = (dfLoc.PolLAS - dfLoc.DedLAS) * self.loss_ratio * dfLoc.Participation * dfLoc.premLAS
        dfLoc['EffPrem'] = (dfLoc.Premium * self.dSubjPrem/ dfLoc.Premium.sum())
        dfLoc['ExpectedLossCount'] = ((np.minimum(dfLoc.RetStepLAS,dfLoc.PolLAS)-np.minimum(dfLoc.PolLAS, dfLoc.RetLAS)) \
            /(dfLoc.PolLAS - dfLoc.DedLAS) * self.loss_ratio*dfLoc.EffPrem )/.01

        # Final
        df_pat = dfLoc[["Limit", "Retention", "Premium", "Participation", "AOI", "LocationIDStack",
                    "RatingGroup", "OriginalPolicyID", "PseudoPolicyID", "PseudoLayerID", "PolLAS", "DedLAS"]] \
                .sort_values(by=['OriginalPolicyID', 'PseudoPolicyID', 'PseudoLayerID'])
        df_pat.rename(columns={
            'Premium':'Allocated_Premium',
            'RatingGroup':'Rating_Group',
            'OriginalPolicyID':'Original_Policy_ID',
            'PseudoLayerID': 'Pseudo_Layer_ID',
            'PolLAS':'PolicyLAS',
            'DedLAS':'DeductibleLAS',
            'LocationIDStack':'Original_Location_ID'
            } )

        return df_pat
      
    def __calc_weights(self,tableWgts, guNewPSTable, gdMu, AOI_split):
        # Initialize Variables
        # holds relative counts for custom mix of occupancies (NET) - i.e. green area in model
        
        uPSCustom_uNet_dOccCnt = [0] * 60
        uPSCustom_uGross_dOccCnt = [0] * 60                  
        # holds relative counts for custom mix of occupancies (GROSS) - i.e. green area in model
        # holds weights for custom mix of occupancies (NET) - i.e. blue area in model
        uPSCustom_uNet_dW = pd.DataFrame(np.zeros((11,60), dtype=float), columns=[f"AOI{i+1}" for i in range(60)])
        # holds weights for custom mix of occupancies (GROSS) - i.e. blue area in model
        uPSCustom_uGross_dW = pd.DataFrame(np.zeros((11,60), dtype=float), columns=[f"AOI{i+1}" for i in range(60)])
        
        dAdjFactor = 1    
        guNewPSTable_sub = guNewPSTable[(guNewPSTable.COVG == self.iCovg) & (guNewPSTable.SUBGRP == self.iSubGrp)]
        for iAOI in range(1,61):
            guNewPSTable_sub2 = guNewPSTable_sub[guNewPSTable_sub.EG == iAOI].reset_index(drop=True)
            # If the rating group is selected in the premium weight portion of the user input table,
            # then calc a running total of the LAS for that rating group in the specific AOI band.
            
            # NET CALCULATIONS
            dSumNetPrem = np.sum(guNewPSTable_sub2[[f"R{i+1}" for i in range(11)]].apply(
                lambda a: (np.sum(a * dAdjFactor * gdMu * (1-np.exp(
                        (-1 * AOI_split[iAOI] * (1 + self.AddtlCovg)) /
                        (dAdjFactor * gdMu))))
                    ) / np.sum(a), axis = 1) * 
                guNewPSTable_sub2.OCC * tableWgts.PremiumPercent)

            # GROSS CALCULATIONS
            dSumGrossPrem = np.sum(guNewPSTable_sub2[[f"G{i+1}" for i in range(11)]].apply(
                lambda a: (np.sum(a * dAdjFactor * gdMu * (1-np.exp(
                        (-1 * AOI_split[iAOI] * (1 + self.AddtlCovg)) /
                        (dAdjFactor * gdMu))))
                    ) / np.sum(a), axis = 1) * 
                guNewPSTable_sub2.AOCC * (tableWgts.PremiumPercent))
            
            
            # If the rating group is selected in the premium weight portion of the user input table, 
            # then calc the relative frequency.
            
            # NET CALCULATIONS
            dtmpOccCnt = tableWgts.PremiumPercent * dSumNetPrem \
                / guNewPSTable_sub2[[f"R{i+1}" for i in range(11)]].apply(
                    lambda a: np.sum(a * dAdjFactor * gdMu *
                        (1-np.exp((-1*AOI_split[iAOI] * (1+self.AddtlCovg)) / (dAdjFactor * gdMu)))) 
                        / sum(a), axis =1)
            uPSCustom_uNet_dOccCnt[iAOI-1] = np.sum(dtmpOccCnt)
            uPSCustom_uNet_dW[f"AOI{iAOI}"] = np.dot(
                guNewPSTable_sub2[[f"R{i+1}" for i in range(11)]].T,
                dtmpOccCnt
            )

            # GROSS CALCULATIONS
            dtmpOccCnt = tableWgts.PremiumPercent * dSumGrossPrem \
                / guNewPSTable_sub2[[f"G{i+1}" for i in range(11)]].apply(
                    lambda a: np.sum(a * dAdjFactor * gdMu *
                        (1-np.exp((-1*AOI_split[iAOI] * (1+self.AddtlCovg)) / (dAdjFactor * gdMu)))) 
                        / sum(a), axis =1)
            uPSCustom_uGross_dOccCnt[iAOI-1] = np.sum(dtmpOccCnt)
            uPSCustom_uGross_dW[f"AOI{iAOI}"] = np.dot(
                guNewPSTable_sub2[[f"G{i+1}" for i in range(11)]].T,
                dtmpOccCnt
            )

            # Convert uPSCustom.dOccCnt from list to vector of length 60
            # uPSCustom_uNet_dOccCnt = unlist(uPSCustom_uNet_dOccCnt)
            # uPSCustom_uGross_dOccCnt = unlist(uPSCustom_uGross_dOccCnt)
            
            uPSCustom_uNet_dW[f"AOI{iAOI}"] = uPSCustom_uNet_dW[f"AOI{iAOI}"] / uPSCustom_uNet_dOccCnt[iAOI-1]
            uPSCustom_uGross_dW[f"AOI{iAOI}"] = uPSCustom_uGross_dW[f"AOI{iAOI}"] / uPSCustom_uGross_dOccCnt[iAOI-1]

        # Combine the occurrence counts and weights into one table (one for Net and one for Gross)
        
        PSCustomNet = pd.DataFrame(np.reshape(uPSCustom_uNet_dOccCnt,(1,60)), columns =uPSCustom_uNet_dW.columns)\
            .append(uPSCustom_uNet_dW, ignore_index=True)

        PSCustomGross = pd.DataFrame(np.reshape(uPSCustom_uGross_dOccCnt,(1,60)), columns =uPSCustom_uNet_dW.columns)\
            .append(uPSCustom_uGross_dW, ignore_index=True)
        
        PSCustom = PSCustomGross.join(PSCustomNet,lsuffix='_gross', rsuffix='_net').reset_index()
        # PSCustom.index = ['OccCnt'] + [f"GrossWgt{i+1}" for i in range(11)]

        return PSCustom

    def __calc_detail_info(self, df, AOI_split):
        dTotalPrem = df.OriginalPremium.sum()
        dfLocation3 = df
        dfLocation3['AddtlCovg'] = self.AddtlCovg
        dfLocation3['iCurveSwitch'] = 1
        dfLocation3.loc[dfLocation3.Retention.isna(), ['iCurveSwitch']] = 2

        dfLocation3['dPolDed'] = 0
        dfLocation3.loc[dfLocation3.Retention.notna(), ['dPolDed']] = dfLocation3.Retention / self.dCurrencyAdj

        dfLocation3['dPolLmt'] = dfLocation3.Limit / self.dCurrencyAdj
        if self.iDedType == 1:
            dfLocation3['dPolLmt'] += dfLocation3['dPolDed'] 

        dfLocation3['EffPrem'] = dfLocation3.OriginalPremium * self.dSubjPrem/dTotalPrem

        dfLocation3['dTIV'] = 0
        dfLocation3.loc[dfLocation3.AOI.notna(), ['dTIV']] = dfLocation3.AOI / self.dCurrencyAdj
        dfLocation3.loc[dfLocation3.AOI.isna(), ['dTIV']] = dfLocation3.Limit / self.dCurrencyAdj
        if self.iDedType == 1:
            dfLocation3.loc[dfLocation3.AOI.isna(), ['dTIV']] += dfLocation3.dPolDed[dfLocation3.AOI.isna()]

        dfLocation3.loc[dfLocation3.Participation.isna(), ['Participation']] = 1
        dfLocation3.loc[dfLocation3.LocationIDStack == '', ['LocationIDStack']] = np.nan

        dfLocation3['dX'] = np.maximum(dfLocation3.dPolLmt - dfLocation3.dPolDed, 0)
        dfLocation3.loc[dfLocation3.LocationIDStack.isna(), ['dX']] *= (1+ self.AddtlCovg)
        dfLocation3['dX']=dfLocation3.dX + dfLocation3.dPolDed

        dfLocation3['dAdjFactor'] = self.dCurrencyAdj if self.dtAveAccDate == 0 \
            else self.dCurrencyAdj * self.gdPSTrendFactor ** ((self.dtAveAccDate - self.gdtPSTrendFrom).days / 365.25)

        dfLocation3['dTIVAdj'] = dfLocation3.dTIV / dfLocation3.dAdjFactor
        dfLocation3['iAOI'] = np.searchsorted(AOI_split[1:-1],dfLocation3.dTIVAdj, side='right') + 1

        dfLocation3['dEffLmt'] = dfLocation3[["dTIV", "dPolLmt"]].min(axis=1)

        return dfLocation3

    def __stack_check(self, df):
        df['OrigOrder'] = df.index
        df.loc[df.LocationIDStack.notna(), ['iCurveSwitch']] = 1


        df['StackedPolicyLimit'] = (df.dPolLmt-df.dPolDed)*df.Participation
        df['StackedPolicyLimit'] = df.sort_values(['LocationIDStack','Retention', 'Limit']) \
            .groupby('LocationIDStack')['StackedPolicyLimit'].transform(lambda x: x.cumsum().shift())
        df.loc[np.logical_or(df.LocationIDStack.isna(),df.StackedPolicyLimit.isna()), 
            ['StackedPolicyLimit']] = 0

        df['dEffLimRet'] = np.maximum(0, ((self.gdReinsuranceLimit + self.gdReinsuranceRetention)-df.StackedPolicyLimit)/df.Participation)
        df['dEffRet'] = np.maximum(0, ((self.gdReinsuranceRetention)-df.StackedPolicyLimit)/df.Participation)
        
        df.set_index('OrigOrder').sort_index()
        df.index.name = None
        df.drop(columns=['OrigOrder'], inplace=True)

        return df

    def __calc_las(self, df, gdMu, dfWeights, dfUnique):
        df['w'] = 0
        df['sum_w'] = 0
        rein = max(min(self.gdReinsuranceRetention *
                    1e-8, self.gdReinsuranceLimit), 1e-2)

        for n, X in [('PolLAS', df.dX),
                    ('DedLAS', df.dPolDed),
                    ('LimRetLAS', (df.dPolDed + df.dEffLimRet)),
                    ('RetLAS', (df.dPolDed + df.dEffRet)),
                    ('RetStepLAS', (df.dPolDed + df.dEffRet + rein/df.Participation))]:

            df[n] = 0
            mask1 = np.logical_and(X.notna(), X > 0)
            mask2 = df.RatingGroup.isna()
            mask3 = df.iCurveSwitch[mask1] == 1
            df['sum_w'] = 0
            for i in range(11):
                df['w'] = 0
                df.loc[np.logical_and(mask1, np.logical_and(mask2, mask3)), [
                        'w']] = dfWeights[f'AOI{i+1}_gross'][i+1]
                df.loc[np.logical_and(mask1, np.logical_and(mask2, ~mask3)), [
                    'w']] = dfWeights[f'AOI{i+1}_net'][i+1]

                mask = np.logical_and(mask1, np.logical_and(~mask2, mask3))
                if np.any(mask) :
                    df.loc[mask, ['w']] = df.loc[mask, ['RatingGroup', 'iAOI']].merge(dfUnique[['RG', 'EG', f'G{i+1}']],
                                                                                    left_on=['RatingGroup', 'iAOI'], right_on=['RG', 'EG'], how='left')[f'G{i+1}']

                mask = np.logical_and(mask1, np.logical_and(~mask2, ~mask3))
                if np.any(mask) :
                    df.loc[mask, ['w']] = df.loc[mask, ['RatingGroup', 'iAOI']].merge(dfUnique[['RG', 'EG', f'R{i+1}']],
                                                                                    left_on=['RatingGroup', 'iAOI'], right_on=['RG', 'EG'], how='left')[f'R{i+1}']
                df['sum_w'] = df.sum_w + df.w

                df[n] += self.__me_las(df, X, gdMu[i])

            df[n] /= df.sum_w

        df.drop(columns=['sum_w', 'w'], inplace=True)
        
        df['MaxLoss'] = 1e-10
        df.loc[df.OriginalPremium > 0, ['MaxLoss']] = np.maximum(np.minimum(
            df.Limit*(1+df.AddtlCovg)-df.dPolDed, df.dEffLimRet)-df.dEffRet, 0) * df.Participation
        df['ExpectedLossCount'] = ((np.minimum(df.RetStepLAS, df.PolLAS) - np.minimum(
            df.PolLAS, df.RetLAS)) / (df.PolLAS-df.DedLAS) * self.loss_ratio * df.EffPrem) / rein
        
        df['PercentExpos'] = 0
        df.loc[df.OriginalPremium > 0, 'PercentExpos'] = \
            (np.maximum(np.minimum(df.PolLAS, df.LimRetLAS)-df.DedLAS, 0) - 
            np.maximum(np.minimum(df.PolLAS, df.RetLAS)-df.DedLAS, 0))/(df.PolLAS-df.DedLAS)
        
        return df

    def __me_las(self,df, X, Mu):
        Y = df[['dX']].rename(columns={'dX':'Y'})
        Y['Y'] = 0

        mask = np.logical_and(np.logical_and(X.notna(), X > 0), X <= (1+ df.AddtlCovg) * df.dEffLmt)
        Y.loc[mask,['Y']] = df.dAdjFactor[mask] * Mu * df.w[mask] * (1- np.exp(-X[mask]/(df.dAdjFactor[mask] * Mu)))

        mask = np.logical_and(np.logical_and(X.notna(), X > 0), X > (1+ df.AddtlCovg) * df.dEffLmt)
        Y.loc[mask,['Y']] = df.dAdjFactor[mask] * Mu * df.w[mask] * (1- np.exp(-(1+ df.AddtlCovg[mask]) * df.dEffLmt[mask]/(df.dAdjFactor[mask] * Mu)))

        return Y['Y']


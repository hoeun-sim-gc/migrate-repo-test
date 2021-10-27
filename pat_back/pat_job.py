import os
import json
import uuid  
import logging
from datetime import datetime
from flask.helpers import _split_blueprint_path

import numpy as np
import pandas as pd

import pyodbc
from bcpandas import SqlCreds, to_sql

from .settings import AppSettings
from .pat_flag import PatFlag,ValidRule

def split_flag(row):
    if row is None or len(row)<=0: 
        return []
         
    desc = PatFlag.describe(row[0])
    return desc.split(',') if desc else []

class PatJob:
    """Class to represent a PAT analysis"""

    peril_table = {1: "eqdet", 2: "hudet", 3: "todet",
                   4: "fldet", 5: "frdet", 6: "trdet"}
    job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    def __init__(self, job_id):
        self.job_id = 0
        self.data_extracted = False
        self.valid_rules = ValidRule(-1)
        self.default_region = 0
        with pyodbc.connect(self.job_conn) as conn:
            df = pd.read_sql(f"select data_extracted, parameters from pat_job where job_id = {job_id} and status='wait_to_start'",conn)
            if len(df) == 1:
                self.job_id = job_id
                self.para = json.loads(df.parameters[0])
                self.data_extracted = df.data_extracted[0] != 0
                
        self.logger = logging.getLogger(f"{self.job_id}")
        self.__update_status("started")
        self.logger.info(f"""Start to process job:{self.job_id}""")

        if 'valid_rules' in self.para:
            self.valid_rules = ValidRule(self.para['valid_rules'])
        if 'default_region' in self.para:
            self.default_region = self.para['default_region']
        self.user_name = self.para['user_name'] if 'user_name' in self.para else None
        self.user_email = self.para['user_email'] if 'user_email' in self.para else None
        
        self.iCovg = 2 if self.para['coverage'] == "Building + Contents + Time Element" else 1
        self.iSubGrp = {
                "Fire":1,
                "Wind":2,
                "Special Cause of Loss": 3,
                "All Perils": 4
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

    def __update_status(self, st):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = '{st.replace("'","''")}', 
                update_time = '{datetime.utcnow().isoformat()}'
                where job_id = {self.job_id}""")
            cur.commit()

    def perform_analysis(self, stop_cb=None):
        self.logger.info("Import data...")

        if not self.data_extracted:
            self.__update_status("extracting_data")
            self.__extract_edm_rdm()
            self.data_extracted = True
            with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
                cur.execute(f"""update pat_job set data_extracted = 1, 
                    update_time = '{datetime.utcnow().isoformat()}'
                    where job_id = {self.job_id}""")
                cur.commit()
            self.logger.info("Import data...OK")
        if stop_cb and stop_cb():
            self.logger.warning("User stopped the analysis")
            self.__update_status('stopped')
            return

        self.__update_status('checking_data')
        self.logger.debug("Check and create valid PAT data...")
        self.__create_pat_data()
        self.logger.debug("Check and create valid PAT data...OK")
        if stop_cb and stop_cb():
            self.logger.warning("User stopped the analysis")
            self.__update_status('stopped')
            return

        if self.__need_correction():
            if (self.valid_rules & ValidRule.ValidContinue) ==0:
                self.logger.error("Need to correct data then run again")
                self.__update_status("stopped")
                return
            else:
                self.logger.warning("Skip erroneous data and continue (item removed)")
        if stop_cb and stop_cb():
            self.logger.warning("User stopped the analysis")
            self.__update_status('stopped')
            return
        
        # start calculation
        self.__update_status("net_of_fac")
        self.logger.info("Create the net of FAC layer stack ...")
        df_facnet = self.__net_of_fac()
        if len(df_facnet)<=0:
            self.logger.warning("Nothing to allocate! Finished.")
            self.__update_status("finished")
            return 
        self.logger.info(f"Create the net of FAC layer stack...OK ({len(df_facnet)})")
        if stop_cb and stop_cb():
            self.logger.warning("User stopped the analysis")
            self.__update_status('stopped')
            return

        self.__update_status("allocating")
        self.logger.info("Allocate premium with PSOLD...")
        df_pat = self.__allocate_with_psold(df_facnet)
        self.logger.info("Allocate premium with PSOLD...OK")
        if stop_cb and stop_cb():
            self.logger.warning("User stopped the analysis")
            self.__update_status('stopped')
            return

        # save results
        self.__update_status("upload_results")
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
        conn_str = f'''DRIVER={{SQL Server}};Server={self.para["server"]};Database={self.para["edm"]};
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
            
            self.logger.debug("Extract policy table...")
            n = self.__extract_policy(conn, suffix)
            self.logger.debug(f"Extract policy table...OK ({n})")
            
            self.logger.debug("Extract location table...")
            n = self.__extract_location(conn, suffix)
            self.logger.debug(f"Extract location table...OK ({n})")
            
            self.logger.debug("Extract fac table...")
            n = self.__extract_facultative(conn, suffix)
            self.logger.debug(f"Extract fac table...OK ({n})")

            with conn.cursor() as cur:            
                cur.execute(f"drop table #sqlpremalloc_{suffix}")
                cur.commit()

    def __verify_edm_rdm(self, conn):
        # Check that PerilID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.para['edm']}]..policy a
                    join [{self.para['edm']}]..portacct b on a.accgrpid = b.accgrpid
                where b.portinfoid = {self.para['portinfoid']} and policytype = {self.para['perilid']}""", conn)) <= 0:
            return 'PerilID is not valid!'

        # Check that portfolio ID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.para['edm']}]..loccvg 
                    inner join [{self.para['edm']}]..property on loccvg.locid = property.locid 
                    inner join [{self.para['edm']}]..portacct on property.accgrpid = portacct.accgrpid 
                where loccvg.peril = {self.para['perilid']} and portacct.portinfoid = {self.para['portinfoid']}""", conn)) <= 0:
            return 'portfolio ID is not valid!'

        # Check that analysis ID is valid
        if 'rdm' in self.para and 'analysisid' in self.para:
            if len(pd.read_sql_query(f"""select top 1 1
            FROM [{self.para['edm']}]..loccvg 
                inner join [{self.para['edm']}]..property on loccvg.locid = property.locid 
                inner join [{self.para['edm']}]..portacct on property.accgrpid = portacct.accgrpid 
                inner join [{self.para['edm']}]..policy on property.accgrpid = policy.accgrpid 
                inner join [{self.para['rdm']}]..rdm_policy on policy.policyid = rdm_policy.id 
            where portacct.portinfoid = {self.para['portinfoid']} and loccvg.peril = {self.para['perilid']}
            and rdm_policy.ANLSID = {self.para['analysisid']}""", conn)) <= 0:
                print('portfolio ID is not valid!')

        return 'ok'
    
    def __create_temp_tables(self, conn, suffix:str):
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
                    into #policy_standard_{suffix}
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
            cur.commit()

            # policy_loc_conditions
            cur.execute(f"""select p.accgrpid, p.policyid, p.policytype, p.blanlimamt, p.partof, 
                    p.undcovamt, polded,lc.locid, pc.conditiontype
                    into #policy_loc_conditions_{suffix} 
                from #policy_standard_{suffix} as p 
                    inner join policyconditions as pc on p.policyid = pc.policyid 
                    inner join locconditions as lc on pc.conditionid = lc.conditionid
                where conditiontype = 2 and included in (1,2) and p.policytype = {self.para['perilid']}
                    and (pc.limit < p.partof + p.undcovamt or pc.deductible > 0)
                group by p.accgrpid, p.policyid, p.policytype, p.blanlimamt, 
                    p.partof, p.undcovamt, p.polded, lc.locid, pc.conditiontype""")
            cur.commit()

            # additional policy_loc_conditions
            cur.execute(f"""with incl_locs as 
                (select p.accgrpid, p.policyid, p.policytype, p.blanlimamt, p.partof, 
                    p.undcovamt, p.polded, lc.locid, pc.conditiontype
                from #policy_standard_{suffix} as p 
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

                insert into #policy_loc_conditions_{suffix} 
                select all_locs.accgrpid, all_locs.policyid, p.policytype, p.blanlimamt, p.partof, p.undcovamt, 
                    case when p.blandedamt > 0 then p.blandedamt else p.mindedamt end as polded,
                    all_locs.locid, 1 as conditiontype
                from all_locs 
                    inner join policy as p on all_locs.policyid = p.policyid 
                    left join incl_locs on all_locs.policyid = incl_locs.policyid and all_locs.locid = incl_locs.locid
                where incl_locs.locid is null""")
            cur.commit()

            # sqlpremalloc
            if 'rdm' in self.para and 'analysisid' in self.para:
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
                    from {self.para['rdm']}..rdm_policy a 
                        inner join {self.para['rdm']}..rdm_eventareadetails b on a.eventid = b.eventid 
                        inner join #policy_standard_{suffix} as p on a.id = p.policyid
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
                        into #sqlpremalloc_{suffix}
                    from locpoltotals a 
                        inner join #policy_standard_{suffix} b on a.policyid = b.policyid
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
                        left join #policy_loc_conditions_{suffix} as cond on a.policyid = cond.policyid and a.locid = cond.locid
                    where case when cond.conditiontype is null then 0 else cond.conditiontype end <> 1
                    order by b.accgrpid, a.locid, a.undcovloss""")
                cur.commit()
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
                        into #sqlpremalloc_{suffix}
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
                cur.commit()

            cur.execute(f"""update #sqlpremalloc_{suffix}
                set bldgval = rev_tiv * (bldgval / origtiv), 
                    contval = rev_tiv * (contval / origtiv), 
                    bival = rev_tiv * (bival / origtiv)
                where origtiv > rev_tiv + 1""")
            cur.commit()

            cur.execute(f"""drop table #policy_standard_{suffix};
                            drop table #policy_loc_conditions_{suffix};""")
            cur.commit()

    def __extract_policy(self, conn, suffix:str):
        retained_lmt = "(locpol.rev_partof - locpol.deductible) * locpol.rev_blanlimamt / locpol.rev_partof" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "locpol.rev_blanlimamt"
        gross_lmt = "locpol.rev_partof - locpol.deductible" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "locpol.rev_partof"
        df_pol = pd.read_sql_query(f"""select locpol.policyid as OriginalPolicyID, policy.ACCGRPID, 
                    concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    {retained_lmt} as PolRetainedLimit, 
                    round({gross_lmt}, 2) as PolLimit, 
                    locpol.participation as PolParticipation, 
                    round(locpol.deductible + locpol.undcovamt, 2) as PolRetention, 
                    policy.blanpreamt as PolPremium
                from #sqlpremalloc_{suffix} as locpol 
                    inner join policy on locpol.policyid = policy.policyid""", conn)

        # Save to DB 
        if len(df_pol) > 0:
            df_pol['job_id'] = self.job_id
            df_pol['data_type'] = int(1)
            df_pol['flag'] = int(0)
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_pol, "pat_policy", creds, index=False, if_exists='append')

        return len(df_pol)

    def __extract_location(self, conn, suffix):
        aoi = "locpol.bldgval + locpol.contval + locpol.bival" if self.para[
            'coverage'] == "Building + Contents + Time Element" else "locpol.bldgval + locpol.contval"
        df_loc = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    locpol.locid as LocationIDStack,
                    {aoi} as AOI, 
                    loc.occscheme as occupancy_scheme, loc.occtype as occupancy_code 
                from #sqlpremalloc_{suffix} as locpol 
                    inner join loc on locpol.locid = loc.locid""", conn)

        # Save to DB
        if len(df_loc) > 0: 
            df_loc['job_id'] = self.job_id
            df_loc['data_type'] = int(1)
            df_loc['flag'] = int(0)
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_loc, "pat_location", creds, index=False, if_exists='append')

        return len(df_loc)

    def __extract_facultative(self, conn, suffix:str):
        df_fac = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    round(case when excessamt + layeramt > rev_blanlimamt then rev_blanlimamt - excessamt 
                        else layeramt end, 2) as FacLimit, 
                    excessamt as FacAttachment, pcntreins/100 as FacCeded
                from #sqlpremalloc_{suffix} as locpol 
                    inner join reinsinf on locpol.policyid = reinsinf.exposureid 
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
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_fac, "pat_facultative", creds, index=False, if_exists='append')

        return len(df_fac)

    def __create_pat_data(self):
        with pyodbc.connect(self.job_conn) as conn:
            self.__check_policy(conn)
            self.__check_location(conn)
            self.__check_facultative(conn)
            self.__cross_tab_check(conn)
            
    def __check_policy(self, conn):
        with conn.cursor() as cur:
            cur.execute(f"""delete from pat_policy where job_id = {self.job_id} and data_type = 0""")
            cur.commit()

            cur.execute(f"""select PseudoPolicyID, max(data_type) as data_type
                        into #pol_active 
                    from pat_policy where job_id = {self.job_id} and data_type in (1,2)
                    group by PseudoPolicyID;
                    insert into pat_policy 
                    select job_id,OriginalPolicyID,ACCGRPID,a.PseudoPolicyID,PolRetainedLimit,
                        PolLimit,
                        (case when round(PolRetainedLimit - PolLimit * PolParticipation, 1) <= 2
                                then PolRetainedLimit / PolLimit
                            else PolParticipation end) as PolParticipation,
                        PolRetention,
                        PolPremium,
                        (case when PolLimit is null or PolRetention is null or PolRetainedLimit is null or 
                                    PolParticipation is null or PolPremium is null  
                                or PolLimit < 0 or PolRetention < 0 or PolRetainedLimit < 0 or
                                    PolParticipation < 0 or PolPremium < 0 then {PatFlag.FlagPolNA}
                            else 0 end) 
                            + (case when round(PolParticipation, 2) > 1 then {PatFlag.FlagPolParticipation}
                                else 0 end)
                            + (case when round(PolRetainedLimit - PolLimit * PolParticipation, 1) > 2 
                                        then {PatFlag.FlagPolLimitParticipation} else 0 end) 
                            as flag, 
                        0 as data_type
                    from pat_policy a                     
                        join #pol_active b on a.PseudoPolicyID = b.PseudoPolicyID and a.data_type = b.data_type
                    where a.job_id = {self.job_id};
                    drop table #pol_active;""")  
            cur.commit()

            # Policy record duplications
            cur.execute(f"""select PseudoPolicyID, {PatFlag.FlagPolDupe} as flag
                                into #tmp_f
                            from pat_policy 
                            where job_id = {self.job_id} and data_type = 0
                            group by PseudoPolicyID having count(*) > 1;
                            update a set a.flag=a.flag + b.flag
                            from pat_policy a join #tmp_f b
                                on a.PseudoPolicyID = b.PseudoPolicyID
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""") 
            cur.commit()

    def __check_location(self, conn):
        with conn.cursor() as cur:
            cur.execute(f"""delete from pat_location where job_id = {self.job_id} and data_type = 0""")
            cur.commit()

            cur.execute(f"""select min(PSOLD_RG) as min_rg, max(PSOLD_RG) as max_rg from psold_mapping""")
            row = cur.fetchone()
            min_psold_rg, max_psold_rg = row

            cur.execute(f"""select PseudoPolicyID, max(data_type) as data_type
                        into #pol_active 
                    from pat_location where job_id = {self.job_id} and data_type in (1,2)
                    group by PseudoPolicyID;
                    insert into pat_location 
                    select job_id, a.PseudoPolicyID, LocationIDStack, occupancy_scheme, 
                        occupancy_code, AOI, 
                        (case when c.PSOLD_RG is null then {self.default_region} else c.PSOLD_RG end) as RatingGroup, 
                        (case when AOI is null or AOI < 0 then {PatFlag.FlagLocNA} else 0 end)
                        + (case when (c.PSOLD_RG is null and {self.default_region} = 0) or c.PSOLD_RG < {min_psold_rg} or c.PSOLD_RG > {max_psold_rg} 
                            then {PatFlag.FlagLocRG} else 0 end) as flag,  
                        0 data_type
                    from pat_location a                     
                        join #pol_active b on a.PseudoPolicyID = b.PseudoPolicyID and a.data_type = b.data_type
                        left join psold_mapping c on a.occupancy_scheme = c.OCCSCHEME and a.occupancy_code =  c.OCCTYPE
                    where a.job_id = {self.job_id};
                    drop table #pol_active""")  
            cur.commit()

             # Apply rule 
            if self.valid_rules & ValidRule.ValidAoi:
                cur.execute(f"""select LocationIDStack, max(aoi) as max_aoi
                                into #tmp_f
                            from pat_location
                            where job_id = {self.job_id} and data_type = 0 and LocationIDStack is not null
                            group by LocationIDStack
                            having count(distinct round(aoi, 1)) > 1;
                            update a set a.aoi = b.max_aoi
                            from pat_location a join #tmp_f b
                                on a.LocationIDStack = b.LocationIDStack
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""")
                cur.commit()  

            # Location record duplications
            cur.execute(f"""select PseudoPolicyID, {PatFlag.FlagLocDupe} as flag
                                into #tmp_f
                            from pat_location 
                            where job_id = {self.job_id} and data_type = 0
                            group by PseudoPolicyID having count(*) > 1;
                            update a set a.flag = a.flag + b.flag
                            from pat_location a join #tmp_f b
                                on a.PseudoPolicyID = b.PseudoPolicyID
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""") 
            cur.commit()
            
            # duplicate LocationIDStack
            cur.execute(f"""select LocationIDStack, {PatFlag.FlagLocIDDupe} as flag
                                into #tmp_f
                            from pat_location
                            where job_id = {self.job_id} and data_type = 0 and LocationIDStack is not null
                            group by LocationIDStack
                            having count(distinct round(aoi, 1)) > 1;
                            update a set a.flag = a.flag + b.flag
                            from pat_location a join #tmp_f b
                                on a.LocationIDStack = b.LocationIDStack
                            where a.job_id = {self.job_id} and data_type = 0;
                            drop table #tmp_f;""")

            cur.commit()

    def __check_facultative(self, conn):
        with conn.cursor() as cur:
            cur.execute(f"""delete from pat_facultative where job_id = {self.job_id} and data_type = 0""")
            cur.commit()

            cur.execute(f"""select FacKey, max(data_type) as data_type
                        into #fac_active 
                    from pat_facultative where job_id = {self.job_id} and data_type in (1,2)
                    group by FacKey;
                    insert into pat_facultative 
                    select job_id, PseudoPolicyID, FacLimit, FacAttachment, FacCeded, a.FacKey,
                        (case when FacLimit is null or FacAttachment is null or FacCeded is null 
                            or FacLimit < 0 or FacAttachment < 0 or FacCeded < 0 
                            then {PatFlag.FlagFacNA} else 0 end) as flag,  
                        0 data_type
                    from pat_facultative a                     
                        join #fac_active b on a.FacKey = b.FacKey and a.data_type = b.data_type
                    where a.job_id = {self.job_id};
                    drop table #fac_active""")  
            cur.commit()

    def __cross_tab_check(self, conn):
        with conn.cursor() as cur:
            # Policies with no Locations
            cur.execute(f"""update a set a.flag = a.flag | {PatFlag.FlagPolOrphan}
                from pat_policy a
                    left join (select distinct PseudoPolicyID from pat_location where job_id = {self.job_id} and data_type = 0) b 
                        on a.PseudoPolicyID = b.PseudoPolicyID
                where a.job_id = {self.job_id} and a.data_type = 0 and b.PseudoPolicyID is null""")
            cur.commit()

            # Location records with no policy
            cur.execute(f"""update a set a.flag = a.flag | {PatFlag.FlagLocOrphan}
                from pat_location a 
                    left join (select distinct PseudoPolicyID from pat_policy where job_id = {self.job_id} and data_type = 0) b
                        on a.PseudoPolicyID = b.PseudoPolicyID
                where a.job_id = {self.job_id} and a.data_type = 0 and b.PseudoPolicyID is null""")
            cur.commit()

            # Orphan Fac Records
            cur.execute(f"""update a set a.flag = a.flag | {PatFlag.FlagFacOrphan}
                from pat_facultative a 
                    left join (select distinct PseudoPolicyID from pat_policy where job_id = {self.job_id} and data_type = 0) b
                        on a.PseudoPolicyID = b.PseudoPolicyID
                where a.job_id = {self.job_id} and a.data_type = 0 and b.PseudoPolicyID is null""")
            cur.commit()
                    
            # FacNet combined specific checks
            f = PatFlag.FlagPolNA | PatFlag.FlagFacNA
            cur.execute(f"""with cte as (
                    select a.PolParticipation, a.PolRetention,
                        PolLimit + PolRetention as PolTopLine,
                        (case when b.FacKey is null then 0 else b.FacLimit end) / PolParticipation as FacGupLimit,
                        case when b.FacKey is null then 0 else b.FacAttachment end as FacAttachment,
                        b.FacKey
                    from pat_policy a 
                        left join pat_facultative b on a.job_id = b.job_id and a.data_type = b.data_type
                            and a.PseudoPolicyID = b.PseudoPolicyID
                    where a.job_id = {self.job_id} and a.data_type = 0
                        and (a.flag & {f.value}) = 0 and (b.flag & {f.value}) = 0
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
                update a set a.flag = a.flag | {PatFlag.FlagFacOverexposed}
                from pat_facultative a
                    join #facover b on a.FacKey = b.FacKey
                where job_id = {self.job_id} and data_type = 0;
                drop table #facover""")
            cur.commit()
    
    def __need_correction(self):
        with pyodbc.connect(self.job_conn) as conn:
            for t in ['pat_policy','pat_location', 'pat_facultative']:
                df = pd.read_sql_query(f"""select count(*) as n from [{t}] where job_id = {self.job_id} 
                        and data_type = 0 and flag != 0""", conn)
                if df is not None and len(df)>0 and df.n[0] > 0: 
                    return True
            
        return False
       
    def __net_of_fac(self):
        with pyodbc.connect(self.job_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""select distinct a.PseudoPolicyID into #pol1
                        from pat_policy a
                            join pat_location b on a.job_id = b.job_id and a.data_type = b.data_type and a.PseudoPolicyID =b.PseudoPolicyID
                        where a.job_id = {self.job_id} and a.data_type = 0 and a.flag = 0 and b.flag = 0;
                        select distinct PseudoPolicyID into #pol2 from pat_facultative 
                        where job_id = {self.job_id} and data_type = 0 and flag != 0;
                    with good_p as (
                        select #pol1.PseudoPolicyID 
                        from #pol1 
                            left join #pol2 on #pol1.PseudoPolicyID = #pol2.PseudoPolicyID 
                        where #pol2.PseudoPolicyID is null
                    )
                    select OriginalPolicyID, ACCGRPID, a.PseudoPolicyID, PolRetainedLimit, PolLimit, 
                            case when PolParticipation > 1 then 1 else PolParticipation end as PolParticipation, 
                            PolRetention, PolPremium,
                            PolLimit + PolRetention as PolTopLine
                        into #dfPolUse
                        from pat_policy a 
                            join good_p b on a.PseudoPolicyID = b.PseudoPolicyID 
                        where a.job_id = {self.job_id} and a.data_type = 0""")
                cur.commit()

                cur.execute(f"""select a.PseudoPolicyID, PolRetention,PolTopLine, FacCeded,
                        FacLimit / PolParticipation as FacGupLimit,
                        FacAttachment / PolParticipation + PolRetention as FacGupAttachment,
                        (FacLimit + FacAttachment) / PolParticipation + PolRetention as FacGupTopLine
                    into #dfPolFac
                    from #dfPolUse a 
                        join pat_facultative b on a.PseudoPolicyID = b.PseudoPolicyID 
                    where b.job_id = {self.job_id} and b.data_type = 0""")
                cur.commit()

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
                        (select a.*, b.OriginalPolicyID, b.ACCGRPID, b.PolParticipation as Participation, b.PolPremium, 
                            case when c.FacCeded is null then 0 else c.FacCeded end as Ceded
                        from cte3 a
                            Left Join #dfPolUse b ON a.PseudoPolicyID = b.PseudoPolicyID
                            Left Join #dfPolFac c ON a.PseudoPolicyID = c.PseudoPolicyID 
                                and a.LayerLow >= c.FacGupAttachment and a.LayerHigh <= FacGupTopLine
                    )
                    select OriginalPolicyID, ACCGRPID,PseudoPolicyID,LayerLow,LayerHigh,Participation,max(PolPremium) as PolPremium,
                        sum(case when Ceded is null then 0 else Ceded end) as Ceded,
                        row_number() OVER (PARTITION BY PseudoPolicyID ORDER BY LayerLow) as LayerID
                        into #dfLayers
                    from cte4
                    group by PseudoPolicyID,OriginalPolicyID, ACCGRPID, LayerLow,LayerHigh,Participation""")
                cur.commit()

                cur.execute(f"""select distinct PseudoPolicyID into #ceded100 
                                from #dfLayers where round(Ceded, 4) >1""")
                cur.commit()
                if self.valid_rules & ValidRule.ValidFac100:
                    cur.execute(f"""update a set a.Ceded = 1
                            from #dfLayers a 
                                join #ceded100 b on a.PseudoPolicyID = b.PseudoPolicyID""")
                    cur.commit()
                    cur.execute("delete from #ceded100")
                    cur.commit()
                else:
                    cur.execute(f"""update a set a.flag = a.flag | {PatFlag.FlagCeded100}
                            from pat_facultative a 
                                join #ceded100 b on a.PseudoPolicyID = b.PseudoPolicyID
                            where job_id = {self.job_id} and data_type = 0;""")
                    cur.commit()
            
            #? why old R code have to include all participation = 0 layers?
            df_facnet = pd.read_sql_query(f"""select OriginalPolicyID, ACCGRPID, a.PseudoPolicyID, LayerID as PseudoLayerID,
                    LayerHigh - LayerLow as Limit, LayerLow as Retention, 
                    PolPremium as OriginalPremium, 
                    case when Ceded >= 1 then 0 else Participation * (1 - Ceded) end as Participation,
                    b.AOI, b.LocationIDStack, b.RatingGroup
                from #dfLayers a
                    left join pat_location b on a.PseudoPolicyID = b.PseudoPolicyID
                    left join #ceded100 c on a.PseudoPolicyID = c.PseudoPolicyID
                where b.job_id ={self.job_id} and b.data_type = 0 
                    and c.PseudoPolicyID is null
                    and case when Ceded >= 1 then 0 else Participation * (1 - Ceded) end > 1e-6 
                order by a.PseudoPolicyID, LayerID, LayerLow, LayerHigh""",conn)

            with conn.cursor() as cur:
                cur.execute(f"""drop table #pol1;
                        drop table #pol2;
                        drop table #dfPolUse;
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
                    "RatingGroup", "OriginalPolicyID", "ACCGRPID", "PseudoPolicyID", "PseudoLayerID", "PolLAS", "DedLAS"]] \
                .sort_values(by=['OriginalPolicyID', 'ACCGRPID', 'PseudoPolicyID', 'PseudoLayerID'])

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


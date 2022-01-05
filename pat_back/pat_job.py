import os
import json
import uuid  
import logging
from datetime import datetime

import numpy as np
import pandas as pd

import pyodbc
from bcpandas import SqlCreds, to_sql

from .settings import AppSettings
from .pat_flag import PatFlag,ValidRule

class PatJob:
    """Class to represent a PAT analysis"""

    peril_table = {1: "eqdet", 2: "hudet", 3: "todet",
                   4: "fldet", 5: "frdet", 6: "trdet"}
    job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    def __init__(self, job_id):
        """Class to represent a PAT analysis"""

        ## Need to make sure the code for covg, subgrp


        self.job_id = 0
        self.data_extracted = False
        self.valid_rules = ValidRule(0)
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

        if 'valid_rules' in self.para and self.para['valid_rules']:
            self.valid_rules = ValidRule(self.para['valid_rules'])
        if 'default_region' in self.para and self.para['default_region']:
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
        self.loss_ratio = float(self.para['loss_alae_ratio'])
        self.dtAveAccDate = datetime.strptime(self.para['average_accident_date'], '%m/%d/%Y')
        self.gdtPSTrendFrom = datetime(2015,12,31)
        self.gdPSTrendFactor = float(self.para['trend_factor'])

        # self.dSubjPrem =1e6 #subject_premium
        self.gdReinsuranceLimit = 1000000 # global reinsurance limit
        self.gdReinsuranceRetention = 1000000 # global reinsurance retention

    def __update_status(self, st):
        tm = ''
        if st == 'started': 
            tm +=f", start_time = '{datetime.utcnow().isoformat()}'"
        elif st == 'finished': 
            tm +=f", finish_time = '{datetime.utcnow().isoformat()}'"
        
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = '{st.replace("'","''")}'
                {tm}           
                where job_id = {self.job_id}""")
            cur.commit()

    def __check_stop(self, stop_cb):
        if stop_cb and stop_cb():
            self.logger.warning("User cancelled the analysis")
            self.__update_status('cancelled')
            return True

    def run(self, stop_cb = None):
        self.logger.info("Import data...")
        self.__update_status("started")

        if not self.data_extracted:
            self.__update_status("extracting_data")

            if "ref_analysis" in self.para and self.para['ref_analysis'] > 0:
                self.data_extracted = self.__extract_ref_data(self.para['ref_analysis'])
            
            if not self.data_extracted:
                self.data_extracted = self.__extract_edm_rdm()
            if self.data_extracted:
                with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
                    cur.execute(f"""update pat_job set data_extracted = 1 where job_id = {self.job_id}""")
                    cur.commit()
                self.logger.info("Import data...OK")
            else:
                self.logger.info("Import data...Error")
                self.__update_status("error")
                return
        if stop_cb and self.__check_stop(stop_cb): return

        self.__update_status('checking_data')
        self.logger.debug("Check data...")
        self.__check_pseudo_policy()
        self.__check_facultative()
        self.logger.debug("Check data...OK")
        if stop_cb and self.__check_stop(stop_cb): return

        if self.__need_correction():
            if (self.valid_rules & ValidRule.ValidContinue) ==0:
                self.logger.error("Need to correct data then run again")
                self.__update_status("error")
                return
            else:
                self.logger.warning("Skip erroneous data and continue (item removed)")
        if stop_cb and self.__check_stop(stop_cb): return
        
        # start calculation
        self.__update_status("net_of_fac")
        self.logger.info("Create the net of FAC layer stack ...")
        df_facnet = self.__net_of_fac()
        if len(df_facnet)<=0:
            self.logger.warning("Nothing to allocate! Finished.")
            self.__update_status("finished")
            return 
        self.logger.info(f"Create the net of FAC layer stack...OK ({len(df_facnet)})")
        if stop_cb and self.__check_stop(stop_cb): return

        self.__update_status("allocating")
        self.logger.info("Allocate premium with PSOLD...")
        df_pat = self.__allocate_with_psold(df_facnet)
        self.logger.info("Allocate premium with PSOLD...OK")
        if stop_cb and self.__check_stop(stop_cb): return

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

    def __extract_ref_data(self, ref_job):
         with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            for t in ['pat_pseudo_policy','pat_facultative']:
                cur.execute(f"""delete from {t} where job_id = {self.job_id} and data_type in (0, 1)""")
                cur.commit()

            cur.execute(f"""insert into pat_pseudo_policy
                    select {self.job_id} as job_id, 1 as data_type,
                        PseudoPolicyID, ACCGRPID, OriginalPolicyID, PolRetainedLimit, PolLimit, 
                        PolParticipation, PolRetention, PolPremium, LocationIDStack, 
                        occupancy_scheme, occupancy_code, Building, Contents, BI, AOI, 
                        0 RatingGroup,
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
            
            self.logger.debug("Extract policy location table...")
            n = self.__extract_pseudo_policy(conn, suffix)
            if n<=0: 
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
                from [{self.para['edm']}]..policy a
                    join [{self.para['edm']}]..portacct b on a.accgrpid = b.accgrpid
                where b.portinfoid = {self.para['portinfoid']} and policytype = {self.para['perilid']}""", conn)) <= 0:
            return 'PerilID is not valid!'

        # Check that portfolio ID is valid
        if len(pd.read_sql_query(f"""select top 1 1
                from [{self.para['edm']}]..loccvg 
                    join [{self.para['edm']}]..property on loccvg.locid = property.locid 
                    join [{self.para['edm']}]..portacct on property.accgrpid = portacct.accgrpid 
                where loccvg.peril = {self.para['perilid']} and portacct.portinfoid = {self.para['portinfoid']}""", conn)) <= 0:
            return 'portfolio ID is not valid!'

        # Check that analysis ID is valid
        if 'rdm' in self.para and self.para['rdm'] and 'analysisid' in self.para and self.para['analysisid']:
            if len(pd.read_sql_query(f"""select top 1 1
            FROM [{self.para['edm']}]..loccvg 
                join [{self.para['edm']}]..property on loccvg.locid = property.locid 
                join [{self.para['edm']}]..portacct on property.accgrpid = portacct.accgrpid 
                join [{self.para['edm']}]..policy on property.accgrpid = policy.accgrpid 
                join [{self.para['rdm']}]..rdm_policy on policy.policyid = rdm_policy.id 
            where portacct.portinfoid = {self.para['portinfoid']} and loccvg.peril = {self.para['perilid']}
            and rdm_policy.ANLSID = {self.para['analysisid']}""", conn)) <= 0:
                print('portfolio ID is not valid!')

        return 'ok'
    
    def __create_temp_tables(self, conn, suffix:str):
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
                where pa.portinfoid = {self.para['portinfoid']} and loccvg.peril = {self.para['perilid']}
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
                    where p.policytype = {self.para['perilid']}
                        and pa.portinfoid = {self.para['portinfoid']}""")
            cur.commit()

            # location policy
            if 'rdm' in self.para and self.para['rdm'] and 'analysisid' in self.para and self.para['analysisid']:
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
                    from {self.para['rdm']}..rdm_policy a 
                        join {self.para['rdm']}..rdm_eventareadetails b on a.eventid = b.eventid 
                        join #policy_standard_{suffix} as p on a.id = p.policyid
                    where anlsid = {self.para['analysisid']}
                    group by a.id, res1value
                    having sum(case when perspcode = 'GU' then perspvalue else 0 end) * 
                        {self.AddtlCovg + 1} > max(p.undcovamt) )

                    select a.locid, b.accgrpid, a.policyid, b.policynum,
                        b.blanlimamt as orig_blanlim,
                        b.partof as orig_partof,
                        b.undcovamt as orig_undcovamt,
                        b.blanlimamt / b.partof as orig_participation,
                        a.grounduploss, clientloss, undcovloss, overlimitloss, otherinsurerloss, grossloss,
                        case when grossloss + otherinsurerloss <= 0 then b.blanlimamt 
                            when overlimitloss > 1 then (b.blanlimamt / b.partof) * (grossloss + otherinsurerloss) 
                            else b.blanlimamt end as rev_blanlimamt,
                        case when grossloss + otherinsurerloss <= 0 then b.partof 
                            when overlimitloss > 1 then grossloss + otherinsurerloss 
                            else b.partof end as rev_partof,	
                        case when b.partof <= 0 then 1 else b.blanlimamt / b.partof end as participation,
                        clientloss as deductible,
                        case when cond.conditiontype = 2 then undcovloss else b.undcovamt end as undcovamt, 
                        grounduploss as origtiv,
                        case when grossloss + otherinsurerloss = 0 then clientloss + undcovloss else grounduploss end as rev_tiv,
                        case when grounduploss < d.tiv then grounduploss * (bldgval/d.tiv) else bldgval end as bldgval,
                        case when grounduploss < d.tiv then grounduploss * (contval/d.tiv) else contval end as contval,
                        case when grounduploss < d.tiv then grounduploss * (bival/d.tiv) else bival end as bival,
                        d.occupancy_scheme, d.occupancy_code, blanpreamt
                        into #sqlpremalloc_{suffix}
                    from locpoltotals a 
                        join #policy_standard_{suffix} b on a.policyid = b.policyid
                        join #location_standard_{suffix} as d on a.locid = d.locid 
                        left join #policy_loc_conditions_{suffix} as cond on a.policyid = cond.policyid 
                            and a.locid = cond.locid and cond.conditiontype != 1
                    order by b.accgrpid, a.locid, a.undcovloss;
                    
                    drop table #policy_standard_{suffix};
                    drop table #location_standard_{suffix};
                    drop table #policy_loc_conditions_{suffix}""")
                cur.commit()

            else: # without spider
                cur.execute(f"""select l.locid, p.accgrpid, 
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
                        l.occupancy_scheme, l.occupancy_code,p.blanpreamt
                        into #sqlpremalloc_{suffix}
                        from #policy_standard_{suffix} as p
                            join #location_standard_{suffix} as l on p.accgrpid = l.accgrpid
                        where l.tiv * {self.AddtlCovg + 1} > p.undcovamt;
                        
                        drop table #policy_standard_{suffix};
                        drop table #location_standard_{suffix};""")
                cur.commit()

            cur.execute(f"""update #sqlpremalloc_{suffix}
                set bldgval = rev_tiv * (bldgval / origtiv), 
                    contval = rev_tiv * (contval / origtiv), 
                    bival = rev_tiv * (bival / origtiv)
                where origtiv > rev_tiv + 1""")
            cur.commit()        

    def __extract_pseudo_policy(self, conn, suffix:str):
        retained_lmt = "(rev_partof - deductible) * rev_blanlimamt / rev_partof" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "rev_blanlimamt"
        gross_lmt = "rev_partof - deductible" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "rev_partof"
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
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_polloc, "pat_pseudo_policy", creds, index=False, if_exists='append')

        
        return len(df_polloc)

    def __extract_facultative(self, conn, suffix:str):
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
            creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
            to_sql(df_fac, "pat_facultative", creds, index=False, if_exists='append')

        return len(df_fac)
            
    def __check_pseudo_policy(self):
        with pyodbc.connect(self.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""delete from pat_pseudo_policy where job_id = {self.job_id} and data_type = 0""")
            cur.commit()

            cur.execute(f"""select min(PSOLD_RG) as min_rg, max(PSOLD_RG) as max_rg from psold_mapping""")
            row = cur.fetchone()
            min_psold_rg, max_psold_rg = row

            aoi = "COALESCE(b.Building, a.Building) + COALESCE(b.Contents, a.Contents)" + \
                    " + COALESCE(b.BI, a.BI)" if self.para['coverage'] == "Building + Contents + Time Element" else ""

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
                                COALESCE(b.RatingGroup, c.PSOLD_RG, {self.default_region}) as RatingGroup,
                                
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
                                        then {PatFlag.FlagPolNA}
                                        else 0 end) 
                                    + (case when round(COALESCE(b.PolParticipation, a.PolParticipation), 2) > 1 then {PatFlag.FlagPolParticipation}
                                        else 0 end)
                                    + (case when round(
                                            COALESCE(b.PolRetainedLimit, a.PolRetainedLimit) 
                                            - COALESCE(b.PolLimit, a.PolLimit) 
                                            * COALESCE(b.PolParticipation, a.PolParticipation), 1) > 2 
                                        then {PatFlag.FlagPolLimitParticipation} else 0 end) 

                                    + (case when COALESCE(b.AOI, {aoi}) < 0 then {PatFlag.FlagLocNA} else 0 end)
                                    + (case when COALESCE(b.RatingGroup, c.PSOLD_RG, {self.default_region}) < {min_psold_rg} 
                                        or COALESCE(b.RatingGroup, c.PSOLD_RG, {self.default_region}) > {max_psold_rg}
                                        then {PatFlag.FlagLocRG} else 0 end)
                                        as flag                                
                            from pat_pseudo_policy a 
                                left join (select * from pat_pseudo_policy where job_id = {self.job_id} and data_type = 2) b 
                                    on a.PseudoPolicyID = b.PseudoPolicyID
                                left join psold_mapping c on 
                                    COALESCE(b.occupancy_scheme, a.occupancy_scheme) = c.OCCSCHEME 
                                    and COALESCE(b.occupancy_code, a.occupancy_code) =  c.OCCTYPE
                            where a.job_id = {self.job_id} and a.data_type = 1;""")  
            cur.commit()

            # Apply rule 
            if self.valid_rules & ValidRule.ValidAoi:
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
            cur.execute(f"""select PseudoPolicyID, {PatFlag.FlagPolLocDupe} as flag
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
            cur.execute(f"""select LocationIDStack, {PatFlag.FlagLocIDDupe} as flag
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
            cur.execute(f"""delete from pat_facultative where job_id = {self.job_id} and data_type = 0""")
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
                                    then {PatFlag.FlagFacNA} else 0 end) as flag                                
                            from pat_facultative a 
                                left join (select * from pat_facultative where job_id = {self.job_id} and data_type = 2) b 
                                    on a.PseudoPolicyID = b.PseudoPolicyID and a.FacKey = b.FacKey
                            where a.job_id = {self.job_id} and a.data_type = 1;""")  
            cur.commit()
   
           # Orphan Fac Records
            cur.execute(f"""update a set a.flag = a.flag | {PatFlag.FlagFacOrphan}
                from pat_facultative a 
                    left join (select distinct PseudoPolicyID from pat_pseudo_policy where job_id = {self.job_id} and data_type = 0) b
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
                update a set a.flag = a.flag | {PatFlag.FlagFacOverexposed}
                from pat_facultative a
                    join #facover b on a.FacKey = b.FacKey
                where job_id = {self.job_id} and data_type = 0;
                drop table #facover""")
            cur.commit()

            # Fac/Pol exceed 100%
            self.__create_tmp_layers(cur) # create #dfLayers
            cur.execute(f"""update a set a.flag = a.flag | {PatFlag.FlagCeded100}
                        from pat_facultative a 
                            join (select distinct PseudoPolicyID 
                                from #dfLayers where round(Ceded, 4) > 1
                                ) b on a.PseudoPolicyID = b.PseudoPolicyID
                        where job_id = {self.job_id} and data_type = 0;
                        drop table #dfLayers""")
            cur.commit()
    
    def __create_tmp_layers(self, cur):
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
                    (select a.*, b.PolParticipation as Participation, b.PolPremium, 
                        case when c.FacCeded is null then 0 else c.FacCeded end as Ceded
                    from cte3 a
                        Left Join #dfPolUse b ON a.PseudoPolicyID = b.PseudoPolicyID
                        Left Join #dfPolFac c ON a.PseudoPolicyID = c.PseudoPolicyID 
                            and a.LayerLow >= c.FacGupAttachment and a.LayerHigh <= FacGupTopLine
                )
                select PseudoPolicyID,LayerLow,LayerHigh,Participation,max(PolPremium) as PolPremium,
                    sum(case when Ceded is null then 0 else Ceded end) as Ceded,
                    row_number() OVER (PARTITION BY PseudoPolicyID ORDER BY LayerLow) as LayerID
                    into #dfLayers
                from cte4
                group by PseudoPolicyID, LayerLow,LayerHigh,Participation;
                drop table #dfPolUse;
                drop table #dfPolFac;""")
            cur.commit()

            # Apply rule
            if self.valid_rules & ValidRule.ValidFac100:
                cur.execute("update #dfLayers set Ceded = 1 where round(Ceded, 4) > 1")
                cur.commit()

    def __need_correction(self):
        with pyodbc.connect(self.job_conn) as conn:
            for t in ['pat_pseudo_policy', 'pat_facultative']:
                df = pd.read_sql_query(f"""select count(*) as n from [{t}] where job_id = {self.job_id} 
                        and data_type = 0 and flag != 0""", conn)
                if df is not None and len(df)>0 and df.n[0] > 0: 
                    return True
            
        return False
       
    def __net_of_fac(self):
        with pyodbc.connect(self.job_conn) as conn:
            with conn.cursor() as cur:
                self.__create_tmp_layers(cur)

                # delete those can be regarded as 1
                cur.execute("delete from #dfLayers where Ceded - 1 > -1e-6")
                cur.commit()
                
            #? why old R code have to include all participation = 0 layers?
            df_facnet = pd.read_sql_query(f"""select b.OriginalPolicyID, a.PseudoPolicyID, a.LayerID as PseudoLayerID,
                    a.LayerHigh - a.LayerLow as Limit, a.LayerLow as Retention, 
                    b.PolPremium as OriginalPremium, 
                    Participation * (case when Ceded <= 0 then 1 when Ceded > 1 then 0 else 1-Ceded end) as Participation,
                    b.AOI, b.LocationIDStack, b.RatingGroup
                from #dfLayers a
                    join pat_pseudo_policy b on a.PseudoPolicyID = b.PseudoPolicyID 
                        and b.job_id ={self.job_id} and b.data_type = 0 
                where Participation * (case when Ceded <= 0 then 1 when Ceded > 1 then 0 else 1-Ceded end) > 1e-6
                order by a.PseudoPolicyID, LayerID, LayerLow, LayerHigh;
                drop table #dfLayers;""",conn)

            return df_facnet
    
    def __allocate_with_psold(self, df_facnet):     
        dfFacNetLast = df_facnet.groupby('PseudoPolicyID').agg({'PseudoLayerID':max}).rename(columns={'PseudoLayerID':'LayerPosition'})
        # dfLocation2
        dfLoc = df_facnet.merge(dfFacNetLast, left_on='PseudoPolicyID', right_index=True)

        with pyodbc.connect(self.job_conn) as conn:
            tableWgts = pd.read_sql_query(f"""select PremiumWeight/sum(PremiumWeight) over() as PremiumPercent 
                                            from psold_weight order by rg""", conn)
            AOI_split = pd.read_sql_query(f"""select * from psold_aoi order by AOI""", conn).AOI.to_numpy() 
            guNewPSTable = pd.read_sql_query(f"""select * from psold_gu_2016 order by COVG, SUBGRP, RG, EG""", conn)
        
        gdMu = [1000 * (np.sqrt(10)**(i-1)) for i in range(1,12)]
        dfWeights = self.__calc_weights(tableWgts, guNewPSTable, gdMu, AOI_split)

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
        dfLoc['Premium'] = (dfLoc.PolLAS.fillna(value=0) - dfLoc.DedLAS.fillna(value=0)) \
            * self.loss_ratio * dfLoc.Participation
        dfLoc['Premium'] = dfLoc.OriginalPremium * dfLoc.Premium \
            / dfLoc['Premium'].groupby(dfLoc.OriginalPolicyID).transform('sum')
        
        # dfLoc['sumLAS'] = (dfLoc.PolLAS-dfLoc.DedLAS)*self.loss_ratio*dfLoc.Participation
        # dfLoc['sumLAS'] = dfLoc['sumLAS'].fillna(value=0)
        # dfLoc['sumLAS'] = dfLoc['sumLAS'].groupby(dfLoc.OriginalPolicyID).transform('sum')
        # dfLoc['premLAS'] = dfLoc.OriginalPremium / dfLoc.sumLAS
        # dfLoc['Premium'] = (dfLoc.PolLAS - dfLoc.DedLAS) * self.loss_ratio * dfLoc.Participation * dfLoc.premLAS
        # dfLoc['EffPrem'] = (dfLoc.Premium * self.dSubjPrem/ dfLoc.Premium.sum())
        # dfLoc['ExpectedLossCount'] = ((np.minimum(dfLoc.RetStepLAS,dfLoc.PolLAS)-np.minimum(dfLoc.PolLAS, dfLoc.RetLAS)) \
        #     /(dfLoc.PolLAS - dfLoc.DedLAS) * self.loss_ratio*dfLoc.EffPrem )/.01

        # Final
        df_pat = dfLoc[["PseudoPolicyID", "PseudoLayerID","Limit", "Retention", "Participation", "Premium", "PolLAS", "DedLAS"]]
        
        return df_pat
      
    def __calc_weights(self, tableWgts, guNewPSTable, gdMu, AOI_split):        
        dfWeights =pd.DataFrame(columns=['AOI',*[f'G{i}' for i in range(12)],*[f'R{i}' for i in range(12)]])
            
        dAdjFactor = 1 #?   
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
                    ) / np.sum(a), axis = 1) * guNewPSTable_sub2.OCC * tableWgts.PremiumPercent)

            # GROSS CALCULATIONS
            dSumGrossPrem = np.sum(guNewPSTable_sub2[[f"G{i+1}" for i in range(11)]].apply(
                lambda a: (np.sum(a * dAdjFactor * gdMu * (1-np.exp(
                        (-1 * AOI_split[iAOI] * (1 + self.AddtlCovg)) /
                        (dAdjFactor * gdMu))))
                    ) / np.sum(a), axis = 1) * guNewPSTable_sub2.AOCC * tableWgts.PremiumPercent)
            
            
            # If the rating group is selected in the premium weight portion of the user input table, 
            # then calc the relative frequency.
            
            # NET CALCULATIONS
            dtmpOccCnt = tableWgts.PremiumPercent * dSumNetPrem \
                / guNewPSTable_sub2[[f"R{i+1}" for i in range(11)]].apply(
                    lambda a: np.sum(a * dAdjFactor * gdMu *
                        (1-np.exp((-1*AOI_split[iAOI] * (1+self.AddtlCovg)) / (dAdjFactor * gdMu)))) 
                        / sum(a), axis =1)
            uNet_dOccCnt = np.sum(dtmpOccCnt)
            uNet_dW = np.dot(
                guNewPSTable_sub2[[f"R{i+1}" for i in range(11)]].T,
                dtmpOccCnt
            )

            # GROSS CALCULATIONS
            dtmpOccCnt = tableWgts.PremiumPercent * dSumGrossPrem \
                / guNewPSTable_sub2[[f"G{i+1}" for i in range(11)]].apply(
                    lambda a: np.sum(a * dAdjFactor * gdMu *
                        (1-np.exp((-1*AOI_split[iAOI] * (1+self.AddtlCovg)) / (dAdjFactor * gdMu)))) 
                        / sum(a), axis =1)
            uGross_dOccCnt = np.sum(dtmpOccCnt)
            uGross_dW = np.dot(
                guNewPSTable_sub2[[f"G{i+1}" for i in range(11)]].T,
                dtmpOccCnt
            )

            uNet_dW = uNet_dW / uNet_dOccCnt
            uGross_dW = uGross_dW / uGross_dOccCnt

            dfWeights.loc[iAOI-1]=[iAOI,uGross_dOccCnt,*uGross_dW,uNet_dOccCnt,*uNet_dW]

        return dfWeights

    def __calc_detail_info(self, df, AOI_split):
        dTotalPrem = df.OriginalPremium.sum()
        df['AddtlCovg'] = self.AddtlCovg
        df['iCurveSwitch'] = 1
        df.loc[df.Retention.isna(), ['iCurveSwitch']] = 2

        df['dPolDed'] = 0
        df.loc[df.Retention.notna(), ['dPolDed']] = df.Retention / self.dCurrencyAdj

        df['dPolLmt'] = df.Limit / self.dCurrencyAdj
        if self.iDedType == 1:
            df['dPolLmt'] += df['dPolDed'] 

        # df['EffPrem'] = df.OriginalPremium * self.dSubjPrem/dTotalPrem

        df['dTIV'] = 0
        df.loc[df.AOI.notna(), ['dTIV']] = df.AOI / self.dCurrencyAdj
        df.loc[df.AOI.isna(), ['dTIV']] = df.Limit / self.dCurrencyAdj
        if self.iDedType == 1:
            df.loc[df.AOI.isna(), ['dTIV']] += df.dPolDed[df.AOI.isna()]

        df.loc[df.Participation.isna(), ['Participation']] = 1
        df.loc[df.LocationIDStack == '', ['LocationIDStack']] = np.nan

        df['dX'] = np.maximum(df.dPolLmt - df.dPolDed, 0)
        df.loc[df.LocationIDStack.isna(), ['dX']] *= (1+ self.AddtlCovg)
        df['dX']=df.dX + df.dPolDed

        df['dAdjFactor'] = self.dCurrencyAdj if self.dtAveAccDate == 0 \
            else self.dCurrencyAdj * self.gdPSTrendFactor ** ((self.dtAveAccDate - self.gdtPSTrendFrom).days / 365.25)

        df['dTIVAdj'] = df.dTIV / df.dAdjFactor
        df['iAOI'] = np.searchsorted(AOI_split[1:-1],df.dTIVAdj, side='right') + 1

        df['dEffLmt'] = df[["dTIV", "dPolLmt"]].min(axis=1)

        return df

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
        rein = max(min(self.gdReinsuranceRetention * 1e-8, self.gdReinsuranceLimit), 1e-2)

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

                mask = np.logical_and(mask1, np.logical_and(mask2, mask3))
                if any(mask):
                    df.loc[mask, ['w']] = df.merge(dfWeights[['AOI',f'G{i+1}']],
                        left_on='iAOI', right_on='AOI', how='left').loc[mask,f'G{i+1}']

                mask = np.logical_and(mask1, np.logical_and(mask2, ~mask3))
                if any(mask):
                    df.loc[mask, ['w']] = df.merge(dfWeights[['AOI',f'R{i+1}']],
                        left_on='iAOI', right_on='AOI', how='left').loc[mask,f'R{i+1}']

                mask = np.logical_and(mask1, np.logical_and(~mask2, mask3))
                if np.any(mask) :
                    df.loc[mask, ['w']] = df.merge(dfUnique, left_on=['RatingGroup', 'iAOI'], right_on=['RG', 'EG'], how='left').loc[mask,f'G{i+1}']

                mask = np.logical_and(mask1, np.logical_and(~mask2, ~mask3))
                if np.any(mask) :
                    df.loc[mask, ['w']] = df.merge(dfUnique, left_on=['RatingGroup', 'iAOI'], right_on=['RG', 'EG'], how='left').loc[mask, f'R{i+1}']
                df['sum_w'] = df.sum_w + df.w

                df[n] += self.__me_las(df, X, gdMu[i])

            df[n] /= df.sum_w

        df.drop(columns=['sum_w', 'w'], inplace=True)
        
        # df['MaxLoss'] = 1e-10
        # df.loc[df.OriginalPremium > 0, ['MaxLoss']] = np.maximum(np.minimum(
        #     df.Limit*(1+df.AddtlCovg)-df.dPolDed, df.dEffLimRet)-df.dEffRet, 0) * df.Participation
        # df['ExpectedLossCount'] = ((np.minimum(df.RetStepLAS, df.PolLAS) - np.minimum(
        #     df.PolLAS, df.RetLAS)) / (df.PolLAS-df.DedLAS) * self.loss_ratio * df.EffPrem) / rein
        
        # df['PercentExpos'] = 0
        # df.loc[df.OriginalPremium > 0, 'PercentExpos'] = \
        #     (np.maximum(np.minimum(df.PolLAS, df.LimRetLAS)-df.DedLAS, 0) - 
        #     np.maximum(np.minimum(df.PolLAS, df.RetLAS)-df.DedLAS, 0))/(df.PolLAS-df.DedLAS)
        
        return df

    def __me_las(self,df, X, Mu):
        Y = df[['dX']].rename(columns={'dX':'Y'})
        Y['Y'] = 0

        mask = np.logical_and(np.logical_and(X.notna(), X > 0), X <= (1+ df.AddtlCovg) * df.dEffLmt)
        Y.loc[mask,['Y']] = df.dAdjFactor[mask] * Mu * df.w[mask] * (1- np.exp(-X[mask]/(df.dAdjFactor[mask] * Mu)))

        mask = np.logical_and(np.logical_and(X.notna(), X > 0), X > (1+ df.AddtlCovg) * df.dEffLmt)
        Y.loc[mask,['Y']] = df.dAdjFactor[mask] * Mu * df.w[mask] * (1- np.exp(-(1+ df.AddtlCovg[mask]) * df.dEffLmt[mask]/(df.dAdjFactor[mask] * Mu)))

        return Y['Y']


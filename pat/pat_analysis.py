
import os
import math
from enum import Flag
from typing import Final
import numpy as np
import pandas as pd
import pyodbc
from pandasql import sqldf
from datetime import datetime

from .pat_flag import PatFlag

class PatAnalysis:
    """Class to repreet a PAT analysis"""

    peril_table = {1: "eqdet", 2: "hudet", 3: "todet",
                   4: "fldet", 5: "frdet", 6: "trdet"}
    
    def __init__(self, self_para):
        self.para = self_para
        self.df_pol = None
        self.df_loc = None
        self.df_fac = None
        self.df_facnet = None
        self.df_pat = None

        self.conn_str = f'''DRIVER={{SQL Server}};Server={self.para["server"]};Database={self_para["edm_database"]};
            Trusted_Connection=True;MultipleActiveResultSets=true;'''

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
        
        self.dtAveAccDate = datetime.strptime(self.para['average_accident_date'], '%m/%d/%Y')
        self.gdtPSTrendFrom = datetime(2015,12,31)
        self.gdPSTrendFactor = float(self.para['trend_factor'])

        self.gdReinsuranceLimit = 1000000 # global reinsurance limit
        self.gdReinsuranceRetention = 1000000 # global reinsurance retention
        

    def extract_edm_rdm(self):
        with pyodbc.connect(self.conn_str) as conn:
            with pyodbc.connect(self.conn_str) as conn:
                if self.__verify_edm_rdm(conn) != 'ok': return

            self.__create_temp_tables(conn)

            self.__extract_policy(conn)
            self.__extract_location(conn)
            self.__extract_fac(conn)

            self.__delete_temp_tables(conn)

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
                    order by  b.accgrpid, a.locid, a.undcovloss""")
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

            cur.commit()

            cur.execute(f"""update #sqlpremalloc
                set bldgval = rev_tiv * (bldgval / origtiv), 
                    contval = rev_tiv * (contval / origtiv), 
                    bival = rev_tiv * (bival / origtiv)
                where origtiv > rev_tiv + 1""")

    def __extract_policy(self, conn):
        retained_lmt = "(locpol.rev_partof - locpol.deductible) * locpol.rev_blanlimamt / locpol.rev_partof" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "locpol.rev_blanlimamt"
        gross_lmt = "locpol.rev_partof - locpol.deductible" if self.para[
            'deductible_treatment'] == 'Erodes Limit' else "locpol.rev_partof"
        self.df_pol = pd.read_sql_query(f"""select locpol.policyid as OriginalPolicyID, 
                    concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    {retained_lmt} as PolRetainedLimit, 
                    {gross_lmt} as PolLimit, 
                    locpol.participation as PolParticipation, 
                    locpol.deductible + locpol.undcovamt as PolRetention, 
                    policy.blanpreamt as PolPremium
                from #sqlpremalloc as locpol 
                    inner join policy on locpol.policyid = policy.policyid 
                order by locpol.accgrpid, locpol.policyid, locpol.locid""", conn)

        mask = np.round(self.df_pol.PolRetainedLimit -
                        self.df_pol.PolLimit * self.df_pol.PolParticipation, 1) <= 2
        self.df_pol.loc[mask, ['PolParticipation']] = np.divide(
            self.df_pol['PolRetainedLimit'][mask], self.df_pol['PolLimit'][mask])
        self.df_pol['PolLimit'] = np.round(self.df_pol['PolLimit'], 2)
        self.df_pol['PolRetention'] = np.round(self.df_pol['PolRetention'], 2)

        # sorting
        self.df_pol.sort_values(['OriginalPolicyID', 'PseudoPolicyID']).reset_index(drop=True, inplace=True)

    def __extract_location(self, conn):
        aoi = "locpol.bldgval + locpol.contval + locpol.bival" if self.para[
            'coverage'] == "Building + Contents + Time Element" else "locpol.bldgval + locpol.contval"
        self.df_loc = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    locpol.locid as LocationIDStack,
                    {aoi} as AOI, 
                    loc.occscheme as occupancy_scheme, loc.occtype as occupancy_code 
                from #sqlpremalloc as locpol 
                    inner join loc on locpol.locid = loc.locid 
                order by locpol.accgrpid, locpol.policyid, locpol.locid""", conn)

        self.__apply_psold_mapping()

        self.df_loc = self.df_loc[["PseudoPolicyID", "AOI", "LocationIDStack", "psold_rg"]].rename(columns={"psold_rg":"RatingGroup"})

        # convert to numeric if possible
        self.df_loc['LocationIDStack'] = self.df_loc['LocationIDStack'].astype(str)

        # sorting
        self.df_loc.sort_values(['LocationIDStack', 'PseudoPolicyID']).reset_index(drop=True, inplace=True)

    def __apply_psold_mapping(self):
        df_psold = pd.read_csv('data/psold_rg_mapping.csv')
        df_psold.columns = df_psold.columns.str.lower()
        self.max_psold_rg = df_psold.psold_rg.max()

        # Attach PSOLD mapping to location data
        self.df_loc = self.df_loc.merge(df_psold, how='left', left_on=['occupancy_scheme', 'occupancy_code'],
                                        right_on=['occscheme', 'occtype'])

        mask = np.isnan(self.df_loc['psold_rg'])
        if np.any(mask):
            self.df_loc['psold_rg'][mask] = 'Unmapped PSOLD: ' + \
                self.df_loc.occupancy_scheme[mask] + ' ' +\
                self.df_loc.occupancy_code.astype(str)[mask]

    def __extract_fac(self, conn):
        self.df_fac = pd.read_sql_query(f"""select concat(locpol.policyid, '_', locpol.locid) as PseudoPolicyID, 
                    case when excessamt + layeramt > rev_blanlimamt then rev_blanlimamt - excessamt 
                        else layeramt end as FacLimit, 
                    excessamt as FacAttachment, pcntreins/100 as FacCeded
                from #sqlpremalloc as locpol 
                    inner join reinsinf on locpol.policyid = reinsinf.exposureid 
                where reinsinf.exposrtype = 'pol' and excessamt < rev_blanlimamt 
                    and layeramt > 0 and pcntreins > 0 
                order by locpol.accgrpid, locpol.locid, locpol.undcovamt, reinsinf.excessamt, FacLimit, pcntreins""", conn)

        # "pollocid","100pct_fac_limit","fac_attachment","fac_pct_ceded"
        # "PseudoPolicyID", "FacLimit", "FacAttachment", "FacCeded"

        # keep the original key (why?)
        self.df_fac['FacKey'] = np.arange(1, len(self.df_fac)+1, dtype=int)
        self.df_fac['FacLimit'] = np.round(self.df_fac['FacLimit'], 2)

        # sorting
        self.df_fac.sort_values(['PseudoPolicyID', 'FacAttachment', 'FacLimit', 'FacCeded']).reset_index(drop=True, inplace=True)
        
    def __delete_temp_tables(self, conn):
        with conn.cursor() as cur:
            cur.execute("drop table #policy_standard")
            cur.execute("drop table #policy_loc_conditions")
            # cur.execute("drop table #locpoltotals")
            cur.execute("drop table #sqlpremalloc")
            cur.commit()

    def check_data(self):
        self.__check_policy()
        self.__check_fac()
        self.__check_location()

        return True

    def __check_policy(self):
        self.df_pol['status'] = int(0)

        # Duplicate Policies 
        mask = self.df_pol.PseudoPolicyID.duplicated(keep=False)
        PatFlag.FlagPolDupe.set_flag(self.df_pol, mask)

        # Policies with no Locations
        diff = set(self.df_pol.PseudoPolicyID).difference(self.df_loc.PseudoPolicyID)
        if len(diff) > 0:
            df = self.df_pol[["PseudoPolicyID"]].merge(pd.DataFrame({"PseudoPolicyID": np.array(diff), "st":1}), on = 'PseudoPolicyID', how="left")
            mask = ~np.isnan(df.st)
            PatFlag.FlagPolNoLoc.set_flag(self.df_pol, mask)


        # Policies with nonNumeric Fields
        mask = np.sum(np.isnan(self.df_pol[["PolLimit", "PolRetention", "PolRetainedLimit", "PolParticipation", "PolPremium"]]), axis=1) > 0
        PatFlag.FlagPolNA.set_flag(self.df_pol, mask)

        # Policies with negative fields
        mask = np.sum(self.df_pol[["PolLimit", "PolRetention", "PolRetainedLimit",
                      "PolParticipation", "PolPremium"]] < 0, axis=1) > 0
        PatFlag.FlagPolNeg.set_flag(self.df_pol, mask)
        
        # Policies with inconsistant limit/participation alegebra
        mask = np.abs(np.round(self.df_pol.PolRetainedLimit -
                      self.df_pol.PolLimit * self.df_pol.PolParticipation, 1)) > 2
        PatFlag.FlagPolLimitParticipation.set_flag(self.df_pol, mask)
        
        # Policies with excess participation
        mask = self.df_pol.PolParticipation > 1
        PatFlag.FlagPolParticipation.set_flag(self.df_pol, mask)

    def __check_fac(self):
        self.df_fac['status'] = int(0)
        if len(self.df_fac) <= 0: return

        # 
        # Orphan Fac Records
        diff = set(self.df_fac.PseudoPolicyID).difference(self.df_pol.PseudoPolicyID)
        if len(diff) > 0:
            df = self.df_fac[["PseudoPolicyID"]].merge(pd.DataFrame({"PseudoPolicyID": np.array(diff), "st":1}), on = 'PseudoPolicyID', how="left")
            mask = ~np.isnan(df.st)
            PatFlag.FlagFacOrphan.set_flag(self.df_fac, mask)
        
        # Fac records with NA entries
        mask = np.sum(np.isnan(self.df_fac[["FacLimit", "FacAttachment", "FacCeded"]]), axis=1) > 0
        PatFlag.FlagFacNA.set_flag(self.df_fac, mask)

        # Fac records with negative entries
        mask = np.sum(self.df_fac[["FacLimit", "FacAttachment", "FacCeded"]] < 0, axis=1) > 0
        PatFlag.FlagFacNeg.set_flag(self.df_fac, mask)

        # FacNet combined specific checks
        dfPolFac = self.df_pol[['PseudoPolicyID', 'PolLimit', 'PolRetention', 'PolParticipation', 'status']]\
            .merge(self.df_fac[['PseudoPolicyID', 'FacLimit', 'FacAttachment', 'FacKey', 'status']]
            , on = 'PseudoPolicyID',how='left')

        dfPolFac['status'] = dfPolFac['status_x'] & dfPolFac['status_y'].fillna(value=0).astype(int)
        dfPolFac.drop(columns=['status_x','status_y'], inplace=True)

        f = PatFlag.FlagPolNA |PatFlag.FlagPolNeg | PatFlag.FlagFacNA | PatFlag.FlagFacNeg
        dfPolFac = dfPolFac[~(dfPolFac.status & f != 0)].sort_values(by=['PseudoPolicyID','FacAttachment']).reset_index(drop=True)
        mask = np.isnan(dfPolFac.FacKey)
        dfPolFac.at[mask, ['FacLimit','FacAttachment']] = 0

        dfPolFac['PolTopLine'] = dfPolFac.PolLimit + dfPolFac.PolRetention
        dfPolFac['FacGupLimit'] = dfPolFac.FacLimit / dfPolFac.PolParticipation
        dfPolFac['FacGupAttachment'] = dfPolFac.FacAttachment / dfPolFac.PolParticipation \
            + dfPolFac.PolRetention
        dfPolFac['FacGupTopLine'] = dfPolFac.FacGupLimit + dfPolFac.FacGupAttachment

        lst = set(dfPolFac[np.logical_or(dfPolFac.FacGupAttachment - dfPolFac.PolTopLine > 1,
        dfPolFac.FacGupTopLine - dfPolFac.PolTopLine > 1)].FacKey)
        df=pd.DataFrame(lst,columns=['FacKey'])
        df['st'] = 1
        self.df_fac = self.df_fac.merge(df, on='FacKey', how='left')
        mask= self.df_fac['st'].notna()
        PatFlag.FlagFacOverexposed.set_flag(self.df_fac,mask)
        self.df_fac.drop(columns= 'st',inplace=True)

    def __check_location(self):
        self.df_loc['status'] = int(0)

        # Location record duplications
        mask = self.df_pol.PseudoPolicyID.duplicated(keep=False)
        PatFlag.FlagLocDupe.set_flag(self.df_loc, mask)

        # Location records with no policy
        diff = set(self.df_loc.PseudoPolicyID).difference(self.df_pol.PseudoPolicyID)
        if len(diff) > 0:
            df = self.df_loc[["PseudoPolicyID"]].merge(pd.DataFrame({"PseudoPolicyID": np.array(diff), "st":1}), on = 'PseudoPolicyID', how="left")
            mask = ~np.isnan(df.st)
            PatFlag.FlagLocOrphan.set_flag(self.df_loc, mask)

        #
        df = pd.concat([self.df_loc["LocationIDStack"], np.round(self.df_loc['AOI'], 1)], axis=1)
        df = df.dropna(subset=['LocationIDStack']).drop_duplicates(ignore_index=True)
        df = df.groupby('LocationIDStack').size().reset_index(name='LocIDCount')
        df = df[df.LocIDCount > 1].reset_index(drop=True)
        if len(df) > 0:
            df = self.df_loc[["LocationIDStack"]].merge(df, on = 'LocationIDStack', how="left")
            mask = df['LocIDCount'].notna()
            PatFlag.FlagLocIDDupe.set_flag(self.df_loc, mask)

        # Location record non numeric entry
        mask = np.sum(np.isnan(self.df_loc[["AOI", "RatingGroup"]]), axis=1) > 0
        PatFlag.FlagLocNA.set_flag(self.df_loc, mask)

        # Location record negative field
        mask = np.sum(self.df_loc[["AOI", "RatingGroup"]] < 0, axis=1) > 0
        PatFlag.FlagLocNeg.set_flag(self.df_loc, mask)
        
        # Location record rating group out of range
        mask = np.logical_or(self.df_loc.RatingGroup < 1,self.df_loc.RatingGroup > self.max_psold_rg)
        PatFlag.FlagLocRG.set_flag(self.df_loc, mask)
    
    def net_of_fac(self):
        df_use = self.get_good_policies()
        dfPolUse = self.df_pol.merge(df_use, on ='PseudoPolicyID', how='inner').reset_index(drop=True)
        dfFacUse = self.df_fac.merge(df_use, on ='PseudoPolicyID', how='inner').reset_index(drop=True)

        # Calculate the metrics for the policy (TopLine) and fac for layering exercise
        dfPolUse['PolTopLine'] = dfPolUse.PolLimit + dfPolUse.PolRetention
        dfPolUse.loc[dfPolUse.PolParticipation > 1, ['PolParticipation']] = 1

        # Combined Policy and Fac table, get metrics
        dfPolFac = dfPolUse.merge(dfFacUse, on="PseudoPolicyID")
        dfPolFac['status'] = dfPolFac['status_x'] + dfPolFac['status_y']
        dfPolFac.drop(columns=['status_x', 'status_y'],  inplace=True)

        dfPolFac['FacGupLimit'] = np.divide(dfPolFac.FacLimit, dfPolFac.PolParticipation)
        dfPolFac['FacGupAttachment'] = np.divide(dfPolFac.FacAttachment, dfPolFac.PolParticipation) \
            + dfPolFac.PolRetention
        dfPolFac['FacGupTopLine'] = dfPolFac.FacGupLimit + dfPolFac.FacGupAttachment

        # Layering Exercise
        # Created four tables. For each PsudoPolicy ID,
        # find the bottom point of exposure, and top point
        # for each the policy and Fac.
        # ............................................................
        dfLayer1 = dfPolUse[["PseudoPolicyID", "PolRetention"]].rename(
            columns={'PolRetention': "LayerLow"})
        dfLayer2 = dfPolUse[["PseudoPolicyID", "PolTopLine"]].rename(
            columns={'PolTopLine': "LayerLow"})
        dfLayer3 = dfPolFac[["PseudoPolicyID", "FacGupAttachment"]].rename(
            columns={'FacGupAttachment': "LayerLow"})
        dfLayer4 = dfPolFac[["PseudoPolicyID", "FacGupTopLine"]].rename(
            columns={'FacGupTopLine': "LayerLow"})

        # Combined the four table by row. Now each PseudoPolicy ID has
        # up to four records. Order based on PPID and Layer attach pt
        dfLayers = dfLayer1.append(dfLayer2).append(
            dfLayer3).append(dfLayer4, ignore_index=True)
        dfLayers.drop_duplicates(inplace=True, ignore_index=True)
        dfLayers.sort_values(by=['PseudoPolicyID', 'LayerLow'], inplace=True, ignore_index=True)

        # Add a key to each layer:
        # i.e.
        # PseudoPolicyID      LayerLow      LayerID
        #   ABCXYZ_123        1,000,000       1
        #   ABCXYZ_123        5,000,000       2
        #   ABCXYZ_123        8,000,000       3
        #   ABCXYZ_123        10,000,000      4

        dfLayers = dfLayers.rename_axis('LayerKey').reset_index()
        dfLayers.LayerKey = dfLayers.LayerKey + 1

        # Join the table above with a copy of itself, OFFSET by one.
        # i.e.
        # PseudoPolicyID      LayerLow      LayerID   LayerHigh
        #   ABCXYZ_123        1,000,000       1       5,000,000
        #   ABCXYZ_123        5,000,000       2       8,000,000
        #   ABCXYZ_123        8,000,000       3       10,000,000

        dfLayers1 = dfLayers[['PseudoPolicyID', 'LayerKey', 'LayerLow']].rename(
            columns={'LayerLow': 'LayerHigh'})
        dfLayers1['LayerKey'] = dfLayers1.LayerKey - 1
        dfLayers = dfLayers.merge(
            dfLayers1, on=['PseudoPolicyID', 'LayerKey'], how='inner')

        # Remove overlap caused by rounding
        dfLayers = dfLayers [dfLayers.LayerHigh - dfLayers.LayerLow > 1] #? 

        # Pull in Fac ceded amount
        dfLayers = sqldf("""Select a.*, b.OriginalPolicyID, b.PolParticipation as Participation, b.PolPremium, c.FacCeded as Ceded
                        FROM dfLayers a
                        Left Join dfPolUse b ON a.PseudoPolicyID = b.PseudoPolicyID
                        Left Join dfPolFac c ON a.PseudoPolicyID = c.PseudoPolicyID and a.LayerLow >= c.FacGupAttachment and a.LayerHigh <= FacGupTopLine
                    """)

        # Summarize Layers, sum over ceded
        dfLayers = sqldf("""select OriginalPolicyID,PseudoPolicyID,LayerLow,LayerHigh,Participation,PolPremium,
                sum(Ceded) as Ceded,
                row_number() OVER (PARTITION BY PseudoPolicyID ORDER BY PseudoPolicyID) as LayerID
                from dfLayers
                group by PseudoPolicyID,LayerLow,LayerHigh,Participation
                """)
        
        # Ceded Error Checkings
        dfLayers['Ceded'] = dfLayers['Ceded'].fillna(value=0)
        dfCededGT100 = dfLayers.loc[dfLayers.Ceded > 1, ['PseudoPolicyID']].drop_duplicates(ignore_index=True)
        dfCededGT100['st'] = 0

        if dfCededGT100 is not None and len(dfCededGT100) > 0:
            self.df_pol = self.df_pol.merge(dfCededGT100, on='PseudoPolicyID', how='left')
            mask = self.df_pol['st'].notna()
            PatFlag.FlagCeded100.set_flag(self.df_pol, mask)
            self.df_pol.drop(columns='st', inplace=True)

            self.df_fac = self.df_fac.merge(dfCededGT100, on='PseudoPolicyID', how='left')
            mask = self.df_fac['st'].notna()
            PatFlag.FlagCeded100.set_flag(self.df_fac, mask)
            self.df_fac.drop(columns='st', inplace=True)

            self.df_loc = self.df_loc.merge(dfCededGT100, on='PseudoPolicyID', how='left')
            mask = self.df_loc['st'].notna()
            PatFlag.FlagCeded100.set_flag(self.df_loc, mask)
            self.df_loc.drop(columns='st', inplace=True)

        # Additional Metrics
        dfLayers.loc[dfLayers.Ceded > 1, ['Ceded']] = 1
        dfLayers['NetParticipation'] = dfLayers.Participation * \
            (1 - dfLayers.Ceded)
        dfLayers['Limit100'] = dfLayers.LayerHigh-dfLayers.LayerLow
        dfLayers['LimitRetained'] = dfLayers.Limit100 * \
            dfLayers.NetParticipation

        # Tack on loss raito (input from sheet)
        dfLayers['LossRatio'] = float(self.para['loss_alae_ratio'])

        # FacNet table
        self.df_facnet = dfLayers[["OriginalPolicyID", "PseudoPolicyID", "LayerID",
                                    "Limit100", "LayerLow", "LossRatio", "PolPremium", "NetParticipation"]]
        self.df_facnet.columns = ["OriginalPolicyID", "PseudoPolicyID", "PseudoLayerID",
                                    "Limit", "Retention", "LossRatio", "OriginalPremium", "Participation"]

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
                df = self.df_loc.set_index('PseudoPolicyID')
                df.update(corr)
                self.df_loc = df.reset_index()
                modified = True
        
        # fac
        fn = r'C:\_Working\PAT_20201019\__temp\fac_correction.csv'
        if os.path.isfile(fn):  
            corr =  pd.read_csv(fn).set_index(['PseudoPolicyID', 'FacKey'])
            if len(corr) > 0:
                df = self.df_loc.set_index(['PseudoPolicyID', 'FacKey'])
                df.update(corr)
                self.df_loc = df.reset_index()
                modified = True

        if modified:
            self.check_data()
            self.net_of_fac()
    
    def get_validation_counts(self):
        res= {
            "Policy Records processed": len(self.df_pol),
            "Location Records processed": len(self.df_loc),
            "Facultative Records processed": len(self.df_fac),
        }

        for f in PatFlag:
            n1 = np.sum(self.df_pol.status & f !=0)
            n2 = np.sum(self.df_loc.status & f !=0)
            res[PatFlag.describe(f)] = max(n1, n2)

        return pd.DataFrame(data={'item': res.keys(), 'count':res.values()})

    def get_validation_data(self):
        df1 = self.df_pol[ self.df_pol.status !=0].sort_values(by='PseudoPolicyID').reset_index(drop = True)
        if len(df1) > 0:
            df = df1[['status']].drop_duplicates(ignore_index=True)
            df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
            df1 = df1.merge(df, on ='status')

        df2 = self.df_loc[ self.df_loc.status !=0].sort_values(by='PseudoPolicyID').reset_index(drop = True)
        if len(df2) > 0:
            df = df2[['status']].drop_duplicates(ignore_index=True)
            df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
            df2 = df2.merge(df, on ='status')

        df3 = self.df_fac[ self.df_fac.status !=0].sort_values(by='PseudoPolicyID').reset_index(drop = True)
        if len(df3) > 0:
            df = df3[['status']].drop_duplicates(ignore_index=True)
            df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
            df3 = df3.merge(df, on ='status')
        
        df1 = df1.drop(columns=['status']).sort_values(by='PseudoPolicyID').reset_index(drop = True)
        df2 = df2.drop(columns=['status']).sort_values(by='PseudoPolicyID').reset_index(drop = True)
        df3 = df3.drop(columns=['status'])\
                .sort_values(by=['PseudoPolicyID','FacAttachment', 'FacLimit', 'FacCeded'])\
                .reset_index(drop = True)

        return (df1, df2, df3)

    def get_good_policies(self):
        lst1 = self.df_pol[self.df_pol.status == 0].PseudoPolicyID
        lst2 = self.df_loc[self.df_loc.status == 0].PseudoPolicyID
        lst= set.intersection(set(lst1),set(lst2))

        lst3 = self.df_fac[self.df_fac.status != 0].PseudoPolicyID
        lst = lst.difference(set(lst3))

        if len(lst) >0: 
            return pd.DataFrame(lst,columns=['PseudoPolicyID'])

    def get_bad_policies(self):
        lst1 = self.df_pol[ self.df_pol.status !=0].PseudoPolicyID
        lst2 = self.df_loc[ self.df_loc.status !=0].PseudoPolicyID
        lst3 = self.df_fac[ self.df_fac.status !=0].PseudoPolicyID

        lst= set.union(set(lst1),set(lst2),set(lst3))
        if len(lst) >0: 
            return pd.DataFrame(lst,columns=['PseudoPolicyID'])

    def allocate_with_psold(self):     
        dfFacNetLast = self.df_facnet.groupby('PseudoPolicyID').agg({'PseudoLayerID':max}).rename(columns={'PseudoLayerID':'LayerPosition'})
        # dfLocation2
        dfLoc = self.df_facnet.merge(self.df_loc[['PseudoPolicyID','AOI', 'LocationIDStack', 'RatingGroup']], on='PseudoPolicyID')\
            .merge(dfFacNetLast, left_on='PseudoPolicyID', right_index=True)
            
        dfWeights = pd.read_csv('data/tmpWeights.csv'
            ,dtype={
                'OccupancyType':str,
                'PremiumWeight':float,
                'HPRMap':str,
                'PremiumPercent':float,
                'HPRTable':int
                })
        dfHPR = pd.read_csv('data/tmpHPR.csv',dtype={'Limit':float,'Weight':float})

        guNewPSTable = pd.read_csv('data/guNewPSTable2016.csv')
        gdMu = [1000 * (np.sqrt(10)**(i-1)) for i in range(1,12)]

        AOI_split = pd.read_csv('data/psold_aoi.csv').x.to_numpy() 
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
        dfLoc['sumLAS'] = (dfLoc.PolLAS-dfLoc.DedLAS)*dfLoc.LossRatio*dfLoc.Participation
        dfLoc['sumLAS'] = dfLoc['sumLAS'].fillna(value=0)
        dfLoc['sumLAS'] = dfLoc['sumLAS'].groupby(dfLoc.OriginalPolicyID).transform('sum')
        dfLoc['premLAS'] = dfLoc.OriginalPremium / dfLoc.sumLAS
        dfLoc['Prem'] = (dfLoc.PolLAS - dfLoc.DedLAS) * dfLoc.LossRatio * dfLoc.Participation * dfLoc.premLAS
        dfLoc['EffPrem'] = (dfLoc.Prem * self.dSubjPrem/ dfLoc.Prem.sum())
        dfLoc['ExpectedLossCount'] = ((np.minimum(dfLoc.RetStepLAS,dfLoc.PolLAS)-np.minimum(dfLoc.PolLAS, dfLoc.RetLAS)) \
            /(dfLoc.PolLAS - dfLoc.DedLAS) * dfLoc.LossRatio*dfLoc.EffPrem )/.01

        # Final
        self.df_pat = dfLoc[["Limit", "Retention", "Prem", "Participation", "LossRatio", "AOI", "LocationIDStack",
                    "RatingGroup", "OriginalPolicyID", "PseudoPolicyID", "PseudoLayerID", "PolLAS", "DedLAS"]] \
                .sort_values(by=['OriginalPolicyID', 'PseudoPolicyID', 'PseudoLayerID'])
        self.df_pat.rename(columns={
            'Prem':'Allocated_Premium',
            'RatingGroup':'Rating_Group',
            'OriginalPolicyID':'Original_Policy_ID',
            'PseudoLayerID': 'Pseudo_Layer_ID',
            'PolLAS':'PolicyLAS',
            'DedLAS':'DeductibleLAS',
            'LocationIDStack':'Original_Location_ID'
            })
      
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
        df['StackedPolicyLimit'] = df.sort_values(['LocationIDStack','Retention']).groupby('LocationIDStack')['StackedPolicyLimit'].transform(lambda x: x.cumsum().shift())
        df.loc[df.LocationIDStack.isna(), ['StackedPolicyLimit']] = 0
        #?
        df.loc[df.StackedPolicyLimit.isna(), ['StackedPolicyLimit']] = 0

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
            df.PolLAS, df.RetLAS)) / (df.PolLAS-df.DedLAS) * df.LossRatio * df.EffPrem) / rein
        
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


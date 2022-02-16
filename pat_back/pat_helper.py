from binascii import Incomplete
from cgitb import text
import hashlib, binascii, os
import json
import base64
import logging
from datetime import datetime
from io import BytesIO, StringIO
from msilib.schema import Complus
from operator import truediv
from pyexpat.errors import XML_ERROR_FEATURE_REQUIRES_XML_DTD
from turtle import right
from typing import Mapping
import uuid
import zipfile
import numpy as np
import pandas as pd
import pyodbc
from bcpandas import SqlCreds, to_sql
from sqlalchemy import column, false, null, true

from pat_back.pat_job import PatJob

from .pat_worker import PatWorker
from .settings import AppSettings
from .pat_flag import DATA_SOURCE_TYPE, PAT_FLAG, RATING_TYPE

class PatHelper:
    """Class to manage PAT jobs"""

    workers=[]
    job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    @classmethod
    def submit(cls, job_para, data=None):
        if (any(c not in job_para.keys() for c in ['job_guid', 'type_of_rating', 'data_source_type'])) \
            or (job_para['type_of_rating'] not in RATING_TYPE.__members__) \
            or (job_para['data_source_type'] not in DATA_SOURCE_TYPE.__members__): return 0

        logger = logging.getLogger(job_para['job_guid'])
        logger.info(f"""Job received:\n{json.dumps(job_para, sort_keys=True, indent=4)}""")

        job_id = cls.__register_job(job_para)
        if job_id > 0:
            logger.info(f"Job registered successfully - {job_id}")
            if data:
                logger.info("Save user uploaded data...")
                if not cls.__process_user_data(job_id,DATA_SOURCE_TYPE[job_para['data_source_type']], data):
                    logger.warning("Save user uploaded data...Failed!")
                    return 0
                logger.info("Save user uploaded data...OK")

            return job_id

        return 0
    
    @classmethod
    def submit_run(cls, job_para, data):
        if (not job_para) or (data is None):
            return "Data missing"

        df = None
        dlist = cls.__extract_user_data(data)
        if dlist: 
            if 'pseudopolicy' in dlist:
                df = dlist['pseudopolicy']
            elif 'policy' in dlist and 'location' in dlist:
                df = cls.__merge_policy_loc(dlist['policy'], dlist['location'])
       
        if df is None or len(df) > 1000000:
            return "Data file error or too big"

        fld = cls.__check_pseudopolicy(df)
        if fld is None: return "Data error"

        if fld:
            df.rename(columns=dict((y,x) for x,y in fld.items()), inplace=True) 
        if 'Participation' not in df: df['Participation'] = 1.0
        if 'LossRatio' not in df: df['LossRatio'] = np.nan
        if 'TIV' not in df: 
            df['TIV'] = 0.0
            if 'Building' in df: df['TIV'] += df.Building
            if 'Contents' in df: df['TIV'] += df.Contents
            if 'BI' in df: df['TIV'] += df.BI

        job = PatJob(param = job_para)
        if job.job_id > 0:
            df = job.allocate_premium(df)
            if df is not None and len(df) > 0:
                dl = set(['Policy', 'EffLmt', 'sumLAS']).intersection(set(df.columns))
                if dl: df.drop(columns = dl, inplace=True)
                if fld: df.rename(columns=dict((x,y) for x,y in fld.items()) | {'Premium':'Allocated_Premium', 
                        'PolLAS' : 'PolicyLimitLAS',
                        'DedLAS' : 'PolicyAttachLAS'}, inplace=True) 
                return df

        return 'Error'

    @classmethod
    def __register_job(cls, job_para):
        job_guid = job_para['job_guid']
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            dt = datetime.utcnow().isoformat()
            n = cur.execute(f"""if not exists(select 1 from pat_job where job_guid = '{job_guid}')
                                begin 
                                insert into pat_job values (next value for pat_analysis_id_seq, '{job_guid}', 
                                    '{(job_para['job_name'] if 'job_name' in job_para else 'No Name')}',
                                    '{dt}', null,null, 'received',0,
                                    '{(job_para['user_name'] if 'user_name' in job_para else 'No Name')}',
                                    '{(job_para['user_email'] if 'user_email' in job_para else '')}',
                                    '{json.dumps(job_para).replace("'", "''")}')
                                end""").rowcount
                                
            cur.commit()
            
            if n==1:
                df = pd.read_sql_query(f"""select job_id from pat_job where job_guid = '{job_guid}'""", conn)
                if len(df)>0:
                    return df.job_id[0]
        return 0

    @classmethod
    def __process_user_data(cls, job_id:int, ds_type:DATA_SOURCE_TYPE, data) -> bool:
        dlist = cls.__extract_user_data(data)
        if not dlist: return False      

        if ds_type== DATA_SOURCE_TYPE.User_Upload:
            df_p, df_f= None, None
            if 'pseudopolicy' in dlist:
                df_p = dlist['pseudopolicy']
            elif 'policy' in dlist and 'location' in dlist:
                df_p = cls.__merge_policy_loc(dlist['policy'], dlist['location'])
            if df_p is None: return False
            fld = cls.__check_pseudopolicy(df_p)
            if fld is None: return false
            elif fld:
                df_p.rename(columns=dict((y,x) for x,y in fld.items()), inplace=True) 
            if not cls.__save_policy_data(job_id, df_p): return False

            
            if 'fac' in dlist:
                df_f = dlist['fac']
                fld = cls.__check_fac(df_f)
                if fld is None: return False
                elif fld:
                    df_f.rename(columns=dict((y,x) for x,y in fld.items()), inplace=True) 
                
                if not cls.__save_fac_data(job_id, df_f): return False

            return True
        else: return cls.__save_correction_data(job_id, dlist)

    @classmethod
    def __extract_user_data(cls, data) -> dict:
        res = {}
        try:
            if data[:4] == b'PK\x03\x04': #zip file
                with zipfile.ZipFile(BytesIO(data),'r') as zf:
                    if 'xl/workbook.xml' in zf.namelist(): # excel
                        names= pd.ExcelFile(data).sheet_names
                        if 'pseudopolicy' in names: res['pseudopolicy'] = pd.read_excel(data, 'pseudopolicy')
                        elif 'policy' in names and 'location' in names:
                            res['policy'] = pd.read_excel(data, 'policy')
                            res['location'] = pd.read_excel(data, 'location')                    
                        if 'fac' in names: res['fac'] = pd.read_excel(data, 'fac')
                    else: #csv zipped zip
                        names = [str.lower(n) for n in zf.namelist()]
                        if 'pseudopolicy.csv' in names: res['pseudopolicy'] = pd.read_csv(zf.open('pseudopolicy.csv'))
                        elif 'policy.csv' in names and 'location.csv' in names:
                            res['policy'] = pd.read_csv(zf.open('policy.csv'))
                            res['location'] = pd.read_csv(zf.open('location.csv'))
                        if 'fac.csv' in names: res['fac'] = pd.read_csv(zf.open('fac.csv'))
            else: #csv
                res['pseudopolicy'] =  pd.read_csv(StringIO(str(data,'utf-8')))
        except:
            pass
        
        return res
    
    @classmethod
    def __find_column(cls, df, *flds):
        return next((f for f in df if f.lower() in map(str.lower, flds)), None)

    @classmethod
    def __merge_policy_loc(cls, df_p, df_l) -> dict:
        f1 = cls.__find_column(df_p,'policyid', 'policy id')
        f2 = cls.__find_column(df_l,'policyid', 'policy id')
        if f1 and f2:
            f3 = cls.__find_column(df_l,'locid', 'locationid', 'location id', 'loc id')
            if not f3:
                f3= "LocID"
                df_l[f3] = df_l.index + 1
            df = df_p.merge(df_l, left_on=f1, right_on=f2, how='inner',suffixes=('', '_loc'))
            df['PseudoPolicyID'] = df[f1].astype(str) + '_' + df[f3].astype(str)

            lst = [c for c in df.columns if c.endswith('_loc')]
            if f2 != f1: lst.append(f2)
            if lst:
                df.drop(columns=lst, inplace=True)

            return df        

    @classmethod
    def __check_pseudopolicy(cls, df) -> dict:
        fld = {}

        # critical ones
        f = cls.__find_column(df, 'originalpolicyid', 'policyid', 'policy id')
        if not f: return 
        elif f != 'PolicyID': fld['PolicyID'] = f

        f = cls.__find_column(df, 'pollimit','limit', 'policy limmit')
        if not f: return 
        elif f != 'Limit': fld['Limit'] = f

        f = cls.__find_column(df, 'polretention','retention')
        if not f: return 
        elif f != 'Retention': fld['Retention'] = f

        f = cls.__find_column(df, 'polpremium','polprem', 'premium')
        if not f: return 
        elif f != 'PolPrem': fld['PolPrem'] = f

        f = cls.__find_column(df, 'tiv','aoi')
        if f:  
            if f != 'TIV': fld['TIV'] = f
        else:
            f1 = cls.__find_column(df, 'building','building value')
            if f1 and f1 != 'Building': fld['Building'] = f1
            f2 = cls.__find_column(df, 'Contents','Contents value')
            if f2 and f2 != 'Contents': fld['Contents'] = f2
            f3 = cls.__find_column(df, 'bi','bi value', 'time_element')
            if f3 and f3 != 'BI': fld['BI'] = f3

            if not any(f1, f2, f3): return

        # optional ones    
        f = cls.__find_column(df, 'pseudopolicyid', 'pseudo policy id')
        if f:
            if f != 'PseudoPolicyID': fld['PseudoPolicyID'] = f
        else: 
            f = cls.__find_column(df, 'locid', 'locationid', 'location id', 'loc id')
            if not f: return
            elif f != 'LocID': fld['LocID'] = f

        f = cls.__find_column(df, 'locationidstack', 'stack')
        if f != 'Stack': fld['Stack'] = f

        f = cls.__find_column(df, 'ratinggroup','rtg', 'rating grp')
        if f and f != 'RatingGroup': fld['RatingGroup'] = f

        f = cls.__find_column(df, 'polparticipation','participate','participation')
        if f and f != 'Participation': fld['Participation'] = f

        f = cls.__find_column(df, 'lossratio', 'loss ratio', 'lae ratio')
        if f and f != 'LossRatio': fld['LossRatio'] = f

        f = cls.__find_column(df, 'accgrpid')
        if f and f != 'ACCGRPID': fld['ACCGRPID'] = f

        f = cls.__find_column(df, 'occupancy_scheme', 'occ_scheme')
        if f and f != 'occupancy_scheme': fld['occupancy_scheme'] = f

        f = cls.__find_column(df, 'occupancy_code', 'occ_code')
        if f and f != 'occupancy_code': fld['occupancy_code'] = f

        f = cls.__find_column(df, 'polretainedlimit', 'retainedlimit')
        if f and f != 'PolRetainedLimit': fld['PolRetainedLimit'] = f
           
        return fld

    @classmethod
    def __check_fac(cls, df) -> dict:
        fld = {}
        
        f = cls.__find_column(df, 'faclimit', 'limit')
        if not f: return 
        elif f != 'FacLimit': fld['FacLimit'] = f

        f = cls.__find_column(df, 'facattachment', 'attachment')
        if not f: return 
        elif f != 'FacAttachment': fld['FacAttachment'] = f

        f = cls.__find_column(df, 'facceded', 'ceded')
        if not f: return 
        elif f != 'FacCeded': fld['FacCeded'] = f

        f = cls.__find_column(df, 'pseudopolicyid')
        if f:
            if f != 'PseudoPolicyID': fld['PseudoPolicyID'] = f
        else:
            f1 = cls.__find_column(df, 'policyid', 'policy id')
            f2 = cls.__find_column(df, 'locid', 'locationid', 'location id', 'loc id')
            if not f1 or not f2: return
            if f1 != 'PolicyID': fld['PolicyID'] = f1
            if f2 != 'LocID': fld['LocID'] = f2

        f = cls.__find_column(df, 'fackey', 'facid', 'fac key', 'fac id')
        if f and f != 'FacKey': fld['FacKey'] = f

        return fld
    
    @classmethod
    def __save_correction_data(cls, job_id:int, dlist) -> bool: 
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        for k, df in dlist.items():
            if k=='pseudopolicy' and len(df) >0:
                tab = 'pat_pseudo_policy'
                kflds = ['PseudoPolicyID']
                vflds = ['PolRetainedLimit', 'PolLimit', 'PolParticipation', 'PolRetention', 'PolPremium',
                                                'occupancy_scheme', 'occupancy_code',
                                                'Building','Contents','BI','AOI','RatingGroup']
            elif k == 'fac' and len(df) > 0:
                tab = 'pat_facultative'
                kflds = ['PseudoPolicyID','FacKey']
                vflds = ['FacLimit', 'FacAttachment', 'FacCeded']
            else:
                continue

            if all(c in df.columns for c in (kflds + vflds)):
                mask0 = np.full(len(df),False)
                for f0 in vflds:
                    if f0 in df.columns:
                        f = f0 +'_revised'
                        if f in df.columns:
                            mask = df[f].notna() & (df[f] != df[f0])
                            df.loc[mask, [f0]] = df.loc[mask,f]
                            df.loc[~mask, [f0]] = None
                            mask0 |= mask
                        else:
                            df[f0] = None
                    else:
                        df[f0] = None

                df = df[mask0]
                df['job_id'] = job_id
                df['data_type'] = int(2)
                df = df[['job_id','data_type',*kflds, *vflds]]
                if len(df) > 0:
                    to_sql(df,tab, creds, index=False, if_exists='append')
            
            return True

    @classmethod
    def __save_policy_data(cls, job_id, df) -> bool:
        df.rename(columns= {
            'PolicyID': 'OriginalPolicyID',
            'Limit':'PolLimit',
            'Retention': 'PolRetention',
            'PolPrem':'PolPremium',
            'TIV' : 'AOI',
            'Stack': 'LocationIDStack',
            'Participation': 'PolParticipation'
        }, inplace=True);
        
        if 'PolParticipation' not in df: df['PolParticipation'] = 1.0
        if 'PolRetainedLimit' not in df: df['PolRetainedLimit'] = df['PolLimit'] * df['PolParticipation']
        if 'AOI' not in df: 
            df['AOI'] = 0.0
            if 'Building' in df: df['AOI'] += df.Building
            if 'Contents' in df: df['AOI'] += df.Contents
            if 'BI' in df: df['AOI'] += df.BI
        
        if 'Building' not in df: df['Building'] = df['AOI']
        if 'Contents' not in df: df['Contents'] = 0.0
        if 'BI' not in df: df['BI'] = 0.0
        
        lst =[c for c in ['OriginalPolicyID', 'PolLimit', 'PolRetention', 'PolPremium', 
            'Building', 'Contents', 'BI', 'AOI',  
            'LocationIDStack', 'RatingGroup', 'PolParticipation', 
            'LossRatio', 'PseudoPolicyID', 'ACCGRPID', 'PolRetainedLimit', 
            'occupancy_scheme', 'occupancy_code'] if c in df.columns]

        df = df[lst]

        df['job_id'] = job_id
        df['data_type'] = int(1)
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        to_sql(df,'pat_pseudo_policy', creds, index=False, if_exists='append')

        return True

    @classmethod
    def __save_fac_data(cls, job_id, df) -> bool:
        if 'PseudoPolicyID' not in df:
            if 'PolicyID' in df and 'LocID' in df:
                df['PseudoPolicyID'] = df['PolicyID'].astype(str)+'_'+df['LocID'].astype(str)
            else:
                return False        
        if 'FacKey' not in df: df['FacKey'] = df.index + 1

        df =df[['PseudoPolicyID','FacLimit','FacAttachment', 'FacCeded','FacKey']]

        df['job_id'] = job_id
        df['data_type'] = int(1)
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        to_sql(df,'pat_facultative', creds, index=False, if_exists='append')
        
        return True

    @classmethod
    def get_job_list(cls, user:str=None):
        with pyodbc.connect(cls.job_conn) as conn:
            u_str= "" if cls.is_admin(user) else f"where user_email is null or lower(user_email) = '{user}'"
            df = pd.read_sql_query(f"""select job_id job_id, job_guid, job_name, 
                    receive_time, start_time, finish_time, status, user_name, user_email
                from pat_job
                {u_str}
                order by finish_time desc, job_id desc""", conn)

            return df

    @classmethod
    def get_job(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select job_id job_id, job_guid, job_name, 
                    receive_time, start_time, finish_time, 
                    status, data_extracted,
                    user_name, user_email,
                    parameters
                from pat_job
                where job_id = {job_id}""", conn)
            if df is not None and len(df) > 0:
                job = df.to_dict('records')[0]
                if job['data_extracted']:
                    job['summary'] =cls.summary(job_id)

                return job

    @classmethod
    def summary(cls,job_id):
        summary = []
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""select count(*) n1, count(distinct OriginalPolicyID) n2, count(distinct LocationIDStack) n3
                from pat_pseudo_policy where job_id = {job_id} and data_type = 0""")
            n1, n2, n3 = cur.fetchone()

            summary.append({'item': '# Policy', 'cnt': n2})
            summary.append({'item': '# Location', 'cnt': n3})
            summary.append({'item': '# Pseudo Policy', 'cnt': n1})

            cur.execute(f"""select count(*) from pat_facultative where job_id = {job_id} and data_type = 0""")
            n1, = cur.fetchone()
            summary.append({'item': '# Facultative', 'cnt': n1})

            cur.execute(f"""select flag, count(*) from pat_pseudo_policy where job_id = {job_id} and data_type = 0 and flag!=0 group by flag""")
            rows = cur.fetchall()
            for row in rows:
                summary.append({'item': f"* {PAT_FLAG.describe(row[0])}", 'cnt': row[1]})

        return summary

    @classmethod
    def delete(cls, job_lst):
        if len(job_lst) >0 : 
            lst= ','.join([f'{a}' for a in job_lst]) 
            with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
                for t in ['pat_job','pat_pseudo_policy', 'pat_facultative', 'pat_premium']:
                    cur.execute(f"""delete from [{t}] where job_id in ({lst})""")
                    cur.commit()
        
        return cls.get_job_list()
   
    @classmethod
    def get_results(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select Limit, Retention, 
                        Premium as Allocated_Premium, 
                        Participation, 
                        Building,Contents, BI, AOI, 
                        a.RatingGroup as Rating_Group, 
                        a.LossRatio as Loss_Ratio, 
                        ACCGRPID, 
                        LocationIDStack as Original_Location_ID, 
                        OriginalPolicyID as Original_Policy_ID,
                        a.PseudoPolicyID, 
                        PseudoLayerID as Pseudo_Layer_ID,
                        PolLAS as PolicyLimitLAS,
                        DedLAS as PolicyAttachLAS
                    from pat_premium a
                        join pat_pseudo_policy b on a.job_id = b.job_id and a.PseudoPolicyID = b.PseudoPolicyID
                    where a.job_id = {job_id} and b.data_type = 0
                    order by b.LocationIDStack, b.OriginalPolicyID, Retention""", conn)

            return df

    @classmethod
    def get_unused(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df_pol = pd.read_sql_query(f"""select * from pat_pseudo_policy 
                where job_id = {job_id} and data_type = 0 and flag != 0
                order by PseudoPolicyID""",conn)
            if len(df_pol) > 0:
                df = df_pol[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df_pol = df_pol.merge(df, on ='flag', how='left').drop(columns=['job_id', 'data_type', 'flag'])

            df_fac = pd.read_sql_query(f"""select * from pat_facultative 
                where job_id = {job_id} and data_type = 0 and flag != 0
                order by PseudoPolicyID""",conn)
            if len(df_fac) > 0:
                df = df_fac[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df_fac = df_fac.merge(df, on ='flag', how='left').drop(columns=['job_id', 'data_type', 'flag'])
            
            df_layers = pd.read_sql_query(f"""select * from pat_layers 
                where job_id = {job_id} and flag != 0
                order by PseudoPolicyID, LayerID""",conn)
            if len(df_layers) > 0:
                df = df_layers[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df_layers = df_layers.merge(df, on ='flag', how='left').drop(columns=['job_id', 'flag'])

        return df_pol, df_fac, df_layers

    @classmethod
    def get_data(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df_pol = pd.read_sql_query(f"""select * from pat_pseudo_policy 
                where job_id = {job_id} and data_type = 0
                order by PseudoPolicyID""",conn)
            if len(df_pol) > 0:
                df = df_pol[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df_pol = df_pol.merge(df, on ='flag', how='left').drop(columns=['job_id', 'data_type', 'flag'])

            df_fac = pd.read_sql_query(f"""select * from pat_facultative 
                where job_id = {job_id} and data_type = 0
                order by PseudoPolicyID""",conn)
            if len(df_fac) > 0:
                df = df_fac[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df_fac = df_fac.merge(df, on ='flag', how='left').drop(columns=['job_id', 'data_type', 'flag'])

            df_layers = pd.read_sql_query(f"""select * from pat_layers 
                where job_id = {job_id}
                order by PseudoPolicyID, LayerID""",conn)
            if len(df_layers) > 0:
                df = df_layers[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df_layers = df_layers.merge(df, on ='flag', how='left').drop(columns=['job_id', 'flag'])

            df_facnet = pd.read_sql_query(f"""select a.PseudoPolicyID, a.LayerID as PseudoLayerID,
                    a.LayerHigh - a.LayerLow as Limit, a.LayerLow as Retention, 
                    b.polParticipation * (1 - a.Ceded) as Participation,
                    b.PolPremium, b.ACCGRPID, b.OriginalPolicyID, b.PolRetainedLimit, b.PolLimit, b.PolParticipation, b.PolRetention,
                    b.OriginalPolicyID, LocationIDStack, 
                    b.occupancy_scheme, b.occupancy_code, b.Building, b.Contents, b.BI,
                    b.AOI as TIV, b.RatingGroup, b.LossRatio,
                    c.Participation as F_Participation, c.RatingGroup as F_RatingGroup, c.LossRatio as F_LossRatio, 
                    c.Premium as Allocated_Premium, c.PolLAS as PolicyLimitLAS, c.DedLAS as PolicyAttachLAS
                from pat_layers a
                    join pat_pseudo_policy b on a.job_id = b.job_id and a.PseudoPolicyID = b.PseudoPolicyID 
                        and b.job_id = {job_id} and b.data_type = 0 
                    left join pat_premium c on b.job_id = c.job_id and a.PseudoPolicyID = c.PseudoPolicyID 
                        and a.LayerID = c.PseudoLayerID and c.job_id = {job_id}
                where a.job_id = {job_id}
                order by a.PseudoPolicyID, a.LayerID""", conn)

            return df_pol, df_fac, df_layers, df_facnet

    @classmethod
    def reset_jobs(cls, job_id, to_orig:bool = False):
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            dtlst= '(0)'
            extracted = ''
            if to_orig:
                cur.execute(f"""select parameters from pat_job where job_id = {job_id}""")
                row = cur.fetchone()
                if row is not None:
                    js = json.loads(row[0])
                    if 'data_source_type' in js and DATA_SOURCE_TYPE[js['data_source_type']] != DATA_SOURCE_TYPE.User_Upload:
                        dtlst=  '(0, 1)'
                        extracted = 'data_extracted = 0,'
                    
            for tab in ['pat_pseudo_policy','pat_facultative']:
                cur.execute(f"""delete from {tab} where job_id = {job_id} and data_type in {dtlst};""")
                cur.commit()

            cur.execute(f"""delete from pat_premium where job_id = {job_id}""")
            cur.commit()
            cur.execute(f"""update pat_job set status = 'received',{extracted} start_time = null, finish_time =null
                    where job_id = {job_id};""")
            cur.commit()

    @classmethod
    def rename_job(cls, job_id, new_name):
        job =cls.get_job(job_id)
        if job:
            para = json.loads(job['parameters'])
            if para:
                para['job_name'] = new_name
                with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
                    cur.execute(f"""update pat_job set job_name = '{new_name}',
                            parameters = '{json.dumps(para).replace("'", "''")}'
                            where job_id = {job_id};""")
                    cur.commit()

        # with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
        #     cur.execute(f"""update pat_job set job_name = '{new_name}'
        #             where job_id = {job_id};""")
        #     cur.commit()

    @classmethod
    def public_job(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""select parameters from pat_job where job_id = {job_id}""")
            row = cur.fetchone()
            if row is not None:
                js = json.loads(row[0])
                js['user_name'] = 'developer'
                js['user_email'] = 'developer.pat@guycarp.com'

                cur.execute(f"""update pat_job set user_name = 'developer', user_email = null,
                    parameters = '{json.dumps(js).replace("'", "''")}' 
                    where job_id = {job_id};""")
                cur.commit()        

    @classmethod
    def decode64(cls, inp):
        try:
            base64_bytes = inp.encode("ascii")
            sample_string_bytes = base64.b64decode(base64_bytes)
            return sample_string_bytes.decode("ascii")
        except:
            pass

    @classmethod
    def is_admin(cls, provided_password):
        """Verify a stored password against one provided by user"""
        if not provided_password:
            return False

        salt = AppSettings.PAT_ADMIN[:64]
        stored_password = AppSettings.PAT_ADMIN[64:]
        pwdhash = hashlib.pbkdf2_hmac('sha512', 
                                    provided_password.encode('utf-8'), 
                                    salt.encode('ascii'), 
                                    100000)
        pwdhash = binascii.hexlify(pwdhash).decode('ascii')
        return pwdhash == stored_password
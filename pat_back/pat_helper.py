import json
import logging
from datetime import datetime
from io import BytesIO
import uuid
import zipfile
import numpy as np
import pandas as pd
import pyodbc
from bcpandas import SqlCreds, to_sql

from pat_back.pat_job import PatJob

from .pat_worker import PatWorker
from .settings import AppSettings
from .pat_flag import PAT_FLAG

class PatHelper:
    """Class to manage PAT jobs"""

    workers=[]
    job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    @classmethod
    def submit(cls, job_para, data=None):
        if not job_para or 'job_guid' not in job_para:
            return 0

        logger = logging.getLogger(job_para['job_guid'])
        logger.info(f"""Job received:\n{json.dumps(job_para, sort_keys=True, indent=4)}""")
    
        job_id = cls.__register_job(job_para)
        if job_id > 0:
            logger.info(f"Job registered successfully - {job_id}")
            if data:
                logger.info("Save correction data...")
                if(cls.__save_data_correction(job_id, data)<=0):
                    logger.warning("Save correction data...Failed!")
                    return 0
                else:
                    logger.info("Save correction data...OK")
            
            return job_id
        
        return 0
    
    @classmethod
    def submit_run(cls, job_para, data):
        if (not job_para) or (data is None):
            return "Parameter missing"
        if len(data) > 1000000:
            return "Job too big"

        for c in ['PolicyID', 'Limit', 'Retention', 'PolPrem', 'TIV', 'Stack', 'RatingGroup']:
            if c not in data:
                return 'Input data format error'
        
        if 'LossRatio' not in data and 'loss_alae_ratio' in job_para:
            data['LossRatio'] = float(job_para['loss_alae_ratio'])        

        job = PatJob(param=job_para)
        if job.job_id > 0:
            df = job.allocate_premium(data)
            if df is not None and len(df) > 0:
                df.rename(columns={'Premium':'Allocated_Premium', 
                        'PolLAS' : 'PolicyLimitLAS',
                        'DedLAS' : 'PolicyAttachLAS'}, inplace= True)
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
    def __save_data_correction(cls, job_id, data) -> bool:
        ret= False
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        with pyodbc.connect(cls.job_conn) as conn, zipfile.ZipFile(BytesIO(data),'r') as zf:
            for d, t, kflds, vflds in [('pol_validation.csv', 'pat_pseudo_policy', ['PseudoPolicyID'],
                                    ['PolRetainedLimit', 'PolLimit', 'PolParticipation', 'PolRetention', 'PolPremium',
                                        'occupancy_scheme', 'occupancy_code','Building','Contents','BI','AOI','RatingGroup']),
                                ('fac_validation.csv', 'pat_facultative', ['PseudoPolicyID','FacKey'], 
                                    ['FacLimit', 'FacAttachment', 'FacCeded'])]:
                if d in zf.namelist():
                    df =  pd.read_csv(zf.open(d))
                    if len(df) > 0 and all(k in df.columns for k in kflds):
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
                            to_sql(df,t, creds, index=False, if_exists='append')
                        ret = True      
        return ret

    @classmethod
    def get_job_list(cls, user:str=None):
        with pyodbc.connect(cls.job_conn) as conn:
            u_str= f"where user_email is null or lower(user_email) = '{user}'" if user else ""
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
    def get_results(cls, job_lst):
        with pyodbc.connect(cls.job_conn) as conn:
            jlst= f"""({','.join([f'{a}' for a in job_lst])})"""
            df = pd.read_sql_query(f"""select a.job_id, 
                        Limit, Retention, 
                        Premium as Allocated_Premium, 
                        Participation, 
                        Building,Contents, BI, AOI, 
                        RatingGroup as Rating_Group, 
                        ACCGRPID, 
                        LocationIDStack as Original_Location_ID, 
                        OriginalPolicyID as Original_Policy_ID,
                        a.PseudoPolicyID, 
                        PseudoLayerID as Pseudo_Layer_ID,
                        PolLAS as PolicyLimitLAS,
                        DedLAS as PolicyAttachLAS
                    from pat_premium a
                        left join pat_pseudo_policy b on a.job_id = b.job_id and a.PseudoPolicyID = b.PseudoPolicyID
                            and b.job_id in {jlst} and b.data_type = 0
                    where a.job_id in {jlst}
                    order by a.job_id, b.LocationIDStack, b.OriginalPolicyID, Retention""", conn)

            if len(job_lst) <= 1:
                df =df.drop(columns=['job_id'])

            return df

    @classmethod
    def delete(cls, *job_lst):
        if len(job_lst) >0 :  
            lst= [f"{a}" for a in job_lst]
            with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
                for t in ['pat_job','pat_pseudo_policy', 'pat_facultative', 'pat_premium']:
                    cur.execute(f"""delete from [{t}] where job_id in ({','.join(lst)})""")
                    cur.commit()
        
        return cls.get_job_list()
   
    @classmethod
    def get_validation_data(self, job_id, flagged:bool=True):
        with pyodbc.connect(self.job_conn) as conn:
            df1 = pd.read_sql_query(f"""select * from pat_pseudo_policy 
                where job_id = {job_id} and data_type = 0 {('and flag != 0' if flagged else '')}
                order by PseudoPolicyID""",conn)
            if len(df1) > 0:
                df = df1[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df1 = df1.merge(df, on ='flag', how='left').drop(columns=['job_id', 'data_type', 'flag'])

            df2 = pd.read_sql_query(f"""select * from pat_facultative 
                where job_id = {job_id} and data_type = 0 {('and flag != 0' if flagged else '')}
                order by PseudoPolicyID""",conn)
            if len(df2) > 0:
                df = df2[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PAT_FLAG.describe(x[0]), axis=1)
                df2 = df2.merge(df, on ='flag', how='left').drop(columns=['job_id', 'data_type', 'flag'])

        return (df1, df2)
    
    @classmethod
    def reset_jobs(cls, lst):
        jlst= ','.join([f'{j}' for j in lst])
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            df =pd.read_sql_query(f"select job_id, data_extracted from pat_job where job_id in ({jlst})",conn)
            if np.any(df.data_extracted==0):
                jlst1 = ','.join([f'{j}' for j in df[df.data_extracted==0]['job_id']]) 
                for tab in ['pat_pseudo_policy','pat_facultative']:
                    cur.execute(f"""delete from {tab} where job_id in ({jlst1}) and data_type != 2;""")
                    cur.commit()
            if np.any(df.data_extracted!=0):
                jlst2 = ','.join([f'{j}' for j in df[df.data_extracted!=0]['job_id']]) 
                for tab in ['pat_pseudo_policy','pat_facultative']:
                    cur.execute(f"""delete from {tab} where job_id in ({jlst2}) and data_type =0;""")
                    cur.commit()
            
            cur.execute(f"""delete from pat_premium where job_id in ({jlst})""")
            cur.commit()
            cur.execute(f"""update pat_job set status = 'received', start_time =null, finish_time =null
                    where job_id in ({jlst});""")
            cur.commit()
    
    @classmethod
    def update_job(cls, job_id, para):
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            for tab in ['pat_pseudo_policy','pat_facultative']:
                cur.execute(f"""delete from {tab} where job_id = {job_id} and data_type = 0;""")
                cur.commit()

            cur.execute(f"""delete from pat_premium where job_id = {job_id}""")
            cur.commit()
            
            cur.execute(f"""update pat_job set status = 'received', start_time = null, finish_time = null,
                    parameters = '{json.dumps(para).replace("'", "''")}'
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
            cur.execute(f"""update pat_job set user_name = 'developer',
                user_email = null where job_id = {job_id};""")
            cur.commit()        

    @classmethod
    def cancel_jobs(cls, lst):
        jlst= ','.join([f'{j}' for j in lst])
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = 'cancelled', start_time = null, finish_time = null
                    where job_id in ({jlst});""")
            cur.commit()


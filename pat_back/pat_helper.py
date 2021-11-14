import json
import logging
from datetime import datetime
from io import BytesIO
import zipfile
import numpy as np
import pandas as pd
import pyodbc
from bcpandas import SqlCreds, to_sql

from .pat_worker import PatWorker
from .settings import AppSettings
from .pat_flag import PatFlag

def split_flag(row):
    if row is None or len(row)<=0: 
        return []
         
    desc = PatFlag.describe(row[0])
    return desc.split(',') if desc else []

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
    def __register_job(cls, job_para):
        job_guid = job_para['job_guid']
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            dt = datetime.utcnow().isoformat()
            n = cur.execute(f"""if not exists(select 1 from pat_job where job_guid = '{job_guid}')
                                begin 
                                insert into pat_job values (next value for pat_analysis_id_seq, '{job_guid}', 
                                    '{(job_para['job_name'] if 'job_name' in job_para else 'No Name')}',
                                    '{dt}', '{dt}', 'received',0,
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
    def __save_data_correction(cls, job_id, data):
        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        with pyodbc.connect(cls.job_conn) as conn, zipfile.ZipFile(BytesIO(data),'r') as zf:
            for d,t in [('pol_validation.csv', 'pat_policy'),
                    ('loc_validation.csv', 'pat_location'),
                    ('fac_validation.csv', 'pat_facultative')]:
                
                if d in zf.namelist():
                    df =  pd.read_csv(zf.open(d))
                    if len(df) > 0:
                        df['job_id'] = job_id
                        df['data_type'] = int(2)
                        df = df.drop(columns=['Notes'])

                        to_sql(df,t, creds, index=False, if_exists='append')
                        return len(df)

    @classmethod
    def get_job_list(cls, user:str=None):
        with pyodbc.connect(cls.job_conn) as conn:
            u_str= f"where lower(user_email) = '{user}'" if user else ""
            df = pd.read_sql_query(f"""select job_id job_id, job_guid, job_name, 
                    receive_time, update_time, status, user_name, user_email
                from pat_job
                {u_str}
                order by update_time desc, job_id desc""", conn)

            return df

    @classmethod
    def get_job_para(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select parameters from pat_job where job_id = {job_id}""", conn)
            if df is not None and len(df) > 0:
                return json.loads(df.parameters[0])

    @classmethod
    def get_job_status(cls, job_id):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select status from pat_job where job_id = {job_id}""", conn)
            if df is not None and len(df) > 0:
                return df.status[0]

    @classmethod
    def get_summary(cls, job_id):
        summary = pd.DataFrame(columns=['item', 'cnt'])
        summary1 = pd.DataFrame(columns=['item', 'cnt'])
        with pyodbc.connect(cls.job_conn) as conn:
            for t in ['Policy', 'Location', 'Facultative']:
                df = pd.read_sql_query(f"""select flag, count(*) as cnt 
                                from pat_{t} where job_id = {job_id} and data_type = 0 group by flag""", conn)
                summary.loc[summary.shape[0]]=[f'{t} Records Processed', df.cnt.sum()]
                
                df["Notes"] = df.apply(split_flag, axis=1)
                df = df.explode("Notes").groupby('Notes').agg({'cnt': 'sum'}).reset_index()
                df=df[df.Notes != ''].rename(columns={'Notes':'item'})
                if len(df) > 0:
                    summary1= pd.concat((summary1,df), ignore_index = True, axis = 0)
        
        return pd.concat((summary,summary1), ignore_index = True, axis = 0)

    @classmethod
    def get_results(cls, job_lst):
        with pyodbc.connect(cls.job_conn) as conn:
            df = pd.read_sql_query(f"""select job_id, Limit, Retention, Premium, Participation, AOI, LocationIDStack, 
                                            RatingGroup, OriginalPolicyID, ACCGRPID, PseudoPolicyID, PseudoLayerID, PolLAS, DedLAS
                                       from pat_premium where job_id in ({','.join([f'{a}' for a in job_lst])})
                                       order by job_id, LocationIDStack, OriginalPolicyID, ACCGRPID""", conn)

            df.rename(columns={
                'Premium':'Allocated_Premium',
                'RatingGroup':'Rating_Group',
                'OriginalPolicyID':'Original_Policy_ID',
                'PseudoLayerID': 'Pseudo_Layer_ID',
                'PolLAS':'PolicyLAS',
                'DedLAS':'DeductibleLAS',
                'LocationIDStack':'Original_Location_ID'
                }, inplace=True)
            
            if len(job_lst) > 1:
                df =df.drop(columns=['job_id'])

            return df

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
    def get_validation_data(self, job_id, flagged:bool=True):
        with pyodbc.connect(self.job_conn) as conn:
            df1 = pd.read_sql_query(f"""select * from pat_policy 
                where job_id = {job_id} and data_type = 0 {('and flag != 0' if flagged else '')}
                order by PseudoPolicyID""",conn)
            if len(df1) > 0:
                df = df1[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
                df1 = df1.merge(df, on ='flag', how='left').drop(columns=['job_id'])
            
            df2 = pd.read_sql_query(f"""select * from pat_location 
                where job_id = {job_id} and data_type = 0 {('and flag != 0' if flagged else '')}
                order by PseudoPolicyID""",conn)
            if len(df2) > 0:
                df = df2[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
                df2 = df2.merge(df, on ='flag', how='left').drop(columns=['job_id'])

            df3 = pd.read_sql_query(f"""select * from pat_facultative 
                where job_id = {job_id} and data_type = 0 {('and flag != 0' if flagged else '')}
                order by PseudoPolicyID""",conn)
            if len(df3) > 0:
                df = df3[['flag']].drop_duplicates(ignore_index=True)
                df["Notes"] = df.apply(lambda x: PatFlag.describe(x[0]), axis=1)
                df3 = df3.merge(df, on ='flag', how='left').drop(columns=['job_id'])

        return (df1, df2, df3)
    
    @classmethod
    def reset_jobs(cls, lst):
        jlst= ','.join([f'{j}' for j in lst])
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            df =pd.read_sql_query(f"select job_id, data_extracted from pat_job where job_id in ({jlst})",conn)
            if np.any(df.data_extracted==0):
                jlst1 = ','.join([f'{j}' for j in df[df.data_extracted==0]['job_id']]) 
                for tab in ['pat_policy', 'pat_location','pat_facultative']:
                    cur.execute(f"""delete from {tab} where job_id in ({jlst1}) and data_type != 2;""")
                    cur.commit()
            if np.any(df.data_extracted!=0):
                jlst2 = ','.join([f'{j}' for j in df[df.data_extracted!=0]['job_id']]) 
                for tab in ['pat_policy', 'pat_location','pat_facultative']:
                    cur.execute(f"""delete from {tab} where job_id in ({jlst2}) and data_type =0;""")
                    cur.commit()
            
            cur.execute(f"""delete from pat_premium where job_id in ({jlst})""")
            cur.commit()
            cur.execute(f"""update pat_job set status = 'received',
                    update_time = '{datetime.utcnow().isoformat()}'
                    where job_id in ({jlst});""")
            cur.commit()
    
    @classmethod
    def update_job(cls, job_id, para):
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            for tab in ['pat_policy', 'pat_location','pat_facultative']:
                cur.execute(f"""delete from {tab} where job_id = {job_id} and data_type = 0;""")
                cur.commit()

            cur.execute(f"""delete from pat_premium where job_id = {job_id}""")
            cur.commit()
            
            cur.execute(f"""update pat_job set status = 'received',
                    parameters = '{json.dumps(para).replace("'", "''")}'  
                    update_time = '{datetime.utcnow().isoformat()}'
                    where job_id = {job_id};""")
            cur.commit()

    @classmethod
    def cancel_jobs(cls, lst):
        jlst= ','.join([f'{j}' for j in lst])
        with pyodbc.connect(cls.job_conn) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = 'cancelled',
                    update_time = '{datetime.utcnow().isoformat()}'
                    where job_id in ({jlst});""")
            cur.commit()


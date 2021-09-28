from os import path
import numpy as np
import pandas as pd
import json
import logging

from datetime import datetime
from flask.wrappers import Response
from flask import abort
import threading

import pyodbc
from bcpandas import SqlCreds, to_sql

from .settings import AppSettings
from pat import PatAnalysis, PatFlag

class PatJobMng (object):
    conn_str= f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''

    def __init__(self, files):
        self.analysis = None 
        self.job_id = 0

        if files and 'para' in files.keys():
            self.analysis = PatAnalysis(json.loads(files['para'].read()))

            if self.analysis and self.analysis.job_guid:
                self.__setup_logging()
                self.__write_job()
                self.logger.info(f"Analysis id: {self.job_id}")

    def __write_job(self):
        with pyodbc.connect(self.conn_str) as conn, conn.cursor() as cur:
            dt = datetime.utcnow().isoformat()
            n = cur.execute(f"""if not exists(select 1 from pat_job where job_guid = '{self.analysis.job_guid}')
                                begin 
                                insert into pat_job values (next value for pat_analysis_id_seq, '{self.analysis.job_guid}', '{self.analysis.job_name}',
                                    '{dt}', '{dt}', 'received',
                                    '{self.analysis.user_name}','{self.analysis.user_email}',
                                    '{json.dumps(self.analysis.para).replace("'", "''")}')
                                end""").rowcount
            cur.commit()
            
            if n==1:
                df = pd.read_sql_query(f"""select job_id from pat_job where job_guid = '{self.analysis.job_guid}'""", conn)
                if len(df)>0:
                    self.job_id = df.job_id[0]

    def __update_status(self, st):
        with pyodbc.connect(self.conn_str) as conn, conn.cursor() as cur:
            cur.execute(f"""update pat_job set status = '{st.replace("'","''")}', 
                update_time = '{datetime.utcnow().isoformat()}'
                where job_id = {self.job_id}""").rowcount
            cur.commit()

    def __setup_logging(self):
        self.log_file = path.join(AppSettings.PAT_LOG, "pat.log")

        self.logger = logging.getLogger(f"{self.analysis.job_guid[:8]}")
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

        self.logger.info(f"Start processing job: {self.analysis.job_guid}")
        self.logger.info(f"parameter: {json.dumps(self.analysis.para, sort_keys=True, indent=4)}")

    def __write_data(self, df, tab, remove_before_insert:bool = False):
        if remove_before_insert:
            with pyodbc.connect(self.conn_str) as conn, conn.cursor() as cur:
                cur.execute(f"delete from [{tab}] where job_id = {self.job_id}")
                cur.commit()

        creds = SqlCreds(AppSettings.PAT_JOB_SVR, AppSettings.PAT_JOB_DB, AppSettings.PAT_JOB_USR, AppSettings.PAT_JOB_PWD)
        to_sql(df, tab, creds, index=False, if_exists='append')

    # deamon
    def process_job_async(self):
        d = threading.Thread(name='pat-daemon', target=self.perform_analysis)
        d.setDaemon(True)
        d.start()

        return True
    
    def perform_analysis(self):
        self.__update_status("extract_data")
        self.logger.info("Extract policy, location, and Fac data...")
        self.analysis.extract_edm_rdm()
        self.logger.info("Extract policy, location, and Fac data...OK")

        self.__update_status("save_data")
        self.logger.info("Save data to tool database...")

        for d, t in [(self.analysis.df_pol,"pat_policy"),
                  (self.analysis.df_loc,"pat_location"),
                  (self.analysis.df_fac,"pat_facultative")]:
            df = d.fillna(value=0)
            df.insert(0, 'job_id', self.job_id)
            self.__write_data(df,t, True)
        self.logger.info("Save data to tool database...OK")
        
        self.__update_status("check_data")     
        self.logger.info("Check data...")     
        valid = self.analysis.check_data()
        self.logger.info("Check data...OK")

        if valid:
            if len(self.analysis.df_fac) > 0:
                self.__update_status("net_of_fac")    
                self.logger.info("Create layer stack for net of FAC...")    
                self.analysis.net_of_fac()
                self.logger.info("Create layer stack for net of FAC...OK")   

            self.__update_status("allocate")
            self.logger.info("Allocate premium...")
            self.analysis.allocate_with_psold()   
            self.logger.info("Allocate premium...OK")

        # else:
        #     return "Need to validate data!"

        # save results
        if self.analysis.df_pat is not None and len(self.analysis.df_pat) > 0:
            df = self.analysis.df_pat.fillna(value=0)
            df.insert(0, 'job_id', self.job_id)
            
            self.__update_status("save_results")
            self.logger.info("Save results to database...")
            self.__write_data(df, 'pat_premium',True)
            self.logger.info("Save results to database...OK")
        
        self.__update_status("finished")
        self.logger.info("Finished!")
    
    @classmethod
    def get_job_list(cls):
        with pyodbc.connect(cls.conn_str) as conn, conn.cursor() as cur:
            df = pd.read_sql_query(f"""select job_id job_id, job_guid, job_name, receive_time, update_time, status, user_name, user_email
            from pat_job order by update_time desc, job_id desc""", conn)

            return df.to_json()

    @classmethod
    def get_job_para(cls, job_id):
        with pyodbc.connect(cls.conn_str) as conn, conn.cursor() as cur:
            df = pd.read_sql_query(f"""select parameters from pat_job where job_id = {job_id}""", conn)
            return df.parameters[0]

    @classmethod
    def get_summary(cls, job_id):
        with pyodbc.connect(cls.conn_str) as conn, conn.cursor() as cur:
            df = pd.read_sql_query(f"""select count(*) as cnt_policy from pat_policy where job_id = {job_id}""", conn)
            df['cnt_location'] = pd.read_sql_query(f"""select count(*) from pat_location where job_id = {job_id}""", conn)
            df['cnt_fac'] = pd.read_sql_query(f"""select count(*) from pat_facultative where job_id = {job_id}""", conn)
            return df.to_json()

    @classmethod
    def get_results(cls, job_id):
        with pyodbc.connect(cls.conn_str) as conn, conn.cursor() as cur:
            df = pd.read_sql_query(f"""select Limit, Retention, Premium, Participation, AOI, LocationIDStack, 
                                            RatingGroup, OriginalPolicyID, PseudoPolicyID, PseudoLayerID, PolLAS, DedLAS
                                       from pat_premium where job_id = {job_id}""", conn)
            return df.to_json()

    @classmethod
    def delete(cls, job_id):
        with pyodbc.connect(cls.conn_str) as conn, conn.cursor() as cur:
            for t in ['pat_job','pat_policy','pat_location', 'pat_facultative', 'pat_premium']:
                cur.execute(f"""delete from [{t}] where job_id = {job_id}""")
            
            cur.commit()
        
        return cls.get_job_list()
   

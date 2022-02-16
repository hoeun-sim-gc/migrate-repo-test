import logging
import threading
import uuid
from time import sleep
from random import random

import pyodbc

from .pat_job import PatJob
from .settings import AppSettings

class PatWorker(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    workers=[]

    def __init__(self, job_id=0):
        super(PatWorker, self).__init__(name='pat-worker', daemon=True, 
                target=self.__run_job, args=(job_id,))
        self._stop_event = threading.Event()       
        self.job_id = 0

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    @classmethod
    def start_worker(cls, job_id:int=0):
        #take some random delay
        sleep(random() * 5)
        cls.workers = [w for w in cls.workers if not w.stopped()]

        if len(cls.workers) < AppSettings.MAX_WORKERS: 
            d = PatWorker(job_id)
            cls.workers.append(d)
            d.start()

    def __run_job(self, job_id):
        try:
            job_id = self.__checkout_job(job_id)
            while job_id > 0:
                job = PatJob(job_id)
                if job.job_id == job_id:
                    self.job_id = job_id
                    try:
                        job.run(self.stopped)
                    except Exception as e:
                        job.update_status('error')
                        logging.warn(f"Error captured: {e}" )

                    finally:
                        self.job_id = 0
                        if self.stopped():
                            self._stop_event.clear()

                job_id = self.__checkout_job()
        except Exception as e:
            logging.warn(f"Error captured: {e}" )
        finally:
            self.workers.remove(self) 
    
    def __checkout_job(self, job_id =0):
        flag = str(uuid.uuid4())[:8]
        job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
            MultipleActiveResultSets=true;'''
        with pyodbc.connect(job_conn) as conn, conn.cursor() as cur:
            if job_id > 0: 
                cur.execute(f"""update pat_job set status = 'wait_to_start_{flag}'
                    where job_id ={job_id} and status = 'received'""")
            else:
                cur.execute(f"""update pat_job set status = 'wait_to_start_{flag}'
                    where job_id in 
                    (select top 1 job_id from pat_job where status = 'received' order by receive_time, job_id)""")
            cur.commit()

            cur.execute(f"""select top 1 job_id from pat_job where status = 'wait_to_start_{flag}'""")
            row =cur.fetchone()
            cur.commit()

            if row is not None:
                job_id = row[0]

                cur.execute(f"""update pat_job set status = 'wait_to_start' where job_id ={job_id}""")
                cur.commit()

                return job_id
        
        return 0

    @classmethod
    def stop_jobs(cls, lst):
        clst=[]
        for j in lst:
            for w in cls.workers:
                if w.job_id == j:
                    w.stop()
        else:
            clst.append(j)
        
        if clst:
            jlst= ",".join([f"{a}" for a in clst]) 
            job_conn = f'''DRIVER={{SQL Server}};Server={AppSettings.PAT_JOB_SVR};Database={AppSettings.PAT_JOB_DB};
                User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};
                MultipleActiveResultSets=true;'''
            with pyodbc.connect(job_conn) as conn, conn.cursor() as cur:
                cur.execute(f"""update pat_job set status = 'cancelled' where job_id in ({jlst})""") 
                cur.commit()            

    @classmethod
    def is_runnung(cls, job_id):
        for w in cls.workers:
            if w.job_id == job_id:
                return True
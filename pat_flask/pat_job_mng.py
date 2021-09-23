from flask.wrappers import Response
from flask import abort
import threading

import json

from .settings import AppSettings
from pat import PatAnalysis, PatFlag

class PatJobMng (object):
    def __init__(self, files):
        self.analysis = None 

        if files and 'para' in files.keys():
            self.analysis = PatAnalysis(json.loads(files['para'].read())) 

    # deamon
    def process_job_async(self):
        d = threading.Thread(name='pat-daemon', target=self.perform_analysis)
        d.setDaemon(True)
        d.start()

        return True
    
    def perform_analysis(self):
        # conn_string = AppSettings.PAT_JOB_CONN

        self.analysis.extract_edm_rdm()
        if self.analysis.check_data():     
            if len(self.analysis.df_fac) > 0:
                self.analysis.net_of_fac()    
            self.analysis.allocate_with_psold()   
            # else:
            #     return "Need to validate data!"

        # save results
        # update job status

        self.analysis.df_pat.to_csv(r'C:\_Working\PAT_20201019\__temp\df_pat_svc.csv',index=False)

        # return self.analysis.df_pat.describe().to_json()
  
    
    # def get_job_list(self, job_type):
    #     resp= requests.get(f"{self.pm_master}/api/Jobs?type={job_type}")
    #     if resp.status_code == 200:
    #         return resp.content
    #     else:
    #         abort(resp.status_code)

    # deamon
    # def process_job_async(self):
    #     d = threading.Thread(name='pr-daemon', target=self.process_job)
    #     d.setDaemon(True)
    #     d.start()

    #     return True

    # def process_job(self):
    #     pr_job_id = AppSettings.PR_JOB_SMALL
    #     try:
    #         self.logger.debug("Extract detail loss data...")
    #         data_size = self.extract_data()
    #         if data_size > 0:
    #             if data_size > AppSettings.PR_JOB_SPLIT:
    #                 pr_job_id = AppSettings.PR_JOB_MEDIUM
    #             self.logger.debug("Extract detail loss data...OK")
    #         else:
    #             self.logger.debug("Extract detail loss data...Failed")
    #             return

    #         # final check flag consistency
    #         if not self.verify_adls_flag():
    #             self.logger.warn("The ADLS flag has changed!")
    #             return

    #         self.logger.debug("Upload detail loss data to Data Lake...")
    #         if self.upload_data():
    #             self.logger.debug("Upload detail loss data to Data Lake...OK")
    #         else:
    #             return

    #         self.logger.debug("Trigger Databricks job...")
    #         if self.trigger_job(pr_job_id):
    #             self.logger.debug("Trigger Databricks job...OK")
    #         else:
    #             return
            
    #     except Exception as e:
    #         self.logger.debug(f"Error in processiing the job: {str(e)}")
    #     finally:
    #         self.clean_up()


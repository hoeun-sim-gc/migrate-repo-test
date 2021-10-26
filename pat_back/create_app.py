import io
import zipfile
import logging
import json

import threading
from time import sleep
from random import random

import pandas as pd
from flask import Flask, request, send_file

from pat_back.settings import AppSettings

from .pat_helper import PatHelper
from .sql_helper import SqlHelper

def create_app(st_folder):
    app = Flask(__name__,static_url_path="/", static_folder=st_folder)

    @app.route('/')
    def index():
        return app.send_static_file('index.html')

    @app.route('/api/job', methods=['POST'])
    def submit_job():
        js = None
        data = None
        if request.files:
            if 'data' in request.files.keys():
                data = request.files['data']
        if request.form and 'para' in request.form.keys():
            js = json.loads(request.form['para'])
        else:
            js = request.json 

        if js:
            job_id = PatHelper.submit(js,data)
            if job_id and job_id > 0:
                wakeup_worker()
                return f'Analysis submitted: {job_id}'
    
        return "Submission failed!"

    @app.route('/api/wakeup', methods=['POST'])
    def wakeup_worker():
        sleep(random() * 5)
        PatHelper.workers = [filter(lambda x: x.is_alive(), PatHelper.workers)]
        if len(PatHelper.workers) < AppSettings.MAX_WORKERS: 
            d = threading.Thread(name=f'pat-worker', target=PatHelper.process_jobs,daemon=True)
            PatHelper.workers.append(d)
            d.start()            
 
        return "ok"
            
    @app.route('/api/job', methods=['GET'])
    def get_job_list():
        df = PatHelper.get_job_list()
        if df is not None and len(df) > 0:
            # return df.to_json()
            lst= df.to_dict('records')
            return json.dumps(df.to_dict('records'))

    @app.route('/api/job/<int:job_id>', methods=['GET'])
    def summary(job_id):
        df = PatHelper.get_summary(job_id)
        if df is not None and len(df) > 0:
            return json.dumps(df.to_dict('records'))

    @app.route('/api/valid/<int:job_id>', methods=['GET'])
    def get_validate_data(job_id):
        df1, df2, df3 = PatHelper.get_validation_data(job_id)
        return send_zip_file(f'pat_validation_{job_id}.zip', 
            ('pol_validation.csv', df1),
            ('loc_validation.csv', df2),
            ('fac_validation.csv', df3)
        )

    @app.route('/api/para/<int:job_id>', methods=['GET'])
    def get_job_para(job_id):
        ret= PatHelper.get_job_para(job_id)
        if ret:
            return ret

    @app.route('/api/status/<int:job_id>', methods=['GET'])
    def get_job_status(job_id):
        ret= PatHelper.get_job_status(job_id)
        if ret:
            return ret
    
    @app.route('/api/result/<job_lst>', methods=['GET'])
    def results(job_lst):
        lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
        lst= [a for a in filter(lambda x: x>0, lst)]

        if len(lst)>0:
            df = PatHelper.get_results(lst)
            if df is not None:
                return send_zip_file("pat_results.zip", ('pat_results.csv', df))

    @app.route('/api/stop/<job_lst>', methods=['POST'])
    def stop_job(job_lst):
        lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
        lst= [a for a in filter(lambda x: x>0, lst)]

        if len(lst)>0:
            PatHelper.stop_jobs(lst)
            
        return "ok"
    
    @app.route('/api/reset/<job_lst>', methods=['POST'])
    def reset_job(job_lst):
        lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
        lst= [a for a in filter(lambda x: x>0, lst)]

        if len(lst)>0:
            PatHelper.reset_jobs(lst)
            
        return "ok"

    @app.route('/api/run<int:job_id>', methods=['POST'])
    def run_job(job_id):
        sleep(random() * 5)
        PatHelper.workers = filter(lambda x: x.is_alive(), PatHelper.workers)
        if len(PatHelper.workers) < AppSettings.MAX_WORKERS: 
            d = threading.Thread(name=f'pat-worker', target=PatHelper.process_jobs, args=job_id, daemon=True)
            PatHelper.workers.append(d)
            d.start()            
 
        return "ok"

    @app.route('/api/job/<int:job_id>', methods=['DELETE'])
    def delete(job_id):
        df= PatHelper.delete(job_id)
        if df is not None:
            return df.to_json()

    def send_zip_file(name, *df_lst):
        try:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, False) as zf:
                for nm, df in df_lst:
                    if df is not None:
                        zf.writestr(nm, df.to_csv(header=True, index=False))
            zip_buffer.seek(0)
            return send_file(zip_buffer, mimetype='application/zip', attachment_filename=name, as_attachment=True, cache_timeout=0)
        except Exception as e:
            logging.warning(f"Download data file: \n{e}")
      
    
    @app.route('/api/db_list/<sever>', methods=['GET'])
    def get_db_list(sever):
        edm,rdm = SqlHelper.get_db_list(sever)
        return json.dumps( {'edm': edm.name.to_list(), 'rdm': rdm.name.to_list() })        

    @app.route('/api/anls/<sever>/<db>', methods=['GET'])
    def get_anls_list(sever, db):
        df = SqlHelper.get_anls_list(sever, db)
        if df is not None and len(df)>0:
            return json.dumps(df.to_dict('records'))

    @app.route('/api/port/<sever>/<db>', methods=['GET'])
    def get_port_list(sever, db):
        df = SqlHelper.get_port_list(sever, db)
        if df is not None and len(df)>0:            
            return json.dumps(df.to_dict('records'))

    @app.route('/api/peril/<sever>/<db>/<int:pid>', methods=['GET'])
    def get_peril_list(sever, db, pid):
        df = SqlHelper.get_peril_list(sever, db, pid)
        if df is not None and len(df)>0:            
            return json.dumps(df.policytype.to_list())

    return app
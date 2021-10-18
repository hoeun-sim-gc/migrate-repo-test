import io
import zipfile
import logging

import pandas as pd

import json
from flask import Flask, abort, request, send_file
from waitress import serve

from pat_sql import PatJob
from pat_common import SqlHelper


def create_app(st_folder):
    app = Flask(__name__,static_url_path="/", static_folder=st_folder)

    @app.route('/')
    def index():
        return app.send_static_file('index.html')

    @app.route('/api/Jobs', methods=['POST'])
    def submit_job():
        js = None
        data = None
        if request.files and 'data' in request.files.keys():
            data = request.files['data']
        if request.form and 'para' in request.form.keys():
            js = json.loads(request.form['para'])
        else:
            request.json 

        if js:
            job = PatJob(js,data)
            if job.job_id and job.job_id > 0:
                job.process_job_async()
                return f'Analysis submitted: {job.job_id}'
            
    @app.route('/api/Jobs', methods=['GET'])
    def get_job_list():
        df = PatJob.get_job_list()
        if df is not None and len(df) > 0:
            # return df.to_json()
            lst= df.to_dict('records')
            return json.dumps(df.to_dict('records'))

    @app.route('/api/Jobs/<int:job_id>', methods=['GET'])
    def summary(job_id):
        df = PatJob.get_summary(job_id)
        if df is not None and len(df) > 0:
            return json.dumps(df.to_dict('records'))

    @app.route('/api/Jobs/<int:job_id>/Validation', methods=['GET'])
    def get_validate_data(job_id):
        df1, df2, df3 = PatJob.get_validation_data(job_id)
        return send_zip_file(f'pat_validation_{job_id}.zip', 
            ('pol_validation.csv', df1),
            ('loc_validation.csv', df2),
            ('fac_validation.csv', df3)
        )

    @app.route('/api/Jobs/<int:job_id>/Para', methods=['GET'])
    def get_job_para(job_id):
        ret= PatJob.get_job_para(job_id)
        if ret:
            return ret

    @app.route('/api/Jobs/<int:job_id>/Status', methods=['GET'])
    def get_job_status(job_id):
        ret= PatJob.get_job_status(job_id)
        if ret:
            return ret
    
    @app.route('/api/Jobs/<int:job_id>/Result', methods=['GET'])
    def results(job_id):
        df = PatJob.get_results(job_id)
        if df is not None:
            return send_zip_file(f"pat_premium_{job_id}.zip", (f'pat_premium.csv', df))

    @app.route('/api/Jobs/<int:job_id>', methods=['DELETE'])
    def delete(job_id):
        df= PatJob.delete(job_id)
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

    
    
    
    @app.route('/api/dblist/<sever>', methods=['GET'])
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
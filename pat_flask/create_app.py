import io
import zipfile
import logging

import json
from flask import Flask, abort, request, send_file
from waitress import serve

from pat_sql import PatJob

def create_app(st_folder):
    app = Flask(__name__,static_url_path="/", static_folder=st_folder)

    @app.route('/')
    def index():
        return app.send_static_file('index.html')

    @app.route('/api/Jobs', methods=['POST'])
    def submit_job():
        js = request.json if request.json else (json.load(request.files['para']) if request.files else None)
        data = request.files['data'] if request.files and 'data' in request.files else None

        if js:
            job = PatJob(js,data)
            if job.job_id > 0:
                job.process_job_async()
                return f'Analysis submitted: {job.job_id}'
        
        abort(404)
            
    @app.route('/api/Jobs', methods=['GET'])
    def get_job_list():
        df = PatJob.get_job_list()
        if df is not None and len(df) > 0:
            return df.to_json()
        else:
            abort(404)

    @app.route('/api/Jobs/<int:job_id>', methods=['GET'])
    def summary(job_id):
        df = PatJob.get_summary(job_id)
        if df is not None and len(df) > 0:
            return df.to_json()
        else:
            abort(404)

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
        else:
            abort(404)
    
    @app.route('/api/Jobs/<int:job_id>/Result', methods=['GET'])
    def results(job_id):
        df = PatJob.get_results(job_id)
        if df is not None:
            return send_zip_file(f"pat_premium_{job_id}.zip", (f'pat_premium.csv', df))
        else:
            abort(404)

    @app.route('/api/Jobs/<int:job_id>', methods=['DELETE'])
    def delete(job_id):
        df= PatJob.delete(job_id)
        if df is not None:
            return df.to_json()
        else:
            abort(404)

    
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
            logging.warning(f"Download daat file: \n{e}")


    return app
import requests
from pat_flask import settings
from flask import Flask, abort, request
from requests.models import Request
from waitress import serve

from  .pat_job_mng import PatJobMng 

def create_app(st_folder):
    app = Flask(__name__,static_url_path="/", static_folder=st_folder)

    @app.route('/')
    def index():
        return app.send_static_file('index.html')

    @app.route('/api/Jobs', methods=['POST'])
    def submit_job():
        if request.files:
            job = PatJobMng(request.files)
            if job.analysis:
                job.process_job_async()
                return 'Analysis submitted!'
        
        abort(404)
            
    @app.route('/api/Jobs', methods=['GET'])
    def get_job_list():
        ret= PatJobMng.get_job_list()
        if ret:
            return ret
        else:
            abort(404)

    @app.route('/api/Jobs/<int:job_id>', methods=['GET'])
    def summary(job_id):
        ret= PatJobMng.get_summary(job_id)
        if ret:
            return ret
        else:
            abort(404)

   
    # @app.route('/api/Jobs/<int:job_id>/Validation', methods=['GET'])
    # def data_file(job_id):
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         ret= pm.download_data(job_id)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)


    @app.route('/api/Jobs/<int:job_id>/Para', methods=['GET'])
    def get_job_para(job_id):
        ret= PatJobMng.get_job_para(job_id)
        if ret:
            return ret
        else:
            abort(404)
    
    @app.route('/api/Jobs/<int:job_id>/Result', methods=['GET'])
    def results(job_id):
        ret= PatJobMng.get_results(job_id)
        if ret:
            return ret
        else:
            abort(404)

    @app.route('/api/Jobs/<int:job_id>', methods=['DELETE'])
    def delete(job_id):
        ret= PatJobMng.delete(job_id)
        if ret:
            return ret
        else:
            abort(404)

    # @app.route('/api/Jobs/<int:job_id>/Stop', methods=['PUT'])
    # def stop_job(job_id):
    #     if 'req_id' in request.args and 'load' in request.args:
    #         pm = PmAppJob(request)
    #         if pm.is_valid():
    #             load =  request.args.get('load') 
    #             ret= pm.stop_job(job_id, load and load.lower() == 'true')
    #             if ret:
    #                 return ret
    #         else:
    #             abort(404)
    #     else:
    #         abort(400)    

    



    return app
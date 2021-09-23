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
            

    # @app.route('/api/Jobs', methods=['GET'])
    # def get_jobs(jobtype):
    #     pm = PatJobMng(request)
    #     if pm.is_valid():
    #         ret= pm.get_job_list(jobtype)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)

    # @app.route('/api/Jobs/<int:job_id>/Para', methods=['GET'])
    # def get_job_para(job_id):
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         ret= pm.get_job_para(job_id)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)
    
    # @app.route('/api/Jobs/<int:job_id>/Priority', methods=['PUT'])
    # def change_priority(job_id):
    #     if 'req_id' in request.args and 'change' in request.args:
    #         pm = PmAppJob(request)
    #         if pm.is_valid():
    #             delta =  request.args.get('change') 
    #             ret= pm.change_job_priority(job_id, delta)
    #             if ret:
    #                 return ret
    #         else:
    #             abort(404)
    #     else:
    #         abort(400)

    # @app.route('/api/Jobs/<int:job_id>/Delete', methods=['PUT'])
    # def delete(job_id):
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         ret= pm.delete_job(job_id)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)

    # @app.route('/api/Jobs/<int:job_id>/Restart', methods=['PUT'])
    # def restart(job_id):
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         ret= pm.restart_job(job_id)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)

    # @app.route('/api/Jobs/<int:job_id>/Run', methods=['PUT'])
    # def run_job(job_id):
    #     if 'req_id' in request.args and 'worker' in request.args:
    #         pm = PmAppJob(request)
    #         if pm.is_valid():
    #             worker =  request.args.get('worker') 
    #             ret= pm.assign_job(job_id, worker)
    #             if ret:
    #                 return ret
    #         else:
    #             abort(404)
    #     else:
    #         abort(400)
        
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

    # @app.route('/api/Jobs/<int:job_id>/Result', methods=['GET'])
    # def results(job_id):
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         ret= pm.download_res(job_id)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)

    # @app.route('/api/Jobs/<int:job_id>/DataFile', methods=['GET'])
    # def data_file(job_id):
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         ret= pm.download_data(job_id)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)

    # @app.route('/api/Jobs', methods=['POST'])
    # def submit_job():
    #     pm = PmAppJob(request)
    #     if pm.is_valid():
    #         files={}
    #         for key in request.files:
    #             file = request.files[key]
    #             files[key] = file.read()
    #         ret= pm.submit_job(files)
    #         if ret:
    #             return ret
    #     else:
    #         abort(404)


    return app
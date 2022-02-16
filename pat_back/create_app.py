from importlib.abc import PathEntryFinder
import io
from os import path
from unicodedata import name
import zipfile
import logging
import json
from logging.config import fileConfig

import pandas as pd

from fastapi import FastAPI,Request, staticfiles, HTTPException, status
from fastapi.responses import RedirectResponse,StreamingResponse
from sqlalchemy import false

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.gzip import GZipMiddleware

from .pat_worker import PatWorker
from .pat_helper import PatHelper
from .sql_helper import SqlHelper

middleware = [
    Middleware(GZipMiddleware, minimum_size = 1000)
]

app = FastAPI(middleware=middleware)

site_path = path.join(path.dirname(__file__),'react_build')
app.mount("/site", staticfiles.StaticFiles(directory=site_path, html = True), name="site")

fileConfig('logging.cfg')
logging.info("Premium Allocation Tool service started!")   

@app.get('/')
def index():
    return RedirectResponse("/site")

@app.get('/hello')
def hello():
    return 'Hello from PAT!'

@app.get('/api/job')
def get_job_list(req_id:str=None):
    df = PatHelper.get_job_list(PatHelper.decode64(req_id))
    if df is not None and len(df) > 0:
        return df.to_dict('records')

@app.get('/api/job/{job_id}')
def get_job(job_id:int):
    return PatHelper.get_job(job_id)

def send_zip_file(name, *df_lst) -> StreamingResponse:
    try:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, False) as zf:
            for nm, df in df_lst:
                if df is not None:
                    zf.writestr(nm, df.to_csv(header=True, index=False, na_rep='NULL'))
        zip_buffer.seek(0)

        return StreamingResponse(zip_buffer, media_type='application/zip',headers={
            'Content-Disposition': f'attachment;filename={name}',
            'Access-Control-Expose-Headers': 'Content-Disposition'
        })
    except Exception as e:
        logging.warning(f"Download data file: \n{e}")

#very slow even with the GZIP middelware
def send_csv_file(name, df) -> StreamingResponse:
    try:
        if df is not None:
            buffer = io.BytesIO()
            df.to_csv(buffer, header=True, index=False)
            buffer.seek(0)
            return StreamingResponse(buffer, media_type='text/csv',headers={'Content-Disposition': 
                        f'attachment;filename={name}',
                        'Access-Control-Expose-Headers': 'Content-Disposition'})
    except Exception as e:
        logging.warning(f"Download data file: \n{e}")

@app.get('/api/data/{job_id}')
def data(job_id:int, data_type:str = 'results') -> StreamingResponse:
    df_lst=[]
    if data_type=='results':
        df = PatHelper.get_results(job_id)
        if df is not None: 
            df_lst.append((f"pat_results_{job_id}.csv", df))
    elif data_type=='unused':
        df_pol, df_fac, df_layers = PatHelper.get_unused(job_id)
        if df_pol is not None and len(df_pol) > 0: 
            df_lst.append((f"pat_unused_policy_{job_id}.csv", df_pol))
        if df_fac is not None and len(df_fac) > 0: 
            df_lst.append((f"pat_unused_fac_{job_id}.csv", df_fac))
        if df_layers is not None and len(df_layers) > 0: 
            df_lst.append((f"pat_unused_layers_{job_id}.csv", df_layers))
    elif data_type=='details':
        df_pol, df_fac, df_layers, df_facnet = PatHelper.get_data(job_id)
        if df_pol is not None and len(df_pol) > 0: 
            df_lst.append((f"pat_detail_policy_{job_id}.csv", df_pol))
        if df_fac is not None and len(df_fac) > 0: 
            df_lst.append((f"pat_detail_fac_{job_id}.csv", df_fac))
        if df_layers is not None and len(df_layers) > 0: 
            df_lst.append((f"pat_detail_layers_{job_id}.csv", df_layers))
        if df_facnet is not None and len(df_facnet) > 0: 
            df_lst.append((f"pat_net_of_fac_{job_id}.csv", df_facnet))
    
    if df_lst:
        return send_zip_file(f"pat_data_{job_id}.zip", *df_lst) 

@app.post('/api/job')
async def submit_job(request: Request, jobrun:bool = False) -> str:
    form  = await request.form()
    if isinstance(form['para'], str):
        js= json.loads(form['para'])
    else:
        js = json.loads(await form['para'].read())
    data = None
    try:
        data = await form["data"].read()
    except:
        pass    

    if js:
        if jobrun:
            ret = PatHelper.submit_run(js,data)
            if isinstance(ret,pd.DataFrame):
                df = ret.fillna(0)
                if df is not None and len(df) > 0:
                    # return send_csv_file(f"pat_res.csv", df)
                    return send_zip_file(f"pat_res.zip", ('pat_res.csv',df))
        else:
            job_id = PatHelper.submit(js,data)
            if job_id and job_id > 100:
                PatWorker.start_worker(job_id)
                return f'Analysis submitted: {job_id}'

    raise HTTPException(status_code=400, detail=f"Submit job failed!")

@app.post('/api/wakeup')
def wakeup_worker():
    PatWorker.start_worker()
    return "ok"

@app.post('/api/stop/{job_lst}')
def stop_job(job_lst:str)->str:
    lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
    lst= [a for a in lst if a>0]

    if len(lst)>0: PatWorker.stop_jobs(lst)
    
    return "ok"

@app.post('/api/reset/{job_id}')
def reset_job(job_id:int, keep_data:bool = False)->str:
    PatWorker.stop_jobs([job_id])
    PatHelper.reset_jobs(job_id, not keep_data)
    
    return "ok"
    
@app.post('/api/run/{job_id}')
def run_job(job_id:int)->str:
    if not PatWorker.is_runnung(job_id):
        PatHelper.reset_jobs(job_id)
        PatWorker.start_worker(job_id) 
        
        return "ok"
    else:
        return "Job is already ruuning"

@app.put('/api/rename/{job_id}/{new_name}')
def rename_job(job_id:int, new_name:str) -> str:
    PatHelper.rename_job(job_id, new_name)
    return "ok"

@app.put('/api/public-job/{job_id}')
def public_job(job_id:int,req_id:str=None) -> str:
    req_id = PatHelper.decode64(req_id)
    if not PatHelper.is_admin(req_id):
        raise HTTPException(status_code=401, detail="Permission not allowed")
        
    PatHelper.public_job(job_id)
    return "ok"    

@app.delete('/api/job/{job_lst}')
def delete(job_lst:str, req_id:str=None)->str:
    req_id = PatHelper.decode64(req_id)
    if not PatHelper.is_admin(req_id):
        raise HTTPException(status_code=401, detail="Permission not allowed")

    lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
    lst= [a for a in lst if a>0]

    if len(lst)>0:
        df= PatHelper.delete(lst)
        if df is not None:
            return df.to_dict('records')
    
@app.get('/api/db_list/{sever}')
def get_db_list(sever:str):
    edm,rdm = SqlHelper.get_db_list(sever)
    return {'edm': edm.name.to_list(), 'rdm': rdm.name.to_list() }

@app.get('/api/anls/{sever}/{db}')
def get_anls_list(sever:str, db:str):
    df = SqlHelper.get_anls_list(sever, db)
    if df is not None and len(df)>0:
        return df.to_dict('records')

@app.get('/api/port/{sever}/{db}')
def get_port_list(sever:str, db:str):
    df = SqlHelper.get_port_list(sever, db)
    if df is not None and len(df)>0:            
        return df.to_dict('records')

@app.get('/api/peril/{sever}/{db}/{pid}')
def get_peril_list(sever:str, db:str, pid:int):
    df = SqlHelper.get_peril_list(sever, db, pid)
    if df is not None and len(df)>0:            
        return df.policytype.to_list()

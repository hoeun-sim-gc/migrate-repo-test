import io
from os import path
import zipfile
import logging
import json
from logging.config import fileConfig

import pandas as pd

from fastapi import FastAPI,Request, staticfiles, HTTPException
from fastapi.responses import RedirectResponse,StreamingResponse

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
def get_job_list(user:str=None):
    df = PatHelper.get_job_list(user)
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
                    zf.writestr(nm, df.to_csv(header=True, index=False))
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

@app.get('/api/result/{job_lst}')
def results(job_lst:str) -> StreamingResponse:
    lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
    lst= [a for a in lst if a>0]

    if len(lst)>0:
        df = PatHelper.get_results(lst)
        if df is not None:
            return send_zip_file(f"pat_res_{lst[0]}.zip", (f"pat_res_{lst[0]}.csv", df))

@app.get('/api/valid/{job_id}')
def get_validate_data(job_id:int, flagged:bool=True) -> StreamingResponse:
    df1, df2 = PatHelper.get_validation_data(job_id, flagged)
    return send_zip_file(f'pat_validation_{job_id}.zip', 
        ('pol_validation.csv', df1),
        ('fac_validation.csv', df2)
    )

@app.post('/api/job')
async def submit_job(request: Request) -> str:
    form  = await request.form()
    js = json.loads(form['para']) 
    data = None
    try:
        data = await form["data"].read()
    except:
        pass    

    if js:
        job_id = PatHelper.submit(js,data)
        if job_id and job_id > 0:
            wakeup_worker()
            return f'Analysis submitted: {job_id}'

    return "Submission failed!"

@app.post('/api/jobrun')
async def submit_jobrun(request: Request):
    try:
        form  = await request.form()
        if isinstance(form['para'], str):
            js= json.loads(form['para'])
        else:
            js = json.loads(await form['para'].read())

        s = str(await form["data"].read(),'utf-8')
        data = pd.read_csv(io.StringIO(s))

        if js and data is not None:
            ret = PatHelper.submit_run(js,data)
            if isinstance(ret,pd.DataFrame):
                df = ret.fillna(0)
                if df is not None and len(df) > 0:
                    # return send_csv_file(f"pat_res.csv", df)
                    return send_zip_file(f"pat_res.zip", ('pat_res.csv',df))
            
            raise HTTPException(
            status_code=400, detail=f"Error encountered!")
    except Exception as e:
        logging.warning(f"Run job error: \n{e}")
        raise HTTPException(
            status_code=400, detail=f"Error encountered!"
        )

@app.route('/api/wakeup', methods=['POST'])
def wakeup_worker():
    PatWorker.start_worker()
    return "ok"

@app.post('/api/stop/{job_lst}')
def stop_job(job_lst:str)->str:
    lst= [int(job) if job.isdigit() else 0 for job in job_lst.split('_')]
    lst= [a for a in lst if a>0]

    if len(lst)>0:
        PatWorker.stop_jobs(lst)
        PatHelper.cancel_jobs(lst)
        
    return "ok"
    
@app.post('/api/run/{job_id}')
def run_job(job_id:int)->str:
    PatWorker.stop_jobs([job_id])
    PatHelper.reset_jobs([job_id])
    PatWorker.start_worker(job_id)     
    return "ok"

@app.put('/api/rename/{job_id}/{new_name}')
def rename_job(job_id:int, new_name:str) -> str:
    PatHelper.rename_job(job_id, new_name)
    return "ok"

@app.put('/api/public-job/{job_id}')
def public_job(job_id:int) -> str:
    PatHelper.public_job(job_id)
    return "ok"    

@app.delete('/api/job/{job_id}')
def delete(job_id: int):
    df= PatHelper.delete(job_id)
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

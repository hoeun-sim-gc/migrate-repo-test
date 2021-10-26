from os import environ
from dotenv import load_dotenv

load_dotenv()

class AppSettings(object):
    PAT_JOB_SVR = environ.get('PAT_JOB_SVR')
    PAT_JOB_DB = environ.get('PAT_JOB_DB')
    PAT_JOB_USR = environ.get('PAT_JOB_USR')
    PAT_JOB_PWD = environ.get('PAT_JOB_PWD')

    PAT_WORKER= environ.get('PAT_WORKER')

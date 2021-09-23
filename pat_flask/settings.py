from os import environ
from dotenv import load_dotenv

load_dotenv()

class AppSettings(object):

    PAT_JOB_CONN = f'''DRIVER={{SQL Server}};Server={environ.get('PAT_JOB_SVR')};Database={environ.get('PAT_JOB_SVR')};
            User Id={environ.get('PAT_JOB_USR')};Password={environ.get('PAT_JOB_PWD')};
            MultipleActiveResultSets=true;'''

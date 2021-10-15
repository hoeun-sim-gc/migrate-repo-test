import os
import json
import logging
import threading
from datetime import datetime
import zipfile

import numpy as np
import pandas as pd

import pyodbc
from bcpandas import SqlCreds, to_sql

from pat_common import AppSettings, PatFlag

class SqlHelper:
    """Class to provide SQL utilities"""

    @classmethod
    def get_db_list(cls, svr):
        conn_str = f'''DRIVER={{SQL Server}};Server={svr};User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};'''
        with pyodbc.connect(conn_str) as conn:
            edm = pd.read_sql_query(f"""select name 
                    from sys.databases 
                    where case when state_desc = 'online' then object_id(quotename(name) + '.[dbo].[accgrp]', 'u') end is not null
                    order by name""", conn)

            rdm = pd.read_sql_query(f"""select name 
                    from sys.databases 
                    where case when state_desc = 'online' then object_id(quotename(name) + '.[dbo].[rdm_analysis]', 'u') end is not null
                    order by name""", conn)

            return (edm, rdm)

    @classmethod
    def get_anls_list(cls, svr, db):
        conn_str = f'''DRIVER={{SQL Server}};Server={svr};Database={db};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};'''
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(f"""select id, name, description, type 
                    from [{db}].dbo.rdm_analysis
                    order by id""", conn)

            return df
 
    @classmethod
    def get_port_list(cls, svr, db):
        conn_str = f'''DRIVER={{SQL Server}};Server={svr};Database={db};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};'''
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(f"""select portinfoid, portnum, portname, descript
                    from [{db}].dbo.portinfo
                    order by portinfoid""", conn)

            return df

    
    @classmethod
    def get_peril_list(cls, svr, db, pid):
        conn_str = f'''DRIVER={{SQL Server}};Server={svr};Database={db};
            User Id={AppSettings.PAT_JOB_USR};Password={AppSettings.PAT_JOB_PWD};'''
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(f"""select distinct policytype 
                from [{db}].dbo.policy a
                    join portacct b on a.accgrpid =b.accgrpid
                where b.portinfoid = {pid}
                order by policytype""", conn)

            return df

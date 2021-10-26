from flask import Flask, abort, request
from flask.signals import appcontext_tearing_down
from waitress import serve
import logging
from logging.config import fileConfig

from pat_back import create_app

app = create_app('react_build')
# app.config["DEBUG"] = True

# logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARN)
logging.getLogger("werkzeug").setLevel(logging.WARN)
fileConfig('logging.cfg')
app.logger.info("Premium Allocation Tool service started!")    

if __name__ == '__main__':
    # app.run(debug=True)
    serve(app, host='0.0.0.0', port=8002, threads=10) #WAITRESS!  

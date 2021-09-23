from flask import Flask, abort, request
from flask.signals import appcontext_tearing_down
from waitress import serve
from pat_flask import create_app

app = create_app('../static')
app.config["DEBUG"] = True

if __name__ == '__main__':
    # app.run(debug=True)
    serve(app, host='0.0.0.0', port=5000, threads=1) #WAITRESS!  
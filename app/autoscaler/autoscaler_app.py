from flask import Flask, request
from app.autoscaler.autoscaler import AutoScaler
import logging

# Configure Flask APP
autoscalerapp = Flask(__name__, static_folder='../static')
autoscaler = AutoScaler()

# Define top level module logger
logger = logging.getLogger(__name__)
logger.info("START AUTOSCLAER APP")


@autoscalerapp.route('/')
def home():
    msg = "Autoscaler App"
    return '<html><body><h1><i>{}</i></h1></body></html>'.format(msg)


@autoscalerapp.route('/refresh_configuration', methods=['GET'])
def get():
    autoscaler.refresh_configuration()
    return {"success": True}

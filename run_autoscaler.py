#!../venv/bin/python
from app.autoscaler.autoscaler_app import autoscalerapp
autoscalerapp.run('0.0.0.0', 5002, debug=True)


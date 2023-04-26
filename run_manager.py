#!../venv/bin/python
from app.manager.manager_app import managerapp
managerapp.run('0.0.0.0', 5001, debug=True)


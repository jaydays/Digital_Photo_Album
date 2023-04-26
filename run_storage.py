#!../venv/bin/python
from app.storage.storage_app import storageapp
storageapp.run('0.0.0.0', 5003, debug=True)


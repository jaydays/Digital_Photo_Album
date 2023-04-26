#!../venv/bin/python
from app.frontend.frontend_app import frontendapp
frontendapp.run('0.0.0.0', 5000, debug=True)

#!../venv/bin/python
from app.memcache.memcache_app import memcacheapp
memcacheapp.run('0.0.0.0', 5004, debug=True)


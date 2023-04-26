from flask import Flask, request
from app.storage.rds import RDS
from app.storage.s3 import S3
import jsonpickle
import io
import base64

from app.common import ReplacementPolicy, CacheConfig
import logging

# Configure Flask APP
storageapp = Flask(__name__)
rds = RDS()
s3 = S3()
rds.create_tables()

@storageapp.route('/')
def home():
    msg = "Storage App"
    return '<html><body><h1><i>{}</i></h1></body></html>'.format(msg)

@storageapp.route("/api/store_image", methods = ['POST'])
def store_image():
    # key goes to RDS
    print("MADE IT HERE 4")
    key          = request.form.get('key')
    print("key from storage app = ", key)
    print("MADE IT HERE 5")
    # filename used in both
    img_filename = request.form.get('img_filename')
    print("Filename from storage app = ", img_filename)
    print("MADE IT HERE 6")

    # file goes to S3
    img_file = request.files['img_file']
    print("MADE IT HERE 7")
    rds.add_key(key, img_filename)
    print("MADE IT HERE 8")
    s3.upload(img_filename, img_file)

    return {"success":True}


@storageapp.route("/api/get_image_url", methods=['POST', 'GET'])
def get_image_url():
    key = request.form.get('key')

    #Query RDS to get imagename
    img_name = rds.get_img_path(key)
    print(img_name)

    #get path to image by concating base_s3_url with img_name
    base_s3_url = "https://g15a2.s3.amazonaws.com/"
    img_url = base_s3_url + img_name
    print(img_url)

    return {"success": True,
            "img_url": img_url,
            }

@storageapp.route("/api/delete_all", methods = ['POST'])
def delete_all():
    rds.delete_all()
    print("delete all in rds")
    s3.delete_all()
    print("delete all in s3")
    return {"success":True}

@storageapp.route("/api/save_autoscaler", methods = ['POST'])
def save_autoscaler_config():
    data = request.data
    scaler_config = jsonpickle.decode(data)
    rds.add_autoscaler_config(scaler_config)
    return {"success":True}

@storageapp.route("/api/get_autoscaler", methods = ['POST', 'GET'])
def get_most_recent_autoscaler_config():
    config = rds.get_most_recent_autoscaler_config()
    data = jsonpickle.encode(config)
    return {"success": True,
            "data": data,
            }

@storageapp.route("/api/save_cache", methods = ['POST'])
def save_cache_config():
    data = request.data
    cache_config = jsonpickle.decode(data)
    rds.add_cache_config(cache_config)
    return {"success":True}

@storageapp.route("/api/get_cache", methods = ['POST', 'GET'])
def get_most_recent_cache_config():
    config = rds.get_most_recent_cache_config()
    data = jsonpickle.encode(config)
    return {"success": True,
            "data": data,
            }

@storageapp.route("/api/get_all_keys", methods = ['POST', 'GET'])
def get_keys():
    keys = rds.get_all_keys()
    print("Keys from Storage app is ", keys)
    return {"success": True,
            "keys": keys,
            }


# HELPER FUNCTIONS
def fig_to_base64(fig):
    img = io.BytesIO()
    fig.savefig(img, format='png',
                bbox_inches='tight')
    img.seek(0)

    return base64.b64encode(img.getvalue())


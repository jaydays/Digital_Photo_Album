from flask import Flask
from flask import render_template, request
import requests
from werkzeug.utils import secure_filename
from PIL import Image
import base64
from io import BytesIO
import os
import logging
from app.apis import *
import socket
from app.boto_utils import *

pool_sizes = ["","",""] # Init with 3 empty messages

# Configure Flask APP
frontendapp = Flask(__name__, static_folder='../static')

# Define Upload Folder Path
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join('../static', 'uploads')

# Define secret key to enable session
frontendapp.secret_key = 'This is the secret key to utilize session in Flask'

# Define top level module logger
logger = logging.getLogger(__name__)
logger.info("START FRONTEND APP")

# Management url
management_url = "http://127.0.0.1:5001/"

# JUST FOR TESTING PURPOSES
ignore_cache = False

@frontendapp.route('/')
def index():
    this_ip = DEFAULT_IP
    print("THIS IP IS ",this_ip)
    update_t = pool_sizes[-1]
    update_t_1 = pool_sizes[-2]
    update_t_2 = pool_sizes[-3]
    return render_template("home_page.html", update1=update_t, update2=update_t_1, update3=update_t_2, url = "http://"+this_ip+":5001/home_page")

@frontendapp.route('/home_page')
def home_page():
    logger.debug("Launched home page.")
    this_ip = DEFAULT_IP
    print("THIS IP IS ",this_ip)
    update_t = pool_sizes[-1]
    update_t_1 = pool_sizes[-2]
    update_t_2 = pool_sizes[-3]
    return render_template("home_page.html", update1=update_t, update2=update_t_1, update3=update_t_2,
                           url="http://" + this_ip + ":5001/home_page")

# UPLOAD
@frontendapp.route('/upload')
def upload():
    return render_template('upload.html')

@frontendapp.route('/upload_pic', methods=['POST'])
def upload_pic():
    '''
        Note: RDS is a dictionary of (hashed_key: image_name)
              S3 needs image_name to retrieve the image.
              Call Flow will be:
                - Querying RDS with a hashed key, retrieving image_name
                - taking image_name, inputting it into S3 to retrieve actual Image
    '''

    # Upload to database rather than mock. call api
    # will be StorageApi.store_img(key, img_data)
    # then fill in Storage API with request to storage app.

    key       = request.form.get('key')  # Get Key From User
    print(key)
    if(not key.isalnum()):
        return render_template('invalid_key.html')
    img_file = request.files['myfile']  # Get Image from user
    img_filename = secure_filename(img_file.filename) # secure filename returns a secure version of existing name
    ManagerApi.invalidate(key)
    StorageApi.store_img(key, img_filename, img_file) # call function to make db query
    return render_template('success.html', message="Uploaded Image successfully")


# RETRIEVE
@frontendapp.route('/retrieve')
def retrieve():
    return render_template("retrieve.html")

#FROM DATA
@frontendapp.route('/display_image', methods=['POST'])
def display_image():

    key = request.form.get('key')  # Get Key From User
    print("Displayed Key:", key)

    # Call ManagerApi.get to see if img is in cache
    img_data = ManagerApi.get(key) # should return None if image key not found

    # If Image not in cache then call database
    if (img_data == None):
        if key not in StorageApi.get_keys():
            return render_template('unknownKey.html')

        img_url  = StorageApi.get_img_url(key)
        response = requests.get(img_url)
        img = Image.open(BytesIO(response.content))

        # Encode the image data in base64
        img_buffer = BytesIO()
        try:
            img.save(img_buffer, format="JPEG")
        except:
            print("Failed to save JPEG, will try png")
            img.save(img_buffer, format="PNG")
        img_data = img_buffer.getvalue()
        encoded_img_data = base64.b64encode(img_data).decode('utf-8')

        ManagerApi.put(key, encoded_img_data)  # Call ManagerApi.put to save encoded_img in cache
        print("displaying from DB")
        return render_template('display_Image_from_data.html', filename=encoded_img_data)

    else:
        print("displaying from memcache")
        return render_template('display_Image_from_data.html', filename=img_data)


#FROM URL
@frontendapp.route('/display_image_working', methods=['POST'])
def display_image_working():

    key = request.form.get('key')  # Get Key From User
    print("Displayed Key:", key)

    # Call ManagerApi.get to see if img is in cache
    img_url = ManagerApi.get(key)

    # If Image not in cache then call database
    if (img_url == None):
        if key not in StorageApi.get_keys():
            return render_template('unknownKey.html')

        img_url = StorageApi.get_img_url(key)
        print("front_end retrieved_url", img_url)
        ManagerApi.put(key, img_url)

    return render_template('display_image.html', user_image=img_url)


# DATABASE
@frontendapp.route('/show_contents')
def show_contents():
    db_keys_string = 'EMPTY'
    db_keys = StorageApi.get_keys()
    if db_keys is not None and len(db_keys) > 0:
        db_keys_string = ', '.join(db_keys)

    mc_keys_string = "EMPTY"
    mc_keys = ManagerApi.get_all_keys()
    if mc_keys is not None and len(mc_keys) > 0:
        mc_keys_string = ', '.join(mc_keys)
    return render_template('show_contents.html', key_data_db = db_keys_string, key_data_mc = mc_keys_string)

# APIs
@frontendapp.route('/api/notify_pool_size_change', methods = ['POST'])
def pool_size_change():
    timestamp = request.form.get('timestamp')
    capacity = request.form.get('capacity')
    replacement_policy = request.form.get('replacement_policy')
    autoscaler_config = request.form.get('autoscaler_config')
    pool_size = request.form.get('pool_size')

    message = "Time: {} | {} Active Nodes | Cap: {} | Rep Policy: {} | AutoScaling: {}".format(timestamp, pool_size, capacity, replacement_policy, autoscaler_config)
    print(message)
    pool_sizes.append(message)
    return {"success": "true"}

@frontendapp.route('/api/getNumNodes', methods = ['POST'])
def api_getNumNodes():
    num_nodes = ManagerApi.get_num_active_nodes()
    if num_nodes is not None:
        return {"success": "true",
                "numNodes": num_nodes}
    else:
        return {"success": "false",
                "error": "Failed to Retrieve number of nodes"}

@frontendapp.route('/api/getRate', methods = ['POST'])
def api_getRate():
    parameters = request.args
    rate_type = parameters.get('rate')
    rate = ManagerApi.get_rate(rate_type)
    if rate is None:
        return {
            "success": "false",
            "error": {
                "code": 404,
                "message": "Rate either not found or undefined"
            }
        }

    return {
        "success": "true",
         "rate": rate_type,
         "value": rate,
    }

@frontendapp.route('/api/configure_cache', methods = ['POST'])
def api_configure_cache():
    parameters = request.args
    mode = parameters.get('mode')
    numNodes = parameters.get('numNodes')
    cacheSize = parameters.get('cacheSize')
    policy = parameters.get('policy')
    exp_ratio_string = parameters.get('expRatio')
    shrink_ratio_string = parameters.get('shrinkRatio')
    max_miss_string = parameters.get('maxMiss')
    min_miss_string = parameters.get('minMiss')

    exp_ratio = None if exp_ratio_string is None else float(exp_ratio_string)
    shrink_ratio = None if shrink_ratio_string is None else float(shrink_ratio_string)
    max_miss = None if max_miss_string is None else float(max_miss_string)
    min_miss = None if min_miss_string is None else float(min_miss_string)
    if mode == "manual":
        ManagerApi.set_num_active_nodes(int(numNodes))
        autoscale_config = AutoScalerConfig(resizing_policy=Resizingpolicy.MANUAL, max_miss_rate=max_miss,
                                      min_miss_rate=min_miss,
                                      shrink_factor=shrink_ratio, growth_factor=exp_ratio)
    elif mode == "auto":
        autoscale_config = AutoScalerConfig(resizing_policy=Resizingpolicy.AUTO, max_miss_rate=max_miss,
                                      min_miss_rate=min_miss,
                                      shrink_factor=shrink_ratio, growth_factor=exp_ratio)
    else:
        return {
            "success": "false",
            "error": {
                "code": 404,
                "message": "Failed to Configure Autoscaler"
            }
        }
    StorageApi.save_autoscaler_config(autoscale_config)
    AutoScalerApi.refresh_config()

    if policy == "RR":
        replacement_policy = ReplacementPolicy.RANDOM
    elif policy == "LRU":
        replacement_policy = ReplacementPolicy.LRU
    else:
        return {
            "success": "false",
            "error": {
                "code": 404,
                "message": "Failed to Configure Cache"
            }
        }

    cache_config = CacheConfig(replacement_policy=replacement_policy,
                               max_size_mb=int(cacheSize),
                               max_num_items=None)

    ManagerApi.set_configuration(cache_config)
    StorageApi.save_cache_config(cache_config)
    response = {
                    "success": "true",
                    "mode": mode,
                    "numNodes": int(numNodes),
                    "cacheSize": int(cacheSize),
                    "policy": policy
                }
    return response


@frontendapp.route('/api/delete_all', methods = ['POST'])
def api_delete_all():
    ManagerApi.clear()
    StorageApi.delete_all()
    return {"success": "true"}

@frontendapp.route('/api/list_keys', methods = ['POST'])
def api_list_keys():
    keys = StorageApi.get_keys()
    if keys == None: keys = []
    return {"success": "true",
            "keys":keys}

@frontendapp.route('/api/upload', methods = ['POST'])
def api_upload():

    key = request.form.get('key')  # Get Key From API call
    img_file = request.files.get('file')  # Get Image from API call

    img_filename = secure_filename(img_file.filename)  # secure filename returns a secure version of existing name
    StorageApi.store_img(key, img_filename, img_file)  # call function to make db query
    response = {
                "success": "true",
                "key":key
                }
    return response
@frontendapp.route('/api/key/<key_value>', methods = ['POST'])
def api_key(key_value):

    key = key_value
    # Call ManagerApi.get to see if img is in cache
    img_data = ManagerApi.get(key)  # should return None if image key not found

    # If Image not in cache then call database
    if (img_data == None):
        if key not in StorageApi.get_keys():
            return {
                "success": "false",
                "error": {
                    "code": 404,
                    "message": "Key Not Found"
                }
            }

        img_url = StorageApi.get_img_url(key)
        response = requests.get(img_url)
        img = Image.open(BytesIO(response.content))

        # Encode the image data in base64
        img_buffer = BytesIO()
        img.save(img_buffer, format="JPEG")
        img_data = img_buffer.getvalue()
        encoded_img_data = base64.b64encode(img_data).decode('utf-8')
        img_data = encoded_img_data
        ManagerApi.put(key, encoded_img_data)  # Call ManagerApi.put to save encoded_img in cache
        print("returning from DB")
    else:
        print("returning from memcache")

    response = {
                "success": "true",
                "key" : key,
                "content" : img_data
                }
    return response

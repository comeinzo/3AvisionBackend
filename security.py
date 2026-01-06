# import json
# import os
# from cryptography.fernet import Fernet

# FERNET_KEY = os.getenv("DATA_CONFIG_ENCRYPTION_KEY").encode()
# fernet = Fernet(FERNET_KEY)

# def encrypt_config(config_dict):
#     """
#     Encrypt config dictionary (source/destination)
#     """
#     if config_dict is None:
#         return None
#     json_data = json.dumps(config_dict)
#     encrypted = fernet.encrypt(json_data.encode())
#     return encrypted.decode()  # store as TEXT

# def decrypt_config(encrypted_str):
#     """
#     Decrypt config dictionary (used internally only)
#     """
#     if encrypted_str is None:
#         return None
#     decrypted = fernet.decrypt(encrypted_str.encode())
#     return json.loads(decrypted.decode())

import json
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# ✅ Load .env file
load_dotenv()

FERNET_KEY = os.getenv("DATA_CONFIG_ENCRYPTION_KEY")

if not FERNET_KEY:
    raise RuntimeError("❌ DATA_CONFIG_ENCRYPTION_KEY not found in environment")

fernet = Fernet(FERNET_KEY.encode())


def encrypt_config(config_dict):
    if config_dict is None:
        return None
    json_data = json.dumps(config_dict)
    encrypted = fernet.encrypt(json_data.encode())
    return encrypted.decode()


def decrypt_config(encrypted_str):
    if encrypted_str is None:
        return None
    decrypted = fernet.decrypt(encrypted_str.encode())
    return json.loads(decrypted.decode())
import json
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# ✅ Load .env file
load_dotenv()

FERNET_KEY = os.getenv("DATA_CONFIG_ENCRYPTION_KEY")

if not FERNET_KEY:
    raise RuntimeError("❌ DATA_CONFIG_ENCRYPTION_KEY not found in environment")

fernet = Fernet(FERNET_KEY.encode())


def encrypt_config(config_dict):
    if config_dict is None:
        return None
    json_data = json.dumps(config_dict)
    encrypted = fernet.encrypt(json_data.encode())
    return encrypted.decode()


def decrypt_config(encrypted_str):
    if encrypted_str is None:
        return None
    decrypted = fernet.decrypt(encrypted_str.encode())
    return json.loads(decrypted.decode())

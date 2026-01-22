import json
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# ✅ Load .env file
load_dotenv()

FERNET_KEY = os.getenv("DATA_CONFIG_ENCRYPTION_KEY")

if not FERNET_KEY:
    raise RuntimeError("❌ DATA_CONFIG_ENCRYPTION_KEY not found in environment")

# Ensure padding if missing
if not FERNET_KEY.endswith("="):
    FERNET_KEY += "="

try:
    fernet = Fernet(FERNET_KEY.encode())
except Exception as e:
    raise RuntimeError(f"❌ Invalid Fernet Key: {e}")


def encrypt_config(config_dict):
    if config_dict is None:
        return None
    json_data = json.dumps(config_dict)
    encrypted = fernet.encrypt(json_data.encode())
    return encrypted.decode()


def decrypt_config(encrypted_str):
    if encrypted_str is None:
        return None
    try:
        decrypted = fernet.decrypt(encrypted_str.encode())
        return json.loads(decrypted.decode())
    except Exception:
        return None

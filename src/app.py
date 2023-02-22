import json
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import udf, sproc, udtf, pandas_udf
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

# need the following so I can use the key-pair connection, instead of user/password
from cryptography.hazmat.primitives import serialization
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa


# requires credentials like this:
# {
#     "account": "account",
#     "user": "username",
#     "private_key_path":"some_path/to/key.p8",
#     "role": "rolename",
#     "database": "dbname",
#     "schema": "schemaname",
#     "warehouse": "whname"
# }

def get_keypair_session(credsPath: str) -> Session:
    
    # Reading Snowflake Connection Details
    snowflake_connection_cfg = json.loads(open(credsPath).read())

    # Handling the key
    with open(snowflake_connection_cfg["private_key_path"], "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )
        
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
        
    snowflake_connection_cfg["private_key"] = pkb    

    # Creating Snowpark Session
    return Session.builder.configs(snowflake_connection_cfg).create()
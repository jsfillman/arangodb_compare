import os
from typing import Optional
from arango import ArangoClient
from arango.database import StandardDatabase

## Variable Names To Import from ENV
# ARANGO_DB_NAME1
# ARANGO_DB_NAME2
# ARANGO_PASSWORD1
# ARANGO_PASSWORD2
# ARANGO_URL1
# ARANGO_URL2
# ARANGO_USERNAME1
# ARANGO_USERNAME2
# RUNTIME (production runs script, development prints to console)
##

def get_env_variable(var_name: str, default: Optional[str] = None) -> str:
    value = os.getenv(var_name)
    if value is None:
        if default is not None:
            return default
        raise ValueError(f"Environment variable {var_name} is not set and no default value is provided.")
    return value

def connect_to_arango(url: str, db_name: str, username: str, password: str) -> StandardDatabase:
    client = ArangoClient(hosts=url)
    return client.db(db_name, username=username, password=password)

def main() -> None:
    arango_url1: str = get_env_variable("ARANGO_URL1", "http://localhost:8529")
    arango_db_name1: str = get_env_variable("ARANGO_DB_NAME1", "test_db1")
    arango_username1: str = get_env_variable("ARANGO_USERNAME1", "root")
    arango_password1: str = get_env_variable("ARANGO_PASSWORD1", "passwd")

    arango_url2: str = get_env_variable("ARANGO_URL2", "http://localhost:8529")
    arango_db_name2: str = get_env_variable("ARANGO_DB_NAME2", "test_db2")
    arango_username2: str = get_env_variable("ARANGO_USERNAME2", "root")
    arango_password2: str = get_env_variable("ARANGO_PASSWORD2", "passwd")

    db1: StandardDatabase = connect_to_arango(arango_url1, arango_db_name1, arango_username1, arango_password1)
    db2: StandardDatabase = connect_to_arango(arango_url2, arango_db_name2, arango_username2, arango_password2)

    # Print a list of collections in both databases
    print("Collections in db1:", db1.collections())
    print("Collections in db2:", db2.collections())

if __name__ == "__main__":
    main()

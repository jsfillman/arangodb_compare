import os
import json
import random
from datetime import datetime
from deepdiff import DeepDiff
from typing import Dict, Any, Set, Tuple, List
from arango import ArangoClient
from arango.database import StandardDatabase

# Utility Functions

def get_env_variable(var_name: str, default: str) -> str:
    value = os.getenv(var_name)
    if value is None:
        return default
    return value

def connect_to_arango(url: str, db_name: str, username: str, password: str) -> Tuple[ArangoClient, StandardDatabase]:
    client = ArangoClient(hosts=url)
    db = client.db(db_name, username=username, password=password)
    return client, db

def connect_to_arango_databases() -> Tuple[Tuple[ArangoClient, StandardDatabase], Tuple[ArangoClient, StandardDatabase]]:
    arango_url1 = get_env_variable("ARANGO_URL1", "http://localhost:8529")
    arango_db_name1 = get_env_variable("ARANGO_DB_NAME1", "test_db1")
    arango_username1 = get_env_variable("ARANGO_USERNAME1", "root")
    arango_password1 = get_env_variable("ARANGO_PASSWORD1", "passwd")

    arango_url2 = get_env_variable("ARANGO_URL2", "http://localhost:8529")
    arango_db_name2 = get_env_variable("ARANGO_DB_NAME2", "test_db2")
    arango_username2 = get_env_variable("ARANGO_USERNAME2", "root")
    arango_password2 = get_env_variable("ARANGO_PASSWORD2", "passwd")

    client1, db1 = connect_to_arango(arango_url1, arango_db_name1, arango_username1, arango_password1)
    client2, db2 = connect_to_arango(arango_url2, arango_db_name2, arango_username2, arango_password2)

    return (client1, db1), (client2, db2)

def setup_logging_directory() -> Tuple[str, str]:
    base_log_dir = get_env_variable("LOGFILE_OUT", os.getcwd())
    arango_db_name1 = get_env_variable("ARANGO_DB_NAME1", "test_db1")
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    log_dir = os.path.join(base_log_dir, f"{arango_db_name1}_{timestamp}")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir, timestamp

def write_log(log_dir: str, entity_type: str, content: str) -> None:
    log_file = os.path.join(log_dir, f"{entity_type}.md")
    with open(log_file, 'a') as file:
        file.write(content + "\n")

# DB Entity Checks

def get_entity_names(db: StandardDatabase, entity_type: str) -> List[str]:
    return [item['name'] for item in getattr(db, entity_type)()]

def get_db_entity_counts(db1: StandardDatabase, db2: StandardDatabase) -> None:
    entity_types = ["analyzers", "graphs", "views", "collections"]

    for entity in entity_types:
        names_db1 = get_entity_names(db1, entity)
        names_db2 = get_entity_names(db2, entity)

        count_db1 = len(names_db1)
        count_db2 = len(names_db2)

        print(f"{entity.capitalize()} Count - DB1: {count_db1}, DB2: {count_db2}")

        if count_db1 != count_db2:
            print(f"\tMismatch found in {entity} counts.")
            # print(f"DB1 {entity} names: {sorted(names_db1)}")
            # print(f"DB2 {entity} names: {sorted(names_db2)}")

            unique_to_db1 = set(names_db1) - set(names_db2)
            unique_to_db2 = set(names_db2) - set(names_db1)

            if unique_to_db1:
                print(f"\tUnique to DB1 {entity}: {sorted(unique_to_db1)}")
            if unique_to_db2:
                print(f"\tUnique to DB2 {entity}: {sorted(unique_to_db2)}")

# Main function
def main() -> None:
    (client1, db1), (client2, db2) = connect_to_arango_databases()
    # log_dir, timestamp = setup_logging_directory()

    # Call to get entity counts
    get_db_entity_counts(db1, db2)

if __name__ == "__main__":
    main()


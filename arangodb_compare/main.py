import os
import json
from deepdiff import DeepDiff
from typing import Dict, Any, Set, Tuple
from arango import ArangoClient
from arango.database import StandardDatabase

# Utility Functions

def get_env_variable(var_name: str, default: str) -> str:
    value = os.getenv(var_name)
    if value is None:
        return default
    return value

def connect_to_arango(url: str, db_name: str, username: str, password: str) -> StandardDatabase:
    client = ArangoClient(hosts=url)
    return client.db(db_name, username=username, password=password)

def fetch_entities(db: StandardDatabase, entity_type: str) -> Dict[str, Any]:
    if entity_type == "analyzer":
        return {analyzer['name']: analyzer for analyzer in db.analyzers()}
    elif entity_type == "graph":
        return {graph['name']: graph for graph in db.graphs()}
    elif entity_type == "view":
        return {view['name']: view for view in db.views()}
    else:
        raise ValueError("Unknown entity type")

def normalize_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True)

def compare_entities_existence(db1: StandardDatabase, db2: StandardDatabase, entity_type: str) -> None:
    entities_db1 = fetch_entities(db1, entity_type)
    entities_db2 = fetch_entities(db2, entity_type)

    unique_to_db1 = set(entities_db1.keys()) - set(entities_db2.keys())
    unique_to_db2 = set(entities_db2.keys()) - set(entities_db1.keys())

    if unique_to_db1:
        print(f"{entity_type.capitalize()}s unique to db1:")
        for entity in unique_to_db1:
            print(f" - {entity}")
    else:
        print(f"No unique {entity_type}s in db1.")

    if unique_to_db2:
        print(f"{entity_type.capitalize()}s unique to db2:")
        for entity in unique_to_db2:
            print(f" - {entity}")
    else:
        print(f"No unique {entity_type}s in db2.")

def compare_entities_detail(db1: StandardDatabase, db2: StandardDatabase, entity_type: str, ignore_fields: Set[str]) -> None:
    entities_db1 = fetch_entities(db1, entity_type)
    entities_db2 = fetch_entities(db2, entity_type)

    for name, entity in entities_db1.items():
        normalized_db1 = normalize_json(entity)

        if name in entities_db2:
            normalized_db2 = normalize_json(entities_db2[name])

            diff = DeepDiff(json.loads(normalized_db1), json.loads(normalized_db2), ignore_order=True, exclude_paths=ignore_fields)
            if diff:
                print(f"Differences in {entity_type} '{name}':")
                print(diff)

def get_collection_names(db: StandardDatabase) -> Set[str]:
    return {collection['name'] for collection in db.collections()}

def compare_collections(db1_collections: Set[str], db2_collections: Set[str]) -> None:
    unique_to_db1 = db1_collections - db2_collections
    unique_to_db2 = db2_collections - db1_collections

    if unique_to_db1:
        print("Collections unique to db1:")
        for collection in unique_to_db1:
            print(f" - {collection}")
    else:
        print("No unique collections in db1.")

    if unique_to_db2:
        print("Collections unique to db2:")
        for collection in unique_to_db2:
            print(f" - {collection}")
    else:
        print("No unique collections in db2.")

# Core Functions

def connect_to_arango_databases() -> Tuple[StandardDatabase, StandardDatabase]:
    arango_url1 = get_env_variable("ARANGO_URL1", "http://localhost:8529")
    arango_db_name1 = get_env_variable("ARANGO_DB_NAME1", "test_db1")
    arango_username1 = get_env_variable("ARANGO_USERNAME1", "root")
    arango_password1 = get_env_variable("ARANGO_PASSWORD1", "passwd")

    arango_url2 = get_env_variable("ARANGO_URL2", "http://localhost:8529")
    arango_db_name2 = get_env_variable("ARANGO_DB_NAME2", "test_db2")
    arango_username2 = get_env_variable("ARANGO_USERNAME2", "root")
    arango_password2 = get_env_variable("ARANGO_PASSWORD2", "passwd")

    db1 = connect_to_arango(arango_url1, arango_db_name1, arango_username1, arango_password1)
    db2 = connect_to_arango(arango_url2, arango_db_name2, arango_username2, arango_password2)

    return db1, db2

# Main Function

def main() -> None:
    db1, db2 = connect_to_arango_databases()

    ignore_fields = {
        "root['id']",
        "root['_rev']",
        "root['_id']",
        "root['properties']['locale']",
        "root['revision']"
    }  # Fields to ignore

    # Compare collections
    db1_collections: Set[str] = get_collection_names(db1)
    db2_collections: Set[str] = get_collection_names(db2)
    compare_collections(db1_collections, db2_collections)

    # Compare existence of analyzers, graphs, and views
    compare_entities_existence(db1, db2, "analyzer")
    compare_entities_existence(db1, db2, "graph")
    compare_entities_existence(db1, db2, "view")

    # Compare detailed data for analyzers, graphs, and views
    compare_entities_detail(db1, db2, "analyzer", ignore_fields)
    compare_entities_detail(db1, db2, "graph", ignore_fields)
    compare_entities_detail(db1, db2, "view", ignore_fields)

# Execution Entry Point

if __name__ == "__main__":
    main()

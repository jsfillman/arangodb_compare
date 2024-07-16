import os
import json
import random
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

def fetch_entities(db: StandardDatabase, entity_type: str, collection_name: str = None) -> Dict[str, Any]:
    if entity_type == "analyzer":
        return {analyzer['name']: analyzer for analyzer in db.analyzers()}
    elif entity_type == "graph":
        return {graph['name']: graph for graph in db.graphs()}
    elif entity_type == "view":
        return {view['name']: view for view in db.views()}
    elif entity_type == "index":
        indexes = {}
        collections = [collection_name] if collection_name else [col['name'] for col in db.collections()]
        for collection in collections:
            for index in db.collection(collection).indexes():
                index_name = f"{collection}_{index['name']}"
                indexes[index_name] = index
        return indexes
    elif entity_type == "document":
        if collection_name:
            return {doc['_key']: doc for doc in db.collection(collection_name).all()}
        else:
            raise ValueError("Collection name must be provided for document entity type")
    elif entity_type == "edge":
        return {collection['name']: collection for collection in db.collections() if collection['type'] == 3}
    else:
        raise ValueError("Unknown or unsupported entity type")

def normalize_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True)

def compare_entities_existence(db1: StandardDatabase, db2: StandardDatabase, entity_type: str, collection_name: str = None) -> None:
    entities_db1 = fetch_entities(db1, entity_type, collection_name)
    entities_db2 = fetch_entities(db2, entity_type, collection_name)

    unique_to_db1 = set(entities_db1.keys()) - set(entities_db2.keys())
    unique_to_db2 = set(entities_db2.keys()) - set(entities_db1.keys())

    if unique_to_db1:
        print(f"{entity_type.capitalize()}s unique to db1 (collection: {collection_name}):")
        for entity in unique_to_db1:
            print(f" - {entity}")
    else:
        print(f"No unique {entity_type}s in db1 (collection: {collection_name}).")

    if unique_to_db2:
        print(f"{entity_type.capitalize()}s unique to db2 (collection: {collection_name}):")
        for entity in unique_to_db2:
            print(f" - {entity}")
    else:
        print(f"No unique {entity_type}s in db2 (collection: {collection_name}).")

def compare_entities_detail(db1: StandardDatabase, db2: StandardDatabase, entity_type: str, ignore_fields: Set[str], collection_name: str = None) -> None:
    entities_db1 = fetch_entities(db1, entity_type, collection_name)
    entities_db2 = fetch_entities(db2, entity_type, collection_name)

    for name, entity in entities_db1.items():
        normalized_db1 = normalize_json(entity)

        if name in entities_db2:
            normalized_db2 = normalize_json(entities_db2[name])

            diff = DeepDiff(json.loads(normalized_db1), json.loads(normalized_db2), ignore_order=True, exclude_paths=ignore_fields)
            if diff:
                print(f"Differences in {entity_type} '{name}' (collection: {collection_name}):")
                for key, value in diff.items():
                    for sub_key, sub_value in value.items():
                        if 'values_changed' in key or 'type_changes' in key:
                            print(f"  From db1 (old_value): {sub_value['old_value']}")
                            print(f"  From db2 (new_value): {sub_value['new_value']}")
                        else:
                            print(f"  {key}: {sub_value}")

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

# Collection Document Functions

def fetch_collection_documents(db: StandardDatabase, collection_name: str) -> Dict[str, Any]:
    return {doc['_key']: doc for doc in db.collection(collection_name).all()}

def compare_collection_documents(db1: StandardDatabase, db2: StandardDatabase, collection_name: str) -> None:
    docs_db1 = fetch_collection_documents(db1, collection_name)
    docs_db2 = fetch_collection_documents(db2, collection_name)

    unique_to_db1 = set(docs_db1.keys()) - set(docs_db2.keys())
    unique_to_db2 = set(docs_db2.keys()) - set(docs_db1.keys())

    if unique_to_db1:
        print(f"Documents unique to db1 (collection: {collection_name}):")
        for doc in unique_to_db1:
            print(f" - {doc}")
    else:
        print(f"No unique documents in db1 (collection: {collection_name}).")

    if unique_to_db2:
        print(f"Documents unique to db2 (collection: {collection_name}):")
        for doc in unique_to_db2:
            print(f" - {doc}")
    else:
        print(f"No unique documents in db2 (collection: {collection_name}).")

def compare_random_documents(db1: StandardDatabase, db2: StandardDatabase, collection_name: str, sample_size: int, ignore_fields: Set[str]) -> None:
    docs_db1 = fetch_collection_documents(db1, collection_name)
    docs_db2 = fetch_collection_documents(db2, collection_name)

    common_keys = set(docs_db1.keys()).intersection(docs_db2.keys())
    common_keys_list = list(common_keys)  # Convert to list to avoid deprecation warning

    if len(common_keys_list) > sample_size:
        sample_keys = random.sample(common_keys_list, sample_size)
    else:
        sample_keys = common_keys_list

    for key in sample_keys:
        doc_db1 = docs_db1[key]
        doc_db2 = docs_db2[key]

        normalized_db1 = normalize_json(doc_db1)
        normalized_db2 = normalize_json(doc_db2)

        diff = DeepDiff(json.loads(normalized_db1), json.loads(normalized_db2), ignore_order=True, exclude_paths=ignore_fields)
        if diff:
            print(f"Differences in document '{key}' (collection: {collection_name}):")
            for k, v in diff.items():
                for sk, sv in v.items():
                    if 'values_changed' in k or 'type_changes' in k:
                        print(f"  From db1 (old_value): {sv['old_value']}")
                        print(f"  From db2 (new_value): {sv['new_value']}")
                    else:
                        print(f"  {k}: {sv}")

# Collection Comparison Functions

def compare_collection_entities(db1: StandardDatabase, db2: StandardDatabase, collection_name: str, ignore_fields: Set[str]) -> None:
    print(f"\nComparing collection: {collection_name}")
    compare_collection_documents(db1, db2, collection_name)
    compare_entities_existence(db1, db2, "index", collection_name)
    compare_entities_detail(db1, db2, "index", ignore_fields, collection_name)

def compare_random_collection_documents(db1: StandardDatabase, db2: StandardDatabase, collection_name: str, sample_size: int, ignore_fields: Set[str]) -> None:
    print(f"\nComparing random documents in collection: {collection_name}")
    compare_random_documents(db1, db2, collection_name, sample_size, ignore_fields)

# Main Function

def main() -> None:
    (client1, db1), (client2, db2) = connect_to_arango_databases()

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

    # Compare existence of analyzers, graphs, views, indexes, and edges
    compare_entities_existence(db1, db2, "analyzer")
    compare_entities_existence(db1, db2, "graph")
    compare_entities_existence(db1, db2, "view")
    compare_entities_existence(db1, db2, "index")
    compare_entities_existence(db1, db2, "edge")

    # Compare detailed data for analyzers, graphs, views, indexes, and edges
    compare_entities_detail(db1, db2, "analyzer", ignore_fields)
    compare_entities_detail(db1, db2, "graph", ignore_fields)
    compare_entities_detail(db1, db2, "view", ignore_fields)
    compare_entities_detail(db1, db2, "index", ignore_fields)
    compare_entities_detail(db1, db2, "edge", ignore_fields)

    # Compare collection-specific entities
    for collection in db1_collections.intersection(db2_collections):
        compare_collection_entities(db1, db2, collection, ignore_fields)

    # Compare random documents in a collection
    sample_size = 5  # Number of documents to sample for detailed comparison
    for collection in db1_collections.intersection(db2_collections):
        compare_random_collection_documents(db1, db2, collection, sample_size, ignore_fields)

# Execution Entry Point

if __name__ == "__main__":
    main()

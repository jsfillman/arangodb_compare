import os
import json
import random
from datetime import datetime
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

def setup_logging_directory() -> Tuple[str, str]:
    base_log_dir = get_env_variable("LOGFILE_OUT", os.getcwd())
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    log_dir = os.path.join(base_log_dir, timestamp)
    os.makedirs(log_dir, exist_ok=True)
    return log_dir, timestamp

def write_log(log_dir: str, entity_type: str, content: str) -> None:
    log_file = os.path.join(log_dir, f"{entity_type}.md")
    with open(log_file, 'a') as file:
        file.write(content + "\n")

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

def compare_entities_existence(log_dir: str, db1: StandardDatabase, db2: StandardDatabase, entity_type: str, collection_name: str = None) -> None:
    entities_db1 = fetch_entities(db1, entity_type, collection_name)
    entities_db2 = fetch_entities(db2, entity_type, collection_name)

    unique_to_db1 = set(entities_db1.keys()) - set(entities_db2.keys())
    unique_to_db2 = set(entities_db2.keys()) - set(entities_db1.keys())

    log_content = f"# {entity_type.capitalize()} Existence Comparison\n\n"

    if unique_to_db1:
        log_content += f"## {entity_type.capitalize()}s unique to db1:\n"
        for entity in unique_to_db1:
            log_content += f"- {entity}\n"
    else:
        log_content += f"No unique {entity_type}s in db1.\n"

    if unique_to_db2:
        log_content += f"## {entity_type.capitalize()}s unique to db2:\n"
        for entity in unique_to_db2:
            log_content += f"- {entity}\n"
    else:
        log_content += f"No unique {entity_type}s in db2.\n"

    write_log(log_dir, entity_type, log_content)

def compare_entities_detail(log_dir: str, db1: StandardDatabase, db2: StandardDatabase, entity_type: str, ignore_fields: Set[str], collection_name: str = None) -> None:
    entities_db1 = fetch_entities(db1, entity_type, collection_name)
    entities_db2 = fetch_entities(db2, entity_type, collection_name)

    log_content = f"# {entity_type.capitalize()} Detailed Comparison\n\n"

    if not entities_db1 and not entities_db2:
        log_content += f"No {entity_type}s found in either database.\n"
    else:
        for name, entity in entities_db1.items():
            normalized_db1 = normalize_json(entity)

            if name in entities_db2:
                normalized_db2 = normalize_json(entities_db2[name])

                diff = DeepDiff(json.loads(normalized_db1), json.loads(normalized_db2), ignore_order=True, exclude_paths=ignore_fields)
                if diff:
                    log_content += f"### Differences in {entity_type} '{name}':\n"
                    for key, value in diff.items():
                        for sub_key, sub_value in value.items():
                            if 'values_changed' in key or 'type_changes' in key:
                                log_content += f"- From db1 (old_value): {sub_value['old_value']}\n"
                                log_content += f"- From db2 (new_value): {sub_value['new_value']}\n"
                            else:
                                log_content += f"- {key}: {sub_value}\n"
                else:
                    log_content += f"No differences in {entity_type} '{name}'.\n"
            else:
                log_content += f"{entity_type.capitalize()} '{name}' is unique to db1.\n"

        for name in entities_db2.keys():
            if name not in entities_db1:
                log_content += f"{entity_type.capitalize()} '{name}' is unique to db2.\n"

    write_log(log_dir, entity_type, log_content)

def get_collection_names(db: StandardDatabase) -> Set[str]:
    return {collection['name'] for collection in db.collections()}

def compare_collections(log_dir: str, db1_collections: Set[str], db2_collections: Set[str]) -> None:
    unique_to_db1 = db1_collections - db2_collections
    unique_to_db2 = db2_collections - db1_collections

    log_content = "# Collections Comparison\n\n"

    if unique_to_db1:
        log_content += "## Collections unique to db1:\n"
        for collection in unique_to_db1:
            log_content += f"- {collection}\n"
    else:
        log_content += "No unique collections in db1.\n"

    if unique_to_db2:
        log_content += "## Collections unique to db2:\n"
        for collection in unique_to_db2:
            log_content += f"- {collection}\n"
    else:
        log_content += "No unique collections in db2.\n"

    write_log(log_dir, "collections", log_content)

# Collection Document Functions

def fetch_collection_documents(db: StandardDatabase, collection_name: str, sample_size: int = 100) -> Dict[str, Any]:
    all_docs = list(db.collection(collection_name).all())
    if len(all_docs) > sample_size:
        return {doc['_key']: doc for doc in random.sample(all_docs, sample_size)}
    return {doc['_key']: doc for doc in all_docs}

def compare_collection_documents(log_dir: str, db1: StandardDatabase, db2: StandardDatabase, collection_name: str, sample_size: int = 100) -> None:
    docs_db1 = fetch_collection_documents(db1, collection_name, sample_size)
    docs_db2 = fetch_collection_documents(db2, collection_name, sample_size)

    unique_to_db1 = set(docs_db1.keys()) - set(docs_db2.keys())
    unique_to_db2 = set(docs_db2.keys()) - set(docs_db1.keys())

    log_content = f"# Document Comparison for Collection: {collection_name}\n\n"

    if unique_to_db1:
        log_content += f"## Documents unique to db1:\n"
        for doc in unique_to_db1:
            log_content += f"- {doc}\n"
    else:
        log_content += f"No unique documents in db1.\n"

    if unique_to_db2:
        log_content += f"## Documents unique to db2:\n"
        for doc in unique_to_db2:
            log_content += f"- {doc}\n"
    else:
        log_content += f"No unique documents in db2.\n"

    write_log(log_dir, "documents", log_content)

def compare_random_documents(log_dir: str, db1: StandardDatabase, db2: StandardDatabase, collection_name: str, sample_size: int, ignore_fields: Set[str]) -> None:
    docs_db1 = fetch_collection_documents(db1, collection_name, sample_size)
    docs_db2 = fetch_collection_documents(db2, collection_name, sample_size)

    common_keys = set(docs_db1.keys()).intersection(docs_db2.keys())
    common_keys_list = list(common_keys)  # Convert to list to avoid deprecation warning

    if len(common_keys_list) > sample_size:
        sample_keys = random.sample(common_keys_list, sample_size)
    else:
        sample_keys = common_keys_list

    log_content = f"# Random Document Comparison for Collection: {collection_name}\n\n"

    if not sample_keys:
        log_content += "No documents to compare or no differences found.\n"
    else:
        for key in sample_keys:
            doc_db1 = docs_db1[key]
            doc_db2 = docs_db2[key]

            normalized_db1 = normalize_json(doc_db1)
            normalized_db2 = normalize_json(doc_db2)

            diff = DeepDiff(json.loads(normalized_db1), json.loads(normalized_db2), ignore_order=True, exclude_paths=ignore_fields)
            if diff:
                log_content += f"### Differences in document '{key}' (collection: {collection_name}):\n"
                for k, v in diff.items():
                    for sk, sv in v.items():
                        if 'values_changed' in k or 'type_changes' in k:
                            log_content += f"- From db1 (old_value): {sv['old_value']}\n"
                            log_content += f"- From db2 (new_value): {sv['new_value']}\n"
                        else:
                            log_content += f"- {k}: {sv}\n"
            else:
                log_content += f"No differences in document '{key}'.\n"

    write_log(log_dir, "random_documents", log_content)

# Collection Comparison Functions

def compare_collection_entities(log_dir: str, db1: StandardDatabase, db2: StandardDatabase, collection_name: str, ignore_fields: Set[str], sample_size: int = 100) -> None:
    print(f"\nComparing collection: {collection_name}")
    compare_collection_documents(log_dir, db1, db2, collection_name, sample_size)
    compare_entities_existence(log_dir, db1, db2, "index", collection_name)
    compare_entities_detail(log_dir, db1, db2, "index", ignore_fields, collection_name)

def compare_random_collection_documents(log_dir: str, db1: StandardDatabase, db2: StandardDatabase, collection_name: str, sample_size: int, ignore_fields: Set[str]) -> None:
    print(f"\nComparing random documents in collection: {collection_name}")
    compare_random_documents(log_dir, db1, db2, collection_name, sample_size, ignore_fields)

# Main Function

def main() -> None:
    (client1, db1), (client2, db2) = connect_to_arango_databases()
    log_dir, timestamp = setup_logging_directory()

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
    compare_collections(log_dir, db1_collections, db2_collections)

    # Compare existence of analyzers, graphs, views, indexes, and edges
    compare_entities_existence(log_dir, db1, db2, "analyzer")
    compare_entities_existence(log_dir, db1, db2, "graph")
    compare_entities_existence(log_dir, db1, db2, "view")
    compare_entities_existence(log_dir, db1, db2, "index")
    compare_entities_existence(log_dir, db1, db2, "edge")

    # Compare detailed data for analyzers, graphs, views, indexes, and edges
    compare_entities_detail(log_dir, db1, db2, "analyzer", ignore_fields)
    compare_entities_detail(log_dir, db1, db2, "graph", ignore_fields)
    compare_entities_detail(log_dir, db1, db2, "view", ignore_fields)
    compare_entities_detail(log_dir, db1, db2, "index", ignore_fields)
    compare_entities_detail(log_dir, db1, db2, "edge", ignore_fields)

    # Compare collection-specific entities
    for collection in db1_collections.intersection(db2_collections):
        compare_collection_entities(log_dir, db1, db2, collection, ignore_fields)

    # Compare random documents in a collection
    sample_size = 5  # Number of documents to sample for detailed comparison
    for collection in db1_collections.intersection(db2_collections):
        compare_random_collection_documents(log_dir, db1, db2, collection, sample_size, ignore_fields)

    # Create README.md with summary
    readme_content = "# Comparison Summary\n\n"
    readme_content += f"## Run Details\n"
    readme_content += f"- Time of Run: {timestamp}\n"
    readme_content += f"- ARANGO_DB_NAME1: {os.getenv('ARANGO_DB_NAME1')}\n"
    readme_content += f"- ARANGO_DB_NAME2: {os.getenv('ARANGO_DB_NAME2')}\n"
    readme_content += f"- ARANGO_URL1: {os.getenv('ARANGO_URL1')}\n"
    readme_content += f"- ARANGO_URL2: {os.getenv('ARANGO_URL2')}\n"
    readme_content += f"- ARANGO_USERNAME1: {os.getenv('ARANGO_USERNAME1')}\n"
    readme_content += f"- ARANGO_USERNAME2: {os.getenv('ARANGO_USERNAME2')}\n"
    readme_content += "\n## Summary of Findings\n"
    readme_content += "- Collections: [collections.md](collections.md)\n"
    readme_content += "- Documents: [documents.md](documents.md)\n"
    readme_content += "- Random Documents: [random_documents.md](random_documents.md)\n"
    readme_content += "- Indexes: [index.md](index.md)\n"
    readme_content += "- Analyzers: [analyzer.md](analyzer.md)\n"
    readme_content += "- Graphs: [graph.md](graph.md)\n"
    readme_content += "- Views: [view.md](view.md)\n"
    readme_content += "- Edges: [edge.md](edge.md)\n"

    with open(os.path.join(log_dir, "README.md"), 'w') as readme_file:
        readme_file.write(readme_content)

    print(f"Comparison results logged to: {log_dir}")

if __name__ == "__main__":
    main()



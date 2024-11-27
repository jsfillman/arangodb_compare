import os
import time
from datetime import datetime
from deepdiff import DeepDiff
from typing import List, Tuple
from arango import ArangoClient
from arango.database import StandardDatabase
import argparse


# Utility Functions
def connect_to_arango(url: str, db_name: str, username: str, password: str) -> Tuple[ArangoClient, StandardDatabase]:
    client = ArangoClient(hosts=url)
    db = client.db(db_name, username=username, password=password)
    return client, db


def setup_logging_directory(db_name: str) -> Tuple[str, str]:
    base_log_dir = os.getenv("LOGFILE_OUT", os.getcwd())
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    log_dir = os.path.join(base_log_dir, f"{db_name}_{timestamp}")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir, timestamp


def write_log(log_dir: str, name: str, content: str, md_type: str = "bullet") -> None:
    log_file = os.path.join(log_dir, f"{name}.md")

    if md_type == "h1":
        output = f"\n# {content}\n"
    elif md_type == "h2":
        output = f"\n## {content}\n"
    elif md_type == "h3":
        output = f"\n### {content}\n"
    elif md_type == "h4":
        output = f"\n#### {content}\n"
    elif md_type == "h5":
        output = f"\n##### {content}\n"
    elif md_type == "bullet":
        output = f"  - {content}\n"
    elif md_type == "list":
        output = f"- {content}\n"
    else:
        output = f"{content}\n"

    with open(log_file, 'a') as file:
        file.write(output)


# Argument Parsing
def parse_arguments():
    parser = argparse.ArgumentParser(description="Compare two ArangoDB databases.")
    parser.add_argument("url1", help="URL of the first ArangoDB instance (e.g., http://localhost:8529).")
    parser.add_argument("url2", help="URL of the second ArangoDB instance (e.g., http://localhost:8530).")
    parser.add_argument("db_name", help="Name of the database to compare on both instances.")
    return parser.parse_args()


# DB-Wide Entity Checks
def get_entity_names(db: StandardDatabase, entity_type: str) -> List[str]:
    return [item['name'] for item in getattr(db, entity_type)()]


def get_db_entity_counts(db1: StandardDatabase, db2: StandardDatabase, log_dir: str) -> None:
    entity_types = ["analyzers", "graphs", "views", "collections"]

    for entity in entity_types:
        print(f"Checking {entity}... ")
        write_log(log_dir, entity, f"{entity.capitalize()}", "h1")
        names_db1 = get_entity_names(db1, entity)
        names_db2 = get_entity_names(db2, entity)
        count_db1 = len(names_db1)
        count_db2 = len(names_db2)
        print(f"DB1: {count_db1}, DB2: {count_db2}")
        write_log(log_dir, "README", f"{entity.capitalize()} Count", "h2")
        write_log(log_dir, "README", f"DB1: {count_db1}, DB2: {count_db2}", "bullet")
        write_log(log_dir, entity, f"{entity.capitalize()} Count", "h2")
        write_log(log_dir, entity, f"DB1: {count_db1}, DB2: {count_db2}", "bullet")

        if count_db1 != count_db2:
            mismatch_message = f"Mismatch found in {entity} counts."
            print(mismatch_message)
            write_log(log_dir, "README", mismatch_message, "list")
            write_log(log_dir, entity, mismatch_message, "list")

            unique_to_db1 = set(names_db1) - set(names_db2)
            unique_to_db2 = set(names_db2) - set(names_db1)

            if unique_to_db1:
                unique_message_db1 = f"Unique to DB1 {entity}:"
                write_log(log_dir, "README", unique_message_db1, "h3")
                write_log(log_dir, entity, unique_message_db1, "h3")
                for item in sorted(unique_to_db1):
                    write_log(log_dir, "README", item, "bullet")
                    write_log(log_dir, entity, item, "bullet")

            if unique_to_db2:
                unique_message_db2 = f"Unique to DB2 {entity}:"
                write_log(log_dir, "README", unique_message_db2, "h3")
                write_log(log_dir, entity, unique_message_db2, "h3")
                for item in sorted(unique_to_db2):
                    write_log(log_dir, "README", item, "bullet")
                    write_log(log_dir, entity, item, "bullet")


# Collection Checks
def get_collection_count(db: StandardDatabase, collection_name: str) -> int:
    query = f"RETURN LENGTH({collection_name})"
    result = db.aql.execute(query)
    return next(result)


def compare_collection_counts(db1: StandardDatabase, db2: StandardDatabase, log_dir: str) -> None:
    collection_names = get_entity_names(db1, 'collections')
    print(f"Checking collection document counts... ")
    write_log(log_dir, "collections", "Collection Document Counts", "h2")
    for collection_name in collection_names:
        try:
            count1 = get_collection_count(db1, collection_name)
            count2 = get_collection_count(db2, collection_name)
            if count1 != count2:
                mismatch_message = f"Mismatch in collection '{collection_name}': {count1} vs {count2}"
                print(mismatch_message)
                write_log(log_dir, "collections", mismatch_message, "h3")
        except Exception as e:
            print(f"Error getting count for {collection_name} in db1 or db2: {e}")


# Main function
def main() -> None:
    args = parse_arguments()

    arango_username = os.getenv("ARANGO_USERNAME", "root")
    arango_password = os.getenv("ARANGO_PASSWORD", "passwd")

    _, db1 = connect_to_arango(args.url1, args.db_name, arango_username, arango_password)
    _, db2 = connect_to_arango(args.url2, args.db_name, arango_username, arango_password)

    log_dir, timestamp = setup_logging_directory(args.db_name)

    # Compare entity counts
    get_db_entity_counts(db1, db2, log_dir)

    # Compare collection counts
    compare_collection_counts(db1, db2, log_dir)


if __name__ == "__main__":
    main()

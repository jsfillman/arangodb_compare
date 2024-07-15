import os
from typing import Optional, Set, Tuple
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
##

def get_env_variable(var_name: str, default: Optional[str] = None) -> str:
    value = os.getenv(var_name)
    if value is None:
        if default is not None:
            return default
        raise ValueError(f"Environment variable {var_name} is not set and no default value is provided.")
    return value

def connect_to_arango_databases() -> Tuple[StandardDatabase, StandardDatabase]:
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

    return db1, db2

def connect_to_arango(url: str, db_name: str, username: str, password: str) -> StandardDatabase:
    client = ArangoClient(hosts=url)
    return client.db(db_name, username=username, password=password)

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

def get_analyzer_names(db: StandardDatabase) -> Set[str]:
    return {analyzer['name'] for analyzer in db.analyzers()}

def compare_analyzers(db1_analyzers: Set[str], db2_analyzers: Set[str]) -> None:
    unique_to_db1 = db1_analyzers - db2_analyzers
    unique_to_db2 = db2_analyzers - db1_analyzers

    if unique_to_db1:
        print("Analyzers unique to db1:")
        for analyzer in unique_to_db1:
            print(f" - {analyzer}")
    else:
        print("No unique analyzers in db1.")

    if unique_to_db2:
        print("Analyzers unique to db2:")
        for analyzer in unique_to_db2:
            print(f" - {analyzer}")
    else:
        print("No unique analyzers in db2.")

def get_graph_names(db: StandardDatabase) -> Set[str]:
    return {graph['name'] for graph in db.graphs()}

def compare_graphs(db1_graphs: Set[str], db2_graphs: Set[str]) -> None:
    unique_to_db1 = db1_graphs - db2_graphs
    unique_to_db2 = db2_graphs - db1_graphs

    if unique_to_db1:
        print("Graphs unique to db1:")
        for graph in unique_to_db1:
            print(f" - {graph}")
    else:
        print("No unique graphs in db1.")

    if unique_to_db2:
        print("Graphs unique to db2:")
        for graph in unique_to_db2:
            print(f" - {graph}")
    else:
        print("No unique graphs in db2.")

def get_view_names(db: StandardDatabase) -> Set[str]:
    return {view['name'] for view in db.views()}

def compare_views(db1_views: Set[str], db2_views: Set[str]) -> None:
    unique_to_db1 = db1_views - db2_views
    unique_to_db2 = db2_views - db1_views

    if unique_to_db1:
        print("Views unique to db1:")
        for view in unique_to_db1:
            print(f" - {view}")
    else:
        print("No unique views in db1.")

    if unique_to_db2:
        print("Views unique to db2:")
        for view in unique_to_db2:
            print(f" - {view}")
    else:
        print("No unique views in db2.")

def main() -> None:
    db1, db2 = connect_to_arango_databases()

    # Compare collections
    db1_collections: Set[str] = get_collection_names(db1)
    db2_collections: Set[str] = get_collection_names(db2)
    compare_collections(db1_collections, db2_collections)

    # Compare analyzers
    db1_analyzers: Set[str] = get_analyzer_names(db1)
    db2_analyzers: Set[str] = get_analyzer_names(db2)
    compare_analyzers(db1_analyzers, db2_analyzers)

    # Compare graphs
    db1_graphs: Set[str] = get_graph_names(db1)
    db2_graphs: Set[str] = get_graph_names(db2)
    compare_graphs(db1_graphs, db2_graphs)

    # Compare views
    db1_views: Set[str] = get_view_names(db1)
    db2_views: Set[str] = get_view_names(db2)
    compare_views(db1_views, db2_views)

if __name__ == "__main__":
    main()

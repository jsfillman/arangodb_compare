import argparse
import os
from arango import ArangoClient
from rich.console import Console
from rich.table import Table
from rich import print as rprint

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Compare two ArangoDB databases.")
    parser.add_argument("-url1", default=os.getenv("ARANGO_URL1"), help="URL of the first ArangoDB instance (e.g., http://localhost:8529).")
    parser.add_argument("-url3", default=os.getenv("ARANGO_URL3"), help="URL of the second ArangoDB instance (e.g., http://localhost:8530).")
    parser.add_argument("-user1", default=os.getenv("ARANGO_USERNAME1", "root"), help="Username for the first ArangoDB instance (default: root).")
    parser.add_argument("-user3", default=os.getenv("ARANGO_USERNAME3", "root"), help="Username for the second ArangoDB instance (default: root).")
    parser.add_argument("-pass1", default=os.getenv("ARANGO_PASSWORD1"), help="Password for the first ArangoDB instance.")
    parser.add_argument("-pass3", default=os.getenv("ARANGO_PASSWORD3"), help="Password for the second ArangoDB instance.")
    parser.add_argument("-db", default=os.getenv("ARANGO_DB_NAME1"), help="Name of the database to compare on both ArangoDB instances.")
    parser.add_argument("-input", required=True, help="Path to a file containing collection/ID combos to process.")
    return parser.parse_args()

def connect_to_arangodb(url, username, password, db_name):
    """Connect to the ArangoDB database."""
    client = ArangoClient(hosts=url)
    db = client.db(
        db_name,  # Use the provided database name
        username=username,
        password=password
    )
    return db

def run_aql_query(db, workspace, workspace_id, graph):
    """Run the specified AQL query on the database."""
    query = f"""
    FOR v, e, p IN 1..1 ANY \"{workspace}/{workspace_id}\"
      GRAPH \"{graph}\"
      RETURN {{
        vertex: v,
        edge: e,
        path: p
      }}
    """
    cursor = db.aql.execute(query, batch_size=1000)
    results = [doc for doc in cursor]
    return results

def sort_results(results):
    """Sort the query results consistently."""
    return sorted(results, key=lambda x: x['vertex'].get('_key', ''))

def compare_and_display_results(results_db1, results_db2, workspace, workspace_id, summary):
    """Compare results from two databases and display differences or a summary in YAML-like format."""
    console = Console()

    # Extract keys from both result sets
    keys_db1 = {r['vertex'].get('_key', '') for r in results_db1}
    keys_db2 = {r['vertex'].get('_key', '') for r in results_db2}

    # Find differences
    only_in_db1 = keys_db1 - keys_db2
    only_in_db2 = keys_db2 - keys_db1

    console.print(f"- Checking workspace/id: {workspace}/{workspace_id}")
    console.print(f"  - {len(keys_db1)} connections found in DB1")
    console.print(f"  - {len(keys_db2)} connections found in DB2")

    if only_in_db1 or only_in_db2:
        console.print(f"  - Differences found:")
        for key in sorted(only_in_db1):
            console.print(f"    - [cyan]Key '{key}'[/cyan] only in DB1")
        for key in sorted(only_in_db2):
            console.print(f"    - [green]Key '{key}'[/green] only in DB2")
        summary.append([f"{workspace}/{workspace_id}", len(keys_db1), len(keys_db2), len(only_in_db1) + len(only_in_db2)])
    else:
        console.print("  - [bold green]No differences found![/bold green]")
        summary.append([f"{workspace}/{workspace_id}", len(keys_db1), len(keys_db2), 0])

def process_input_file(input_file, db1, db2):
    """Process the input file containing graph/collection/ID combos."""
    """Process the input file containing collection/ID combos."""
    summary = []
 with open(input_file, 'r') as file:
        lines = file.readlines()

    for line in lines:
        graph, workspace_and_id = line.strip().split(',')
        workspace, workspace_id = workspace_and_id.split('/')
        results_db1 = sort_results(run_aql_query(db1, workspace, workspace_id, graph))
        results_db2 = sort_results(run_aql_query(db2, workspace, workspace_id, graph))
        compare_and_display_results(results_db1, results_db2, workspace, workspace_id, summary)

    display_summary(summary)

def display_summary(summary):
    """Display a summary table of all comparisons."""
    console = Console()
    table = Table(title="Comparison Summary")

    table.add_column("Workspace/ID", justify="left", style="cyan", no_wrap=True)
    table.add_column("Connections in DB1", justify="center", style="magenta")
    table.add_column("Connections in DB2", justify="center", style="green")
    table.add_column("Differences Found", justify="center", style="red")

    for row in summary:
        table.add_row(*map(str, row))

    console.print(table)

def main():
    # Parse arguments
    args = parse_arguments()

    # Validate required arguments
    if not args.db:
        raise ValueError("Database name is required. Please set ARANGO_DB_NAME1 or use the -db flag.")

    # Step 1: Connect to the first database
    db1 = connect_to_arangodb(args.url1, args.user1, args.pass1, args.db)

    # Step 2: Connect to the second database using ARANGO_URL3, ARANGO_USERNAME3, and ARANGO_DB_NAME1
    db2 = connect_to_arangodb(
        args.url3,
        args.user3,
        args.pass3,
        args.db
    )

    # Step 3: Process the input file
    process_input_file(args.input, db1, db2)

if __name__ == "__main__":
    main()

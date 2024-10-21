import os
import time
import requests
import csv
from rich.console import Console
from rich.table import Table
from rich.progress import BarColumn, Progress
from datetime import datetime

# Initialize rich console for ASCII charts
console = Console()

# Ensure environment variables are set
arango_username = os.getenv("ARANGO_USERNAME1")
arango_password1 = os.getenv("ARANGO_PASSWORD1")
arango_password2 = os.getenv("ARANGO_PASSWORD2")
arango_password3 = os.getenv("ARANGO_PASSWORD3")

if not arango_username or not arango_password1 or not arango_password2 or not arango_password3:
    console.print("[red]Please set ARANGO_USERNAME1, ARANGO_PASSWORD1, ARANGO_PASSWORD2, and ARANGO_PASSWORD3.[/red]")
    exit(1)

# ArangoDB endpoints
endpoints = {
    "3.5 Cluster": "http://140.221.43.238:8531",
    "3.11 Single": "http://140.221.43.238:8530",
    "3.11 Cluster": "http://140.221.43.238:8529"
}

# CSV Log file location
# csv_log_file = "/work/ArangoDB/dump/arango3-rancher1/output/arangodb_connection_log.csv"

log_directory = os.getenv('LOG_DIRECTORY', '/work/ArangoDB/dump/arango3-rancher1/output/')
timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")  # Format: 2024-07-25_032944
filename = f"arangodb_connection_log_{timestamp}.csv"

# Full path to the CSV log file
csv_log_file = os.path.join(log_directory, filename)

# Create CSV log if it doesn't exist
if not os.path.exists(csv_log_file):
    with open(csv_log_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "3.5 Cluster", "3.11 Single", "3.11 Cluster"])

# Function to get connection count from ArangoDB server
def get_connection_count(endpoint, username, password):
    try:
        response = requests.get(f"{endpoint}/_admin/statistics", auth=(username, password))
        data = response.json()
        return data['clientUser']['httpConnections']
    except Exception as e:
        console.print(f"[red]Error fetching data from {endpoint}: {e}[/red]")
        return 0

# Function to log data to CSV
def log_data_csv(timestamp, connections):
    with open(csv_log_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, connections["3.5 Cluster"], connections["3.11 Single"], connections["3.11 Cluster"]])

# Function to display connection counts as an ASCII chart
def display_ascii_chart(connections):
    console.clear()
    table = Table(title="ArangoDB Active Connections")
    table.add_column("Server", justify="center", style="cyan", no_wrap=True)
    table.add_column("Connections", justify="center", style="magenta")

    progress = Progress(BarColumn(bar_width=None), console=console)

    for server, count in connections.items():
        table.add_row(server, str(count))

    console.print(table)
    console.print()

    # Display connection counts as a progress bar
    with progress:
        for server, count in connections.items():
            progress.add_task(f"Active Connections: {server}", total=100, completed=int(count))

# Infinite loop to check connection counts every 30 seconds
while True:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get connection counts for all servers
    connections = {
        "3.5 Cluster": get_connection_count(endpoints["3.5 Cluster"], arango_username, arango_password1),
        "3.11 Single": get_connection_count(endpoints["3.11 Single"], arango_username, arango_password2),
        "3.11 Cluster": get_connection_count(endpoints["3.11 Cluster"], arango_username, arango_password3)
    }

    # Log connection data to CSV
    log_data_csv(timestamp, connections)

    # Display an ASCII chart in the terminal
    display_ascii_chart(connections)

    console.print(f"[bold]Last updated:[/bold] {timestamp}")
    console.print("--------------------------------------------")

    # Wait for 30 seconds before checking again
    time.sleep(30)

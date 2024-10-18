# arangodb_compare


## Overview

`arangodb_compare` is a tool that compares collections, documents, and indexes between two ArangoDB instances. It helps in identifying differences between two ArangoDB databases, including collection details and additional database features like graphs, views, and analyzers.

## Environment Variables

When deploying `arangodb_compare`, the following environment variables can be defined:

### ArangoDB Instance 1

- `ARANGO_URL1`: The URL of the first ArangoDB instance. Default is `http://localhost:8529`.
- `ARANGO_USERNAME1`: The username for the first ArangoDB instance. Default is `root`.
- `ARANGO_PASSWORD1`: The password for the first ArangoDB instance. Default is `password`.
- `ARANGO_DB_NAME1`: The database name for the first ArangoDB instance. Default is `_system`.

### ArangoDB Instance 2

- `ARANGO_URL2`: The URL of the second ArangoDB instance. Default is `http://localhost:8530`.
- `ARANGO_USERNAME2`: The username for the second ArangoDB instance. Default is `root`.
- `ARANGO_PASSWORD2`: The password for the second ArangoDB instance. Default is `password`.
- `ARANGO_DB_NAME2`: The database name for the second ArangoDB instance. Default is `_system`.

### Additional Configuration

- `ENV`: The environment in which the tool is running (e.g., `production`, `development`). Default is `development`.

## Deploying

The simplest method of deploying `arangodb_compare` is to use the provided Docker image.
The above variables can be used to configure the tool when deploying it as a container.


#!/bin/sh

if [ "$RUNTIME" = "production" ]; then
    python -m arangodb_compare.main
else
    echo "Development mode: Build successful"
fi

#!/bin/sh

if [ "$RUNTIME" = "production" ]; then
    python -m arango_compare.main
else
    echo "Development mode: Build successful"
fi

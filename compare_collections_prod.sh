#!/bin/sh

# Compare databases for the 'collections_prod' environment
python -m arangodb_compare.main \
  -url1 "$ARANGO_URL2" \
  -url2 "$ARANGO_URL3" \
  -pass1 "$ARANGO_PASSWORD2" \
  -pass2 "$ARANGO_PASSWORD3" \
  -db "collections_prod"

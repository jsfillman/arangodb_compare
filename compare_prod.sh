#!/bin/sh

# Compare databases for the 'prod' environment
python -m arangodb_compare.main \
  -url1 "$ARANGO_URL1" \
  -url2 "$ARANGO_URL3" \
  -pass1 "$ARANGO_PASSWORD1" \
  -pass2 "$ARANGO_PASSWORD3" \
  -db "prod"

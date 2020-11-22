#!/bin/bash

AUTH="Authorization: Bearer $1"
TABLE_SCHEMA='{ "column_types": ["STRING", "i64", "f64" ] }'

# Test Token
http :6969/auth "$AUTH"

# Create a Table Schema
echo $TABLE_SCHEMA | http post :6969/table_schemas "$AUTH" | tee target/table_schema.txt | jq '.id' > target/table_schema_id.txt

# Create a Table
echo '{ "table_schema_id": '$(cat target/table_schema_id.txt)', "name": "hndefault" }' | http post :6969/tables "$AUTH" | tee target/table.txt | jq '.id' > target/table_id.txt

cat target/table.txt

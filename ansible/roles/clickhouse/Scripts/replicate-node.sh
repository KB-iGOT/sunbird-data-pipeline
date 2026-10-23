#!/usr/bin/env bash
set -euo pipefail
SRC="clickhouse-client --secure --host <ch1-ip> --user <admin> --password <pw>"
DST="clickhouse-client --secure --host <ch4-ip> --user <admin> --password <pw>"
DB=warehouse
OUT=schema.sql

# Database definition
$SRC --format TSVRaw -q "SHOW CREATE DATABASE $DB" > $OUT; echo ";" >> $OUT

# Tables first, views/MVs last; skip MV inner tables (their MV recreates them)
$SRC --format TSVRaw -q "
  SELECT name FROM system.tables
  WHERE database = '$DB' AND NOT startsWith(name, '.inner') AND engine != 'Dictionary'
  ORDER BY engine IN ('View', 'MaterializedView'), name" |
while read -r t; do
  # Include the table's UUID so ch4 joins the SAME Keeper path as ch1/ch2
  $SRC --show_table_uuid_in_table_create_query_if_not_nil=1 --format TSVRaw \
       -q "SHOW CREATE TABLE $DB.\`$t\`"
  echo ";"
done >> $OUT

# Make it safe to re-run
sed -i -E 's/^CREATE (DATABASE|TABLE|VIEW|MATERIALIZED VIEW) /CREATE \1 IF NOT EXISTS /' $OUT

grep -c '^CREATE' $OUT   # should match the table count on ch1
$DST --queries-file $OUT
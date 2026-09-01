#!/usr/bin/env bash
# Stand up the three on-prem source databases and load them.
set -euo pipefail

if [ ! -f .env ]; then
  echo "No .env found. Run: cp .env.example .env   (then edit the password)"
  exit 1
fi
set -a; source .env; set +a

SQLCMD="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${MSSQL_SA_PASSWORD} -C"

echo "Starting SQL Server..."
docker compose up -d

echo "Waiting for the instance to accept connections..."
for i in $(seq 1 40); do
  if docker exec ohn-sqlserver $SQLCMD -Q "SELECT 1" >/dev/null 2>&1; then
    echo "  ready"; break
  fi
  sleep 10
  if [ "$i" -eq 40 ]; then
    echo "Timed out. Check: docker logs ohn-sqlserver"; exit 1
  fi
done

for script in 01_create_databases 02_create_tables 03_load_data 04_create_reader_login; do
  echo "Running ${script}.sql ..."
  docker exec ohn-sqlserver $SQLCMD -i "/scripts/${script}.sql"
done

echo
echo "Row counts:"
for db in OHN_EHR OHN_SCHED OHN_FIN; do
  docker exec ohn-sqlserver $SQLCMD -d "$db" -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT '  $db.' + t.name + ' = ' + CAST(SUM(p.rows) AS VARCHAR(20))
     FROM sys.tables t
     JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
     GROUP BY t.name ORDER BY t.name;"
done

echo
echo "Done. Point the gateway at localhost,1433 using the fabric_reader login."

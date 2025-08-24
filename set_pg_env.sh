#!/bin/bash

export PGHOST="instance-bb41d15b-8823-422b-bae0-bd01fba56fed.database.azuredatabricks.net"
export PGUSER="vik.malhotra@databricks.com"
export PGDATABASE="databricks_postgres"
export PGPORT="5432"
export PGSSLMODE="require"
export PGPASSWORD=$(databricks database generate-database-credential --request-id $(uuidgen) --json '{"instance_names": ["myapp"]}' | jq -r '.token')

echo "PGHOST=$PGHOST"
echo "PGUSER=$PGUSER"
echo "PGDATABASE=$PGDATABASE"
echo "PGPORT=$PGPORT"
echo "PGSSLMODE=$PGSSLMODE"
echo "PGPASSWORD=[hidden - length ${#PGPASSWORD}]"




#chmod +x set_pg_env.sh
#source set_pg_env.sh
# env | grep ^PG
# printenv | grep ^PG

# echo $PGHOST
# echo $PGUSER
# echo $PGDATABASE
# echo $PGPORT
# echo $PGSSLMODE
# echo $PGPASSWORD


# printenv PGHOST
# printenv PGUSER
# printenv PGDATABASE
# printenv PGPORT
# printenv PGSSLMODE
# printenv PGPASSWORD


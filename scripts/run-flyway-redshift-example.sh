#!/bin/bash
# http://blog.kablamo.org/2015/11/08/bash-tricks-eux/
set -euxo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC2016

#psql "host=${redshift_endpoint_noport} user=awssysdba password=${CFN_REDSHIFT_MASTER_USER_PASSWORD} dbname=${dbname} port=5439" -f ./dbscripts/1__createkdwschema.sql

cat > flyway.conf << EOF
flyway.url=jdbc:postgresql://db:5432/postgres
flyway.user=postgres
flyway.password=P@ssw0rd
flyway.locations=filesystem:${PWD}/dbscripts
flyway.schemas=r2
flyway.placeholders.db=postgres
flyway.placeholders.env=${DB_ENVIRONMENT}
EOF

#cat flyway.conf
flyway -configFiles=flyway.conf migrate

#!/bin/bash
set -e
set -u

# TODO: check license : https://datahub.io/core/language-codes
rm -f language-codes-3b2_csv.csv
wget http://pkgstore.datahub.io/core/language-codes:language-codes-3b2_csv/data/language-codes-3b2_csv.csv


echo """
    CREATE SCHEMA IF NOT EXISTS  codes;
    DROP TABLE IF EXISTS codes.iso_language_codes;
""" | psql


pgfutter --schema codes \
         --table iso_language_codes \
         csv \
         language-codes-3b2_csv.csv 


echo """
    -- analyze --
    ANALYZE codes.iso_language_codes;

    -- test --
    SELECT * 
    FROM codes.iso_language_codes 
    LIMIT 12;

    \d+ codes.iso_language_codes;

""" | psql

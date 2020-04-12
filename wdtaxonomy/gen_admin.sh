#!/bin/bash
set -o errexit

wdtaxonomy Q55977691 -i -f csv | cut -d, -f2,3 | grep "^P[0-9].*"  > wikidata_admincode.csv
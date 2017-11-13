
\cd :reportdir
\copy (select * from wikidata.wd_names_preferred) TO 'wikidata_wd_names_preferred.csv' CSV;
\copy (select * from wikidata.wd_sitelinks)       TO 'wikidata_wd_sitelinks.csv' CSV;
\copy (select * from wikidata.wd_descriptions)    TO 'wikidata_wd_descriptions.csv' CSV;
\copy (select * from wikidata.wd_aliases) TO 'wikidata_wd_aliases' CSV;
\copy (select * from wikidata.wd_labels) TO 'wikidata_wd_labels.csv' CSV;
\copy (select * from wikidata.wd_P227_gnd_id) TO 'wikidata_wd_P227_gnd_id.csv' CSV;
\copy (select * from wikidata.wd_P300_iso3166_2_code) TO 'wikidata_wd_P300_iso3166_2_code.csv' CSV;



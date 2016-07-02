lingual catalog --schema LAST_FM --table listen_events --remove
lingual catalog --schema LAST_FM --stereotype listen_evts_s --remove
lingual catalog --schema LAST_FM --remove

lingual catalog --schema LAST_FM --add

lingual catalog --schema LAST_FM --stereotype listen_evts_s --add \
  --columns UID,DATETIME,ARTIST_MBID,ARTIST_NAME,TRACK_MBID,TRACK_NAME \
  --types string,string,string,string,string,string


lingual catalog --schema LAST_FM --table LISTEN_EVENTS --stereotype listen_evts_s \
  --add last.fm/clean.tsv


### Don't use anymore ->


lingual catalog --schema LAST_FM --table LISTEN_EVENTS --stereotype listen_evts_s \
  --add last.fm/userid-timestamp-artid-artname-traid-traname.psv


lingual catalog --schema LAST_FM --table LISTEN_EVENTS --update last.fm/userid-timestamp-artid-artname-traid-traname.psv --format psv

lingual catalog --schema LAST_FM --format psv --add --provider text --extensions '.psv' --properties "delimiter=|"

lingual catalog --schema LAST_FM --format tsv --add --provider text --extensions '.tsv' --properties "delimiter=\t"
  
  
psql postgres livlab;
create table events (
userid varchar(100),
timestamp varchar(100),
artid varchar(100),
artname varchar(100),
traid varchar(100),
traname varchar (100)
);
postgres=> \copy events from '/home/livlab/data/lastfm-dataset-1K/sorted_lastfm.tsv' with delimiter E'\t';

postgres=> alter table events alter column artname type varchar(300);
postgres=> alter table events alter column traname type varchar(200);

cat userid-timestamp-artid-artname-traid-traname.tsv | tr -d '\\' > cleaned1.tsv
 sort -k 2 cleaned1.tsv > sorted_cleaned.tsv
livlab@livlab-VirtualBox:~/data/lastfm-dataset-1K$ mv sorted_cleaned.tsv cleaned1.tsv 

\copy events from '/home/livlab/data/lastfm-dataset-1K/cleaned1.tsv' with delimiter E'\t';

postgres=> select count(*) from events;
  count   
----------
 19150868
(1 row)


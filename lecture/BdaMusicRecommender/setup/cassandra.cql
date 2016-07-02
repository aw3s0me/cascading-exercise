CREATE KEYSPACE recommendations WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};
use recommendations;
CREATE TABLE streamtable (
  artname text,
  clusterid text,
  PRIMARY KEY (artname)
);
CREATE TABLE batchtable (
  artname text,
  clusterid text,
  PRIMARY KEY (artname)
);
CREATE TABLE clustertable (
  clusterid text,
  artnames text,
  PRIMARY KEY (clusterid)
);

insert into streamtable(artname,clusterid) values ('abba','cluster_0');
insert into batchtable(artname,clusterid) values ('queen','cluster_0');
insert into clustertable(clusterid,artnames) values ('cluster_0','abba,queen,bay city rollers');


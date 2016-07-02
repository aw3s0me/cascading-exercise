# uncomment next line the first time
#install.packages("ggplot2")
library(ggplot2)

library(RJDBC)
cp <- c(
  "/home/livlab/.lingual-client/platform/hadoop/lingual-hadoop-1.1.0-jdbc.jar",
  "/etc/hadoop",
  #   "/home/livlab/.lingual-client/platform/local/lingual-local-1.0.3-wip-318-jdbc.jar"
  "/usr/share/hadoop/hadoop-core-1.2.1.jar",
  "/usr/share/hadoop/lib/asm-3.2.jar",
  "/usr/share/hadoop/lib/aspectjrt-1.6.11.jar",
  "/usr/share/hadoop/lib/aspectjtools-1.6.11.jar",
  "/usr/share/hadoop/lib/commons-beanutils-1.7.0.jar",
  "/usr/share/hadoop/lib/commons-beanutils-core-1.8.0.jar",
  "/usr/share/hadoop/lib/commons-cli-1.2.jar",
  "/usr/share/hadoop/lib/commons-codec-1.4.jar",
  "/usr/share/hadoop/lib/commons-collections-3.2.1.jar",
  "/usr/share/hadoop/lib/commons-configuration-1.6.jar",
  "/usr/share/hadoop/lib/commons-daemon-1.0.1.jar",
  "/usr/share/hadoop/lib/commons-digester-1.8.jar",
  "/usr/share/hadoop/lib/commons-el-1.0.jar",
  "/usr/share/hadoop/lib/commons-httpclient-3.0.1.jar",
  "/usr/share/hadoop/lib/commons-io-2.1.jar",
  "/usr/share/hadoop/lib/commons-lang-2.4.jar",
  "/usr/share/hadoop/lib/commons-logging-1.1.1.jar",
  "/usr/share/hadoop/lib/commons-logging-api-1.0.4.jar",
  "/usr/share/hadoop/lib/commons-math-2.1.jar",
  "/usr/share/hadoop/lib/commons-net-3.1.jar",
  "/usr/share/hadoop/lib/core-3.1.1.jar",
  "/usr/share/hadoop/lib/hadoop-capacity-scheduler-1.2.1.jar",
  "/usr/share/hadoop/lib/hadoop-fairscheduler-1.2.1.jar",
  "/usr/share/hadoop/lib/hadoop-thriftfs-1.2.1.jar",
  "/usr/share/hadoop/lib/hsqldb-1.8.0.10.jar",
  "/usr/share/hadoop/lib/jackson-core-asl-1.8.8.jar",
  "/usr/share/hadoop/lib/jackson-mapper-asl-1.8.8.jar",
  "/usr/share/hadoop/lib/jasper-compiler-5.5.12.jar",
  "/usr/share/hadoop/lib/jasper-runtime-5.5.12.jar",
  "/usr/share/hadoop/lib/jdeb-0.8.jar",
  "/usr/share/hadoop/lib/jersey-core-1.8.jar",
  "/usr/share/hadoop/lib/jersey-json-1.8.jar",
  "/usr/share/hadoop/lib/jersey-server-1.8.jar",
  "/usr/share/hadoop/lib/jets3t-0.6.1.jar",
  "/usr/share/hadoop/lib/jetty-6.1.26.jar",
  "/usr/share/hadoop/lib/jetty-util-6.1.26.jar",
  "/usr/share/hadoop/lib/jsch-0.1.42.jar",
  "/usr/share/hadoop/lib/junit-4.5.jar",
  "/usr/share/hadoop/lib/kfs-0.2.2.jar",
  "/usr/share/hadoop/lib/log4j-1.2.15.jar",
  "/usr/share/hadoop/lib/mockito-all-1.8.5.jar",
  "/usr/share/hadoop/lib/oro-2.0.8.jar",
  "/usr/share/hadoop/lib/servlet-api-2.5-20081211.jar",
  "/usr/share/hadoop/lib/slf4j-api-1.4.3.jar",
  "/usr/share/hadoop/lib/slf4j-log4j12-1.4.3.jar",
  "/usr/share/hadoop/lib/xmlenc-0.52.jar"
) 

drv <- JDBC("cascading.lingual.jdbc.Driver", cp)
connection <- dbConnect(drv, "jdbc:lingual:hadoop;")

# query the repository

df <- dbGetQuery(connection, "SELECT artist_mbid, count(DISTINCT uid) count_dist FROM last_fm.listen_events GROUP BY artist_mbid ORDER BY count_dist")
head(df)

summary(df)

dfsample <- df[sample(nrow(df), 100),];
dfsample
m <- ggplot(df, aes(x=COUNT_DIST))
m <- m + ggtitle("Distribution of distinct listeners on artists")
m + geom_histogram(binwidth=1, aes(y=..density.., fill=..count..)) + geom_density()

m + geom_histogram(binwidth=1, aes(y=..count..)) 
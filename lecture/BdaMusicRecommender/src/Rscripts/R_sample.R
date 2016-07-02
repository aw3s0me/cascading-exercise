Sys.setenv("HADOOP_CMD"="/usr/bin/hadoop")
library(rmr2)

map <- function(k,lines) {
  words.list <- strsplit(lines, '\\s')
  words <- unlist(words.list)
  return( keyval(words, 1) )
}

reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text", map=map, reduce=reduce)
}

## read text files from folder wordcount/data
## save result in folder wordcount/out
## Submit job

hdfs.root <- 'wordcount'
hdfs.data <- file.path(hdfs.root, 'data')
hdfs.out <- file.path(hdfs.root, 'Rout')
out <- wordcount(hdfs.data, hdfs.out)


## Fetch results from HDFS
results <- from.dfs(out)
results.df <- as.data.frame(results, stringsAsFactors=F)
colnames(results.df) <- c('word', 'count')

head(results.df, n=15)

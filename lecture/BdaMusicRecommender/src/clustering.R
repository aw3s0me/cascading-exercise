mydata = read.csv("/home/livlab/recommender_full/matrix/part-00000", header = FALSE, sep = "|")
str(mydata)
mydata[c(1:110000),]

require(cluster)
clusters <- pam(mydata, 75, diss = FALSE, metric = "euclidean", medoids = NULL, stand = TRUE, cluster.only = TRUE, do.swap = FALSE) 


sd <- mydata[sample(nrow(mydata), size = 10000),]
write.csv(sd, "/home/livlab/recommender_full/matrix/part-00000-sample")


clusters <- pam(sdc, 75, diss = FALSE, metric = "euclidean", medoids = NULL, stand = TRUE, cluster.only = FALSE, do.swap = FALSE)

str(clusters)

plot(silhouette(clusters))

install.packages('vegan')
require(vegan)
d <- vegdist(sdc], method = "jaccard")
?vegdist

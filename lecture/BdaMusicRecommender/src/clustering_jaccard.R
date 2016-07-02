#install.packages('flexclust')
require('flexclust')


# read the interaction matrix from csv
mydata = read.csv("/home/livlab/recommender_full/matrix/part-00000", header = FALSE, sep = "|")
str(mydata)

# sample 1000
sd <- mydata[sample(nrow(mydata), size = 5000),]
# optionally save the sample
#write.csv(sd, "/home/livlab/recommender_full/matrix/part-00000-sample")

data <-data.matrix( sd[,2:1002], rownames.force = NA);

## setup options
mycont <- list(iter=2, tol=0.001, verbose=1)
opts <-as(mycont, "flexclustControl")

# time clustering and do it :-)
ptm <- proc.time()
cl1 = kcca(data, k=50, family=kccaFamily("jaccard"), control=opts)
proc.time() - ptm
# optionally save
#save(cl1, file="~/clustering_u1000_iter2_k50.rdata")
# do pca
pca <- prcomp(data);

plot(cl1, data =data, project = pca)

cl1
str(cl1)
centers <- cl1@centers
write.csv(centers, "/home/livlab/centers_1000_iter_2_50.csv")

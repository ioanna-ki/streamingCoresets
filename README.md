# streamingCoresets

streaming coreset extraction for mllib's StreamingKMeans model

### How to run:

sbt package

./bin/spark-submit --class ClusteringTrainEvaluate --master yarn-client /home/hduser/streamingcoresets_2.11-0.1.jar 10000 50 2 hdfs://host:port/user/hduser/train/ hdfs://host:port/user/hduser/test

args(0) = the size of the extracted coreset (default: 10000)

args(1) = number of k centers for the kmeans model (default: 50)

args(2) = data dimensionality (default: 2)

args(3) = directory for training data

args(4) = directory of test data

#### training and test data (2-dimensions) example:
0.4,2.1

1.6,15.9

4.2,1.4
....

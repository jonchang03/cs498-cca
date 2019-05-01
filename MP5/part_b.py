from pyspark import SparkContext
from numpy import array
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import sql
import numpy as np
from collections import defaultdict

############################################
#### PLEASE USE THE GIVEN PARAMETERS     ###
#### FOR TRAINING YOUR KMEANS CLUSTERING ###
#### MODEL                               ###
############################################

NUM_CLUSTERS = 4
SEED = 0
MAX_ITERATIONS = 100
INITIALIZATION_MODE = "random"

sc = SparkContext()
sqlContext = sql.SQLContext(sc)


def get_clusters(data_rdd, labels, num_clusters=NUM_CLUSTERS, max_iterations=MAX_ITERATIONS,
                 initialization_mode=INITIALIZATION_MODE, seed=SEED):
    kmeans = KMeans.train(data_rdd, k=num_clusters, seed=seed, maxIterations=max_iterations, initializationMode=initialization_mode )
    clusters = kmeans.predict(data_rdd).collect()
    clusterDict = defaultdict(list)
    for key, val in enumerate(clusters):
        clusterDict[val].append(labels[key])




    # TODO:
    # Use the given data and the cluster pparameters to train a K-Means model
    # Find the cluster id corresponding to data point (a car)
    # Return a list of lists of the titles which belong to the same cluster
    # For example, if the output is [["Mercedes", "Audi"], ["Honda", "Hyundai"]]
    # Then "Mercedes" and "Audi" should have the same cluster id, and "Honda" and
    # "Hyundai" should have the same cluster id



    return list(clusterDict.values())


if __name__ == "__main__":
    f = sc.textFile("dataset/cars.data")
    # print(f.take(10))
    data_rdd = f.map(lambda k: k.split(",")[1:])

    labels = []
    with open('dataset/cars.data') as f:
        for line in f:
            labels.append(line.split(",")[0])

    clusters = get_clusters(data_rdd, labels)
    for cluster in clusters:
        print(','.join(cluster))

from pyspark.mllib.tree import RandomForest
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors

sc = SparkContext()


def predict(training_data, test_data):
    # TODO: Train random forest classifier from given data
    # Result should be an RDD with the prediction of the random forest for each
    # test data point
    #
    model = RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=25, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=8, maxBins=64)

    predictions =  model.predict(test_data.map(lambda x: x.features))
    return predictions


if __name__ == "__main__":
    raw_training_data = sc.textFile("dataset/training.data")
    raw_test_data = sc.textFile("dataset/test-features.data")



    training_data = raw_training_data.map(lambda k: LabeledPoint(
                                                        k.split(",")[-1],
                                                         Vectors.dense(k.split(",")[0:-1] )))
    test_data = raw_test_data.map(lambda k : LabeledPoint(0,
                                                        Vectors.dense(k.split(","))
                                                        ))


    predictions = predict(training_data, test_data)

    # You can take a look at dataset/test-labels.data to see if your
    # predictions were right
    for pred in predictions.collect():
        print(int(pred))

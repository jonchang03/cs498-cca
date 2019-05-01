from pyspark import *
from pyspark.sql import SparkSession
from graphframes import *

sc = SparkContext()
spark = SparkSession.builder.appName('fun').getOrCreate()


def get_shortest_distances(graphframe, dst_id):
    # TODO
    # Find shortest distances in the given graphframe to the vertex which has id `dst_id`
    # The result is a dictionary where key is a vertex id and the corresponding value is
    # the distance of this node to vertex `dst_id`.
    results = graphframe.shortestPaths(landmarks=[dst_id])
    results.orderBy('id', ascending=False)

    resultDict = dict(results.collect())
    # print(resultDict)
    for key, value in resultDict.items():
        if not resultDict[key]:
            print("{} {}".format(key, -1))
        else:
            print("{} {}".format(key, resultDict[key]['1']))

    return {}


if __name__ == "__main__":
    vertex_list = []
    edge_list = []
    with open('dataset/graph.data') as f:
        for line in f:
            src = line.split()[0]  # TODO: Parse src from line
            dst_list = line.split()[1:]  # TODO: Parse dst_list from line
            vertex_list.append((src,))
            edge_list += [(src, dst) for dst in dst_list]

    vertices = spark.createDataFrame(vertex_list, ['id'])  # TODO: Create vertices dataframe
    edges = spark.createDataFrame(edge_list, ['src', 'dst'])  # TODO: Create edges dataframe

    g = GraphFrame(vertices, edges)
    sc.setCheckpointDir("/tmp/shortest-paths")

    # We want the shortest distance from every vertex to vertex 1
    for k, v in get_shortest_distances(g, '1').items():
        print(k, v)

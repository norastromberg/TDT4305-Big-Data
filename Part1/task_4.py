from pyspark.shell import spark, sqlContext

folder_name = "../data/Part_1_results/"

def initialize():
    sc = spark.sparkContext
    friendship_table_fname = "../data/yelp_top_users_friendship_graph.csv"
    friendship_textfile = sc.textFile(friendship_table_fname)
    friendship_rdd = friendship_textfile.map(lambda line: line.split(','))
    return sc, friendship_rdd


def task4():
    sc, friendship_rdd = initialize()
    out_degrees, in_degrees = task4a(sc, friendship_rdd)
    task4b(sc, out_degrees, in_degrees)
    return


def task4a(sc,friendship_rdd):
    header = friendship_rdd.first()
    f_rdd = friendship_rdd.filter(lambda row: row != header)

    # Top 10 OUT-degree-nodes:
    out_nodes = f_rdd.map(lambda fields: fields[0])
    out_degrees = out_nodes.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
    top_10_out_degrees = out_degrees.takeOrdered(10, key=lambda x: -x[1])


    # Top 10 IN-degree-nodes:
    in_nodes = f_rdd.map(lambda fields: fields[1])
    in_degrees = in_nodes.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
    top_10_in_degrees = in_degrees.takeOrdered(10,key=lambda x: -x[1])

    # Saving to file
    #A_rdd = sc.parallelize([top_10_out_degrees, top_10_in_degrees])
    #A_rdd.repartition(1).saveAsTextFile(folder_name + 'task_4a')

    # Preparing for task 4b:
    out_d_list = out_degrees.map(lambda fields: fields[1]).collect()
    in_d_list = in_degrees.map(lambda fields: fields[1]).collect()
    return out_d_list, in_d_list


def task4b(sc, out_degrees, in_degrees):     # Find the mean and median for number of in and out degrees in the friendship graph
    median_out = median(out_degrees)
    mean_out = mean(out_degrees)
    result_str_out = ('Median out degrees = ', median_out, ' Mean out degrees = ', mean_out)

    median_in = median(in_degrees)
    mean_in = mean(in_degrees)
    result_str_in = ('Median in degrees = ', median_in, ' Mean in degrees = ', mean_in)

    # B_rdd = sc.parallelize([result_str_out, result_str_in])
    # B_rdd.repartition(1).saveAsTextFile(folder_name + 'task_4b')
    return


def median(lst):
    n = len(lst)
    lst.sort()
    if n % 2 == 0:
        median1 = lst[n // 2]
        median2 = lst[n // 2 - 1]
        median = (median1 + median2) / 2
    else:
        median = lst[n // 2]
    return median

def mean(lst):
    n = len(lst)
    get_sum = sum(lst)
    mean = get_sum / n
    return mean

def main():
    task4()


if __name__ == '__main__':
    main()
from pyspark.shell import spark

folder_name = "../data/Part_1_results/"

def initialize():
    sc = spark.sparkContext
    business_table_fname = "../data/yelp_businesses.csv"
    business_textfile = sc.textFile(business_table_fname)
    business_rdd = business_textfile.map(lambda line: line.split('\t'))
    return sc, business_rdd


def task3():
    sc, bus_rdd = initialize()
    task3a(sc, bus_rdd)
    return

def task3a(sc,bus_rdd):     # What is the average rating for businesses in each city?
    header = bus_rdd.first()
    bus_rdd.filter(lambda row: row != header)
    # a = bus_rdd.map(lambda fields: (fields[3], fields[8])).groupByKey().mapValues(lambda x: sum(float(x)) / len(float(x))).collect()

    atuple = (0, 0)
    bus_rdd = bus_rdd.aggregateByKey(atuple, lambda a, b: (a[0] + b, a[1] + 1),
                                     lambda a, b: (a[0] + b[0], a[1] + b[1]))
    bus_rdd = bus_rdd.mapValues(lambda v: v[0] / v[1])
    bus_rdd.repartition(1).saveAsTextFile(folder_name + 'task_3a')
    return


def main():
    task3()



if __name__ == '__main__':
    main()
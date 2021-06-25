from pyspark.shell import spark

def task1(sc):
    folder_name = "../data/Part_1_Results/"

    review_table_fname = "../data/yelp_top_reviewers_with_reviews.csv"
    rev_textfile = sc.textFile(review_table_fname)
    review_rdd = rev_textfile.map(lambda line: line.split('\t'))
    rev_header = review_rdd.first()
    review_rdd = review_rdd.filter(lambda row: row != rev_header)

    business_table_fname = "../data/yelp_businesses.csv"
    business_textfile = sc.textFile(business_table_fname)
    business_rdd = business_textfile.map(lambda line: line.split('\t'))
    bus_header = business_rdd.first()
    business_rdd = business_rdd.filter(lambda row: row != bus_header)

    friendship_table_name = "../data/yelp_top_users_friendship_graph.csv"
    friendship_textfile = sc.textFile(friendship_table_name)
    friendship_rdd = friendship_textfile.map(lambda line: line.split(','))
    friend_header = friendship_rdd.first()
    friendship_rdd = friendship_rdd.filter(lambda row: row != friend_header)

    print(review_rdd.count(), business_rdd.count(), friendship_rdd.count())
    rdd = sc.parallelize([review_rdd.count(), business_rdd.count(), friendship_rdd.count()])
    rdd.repartition(1).saveAsTextFile(folder_name + 'task_1')

    return review_rdd, business_rdd, friendship_rdd


def main():
    sc = spark.sparkContext
    review_rdd, business_rdd, friendship_rdd = task1(sc)

if __name__ == '__main__':
    main()
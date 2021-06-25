import base64
import math
from datetime import datetime
from pyspark import SQLContext
from pyspark.shell import spark

folder_name = "../data/Part_1_results/"

def initialize():
    sc = spark.sparkContext
    review_table_fname = "../data/yelp_top_reviewers_with_reviews.csv"
    rev_textfile = sc.textFile(review_table_fname)
    review_rdd = rev_textfile.map(lambda line: line.split('\t'))
    return sc, review_rdd


def task2():
    sc, rev_rdd = initialize()
    # task2a(sc, rev_rdd)
    # task2b(sc, rev_rdd)
    # task2c(sc)
    # task2d(sc, rev_rdd)
    # task2e(sc, rev_rdd)
    task2f(sc, rev_rdd)
    return

#
# def task2a(sc,rev_rdd):    # A) Num distinct user_ids:
#     distinct_users_rdd = rev_rdd.map(lambda fields: fields[1]).distinct()
#     A_rdd = sc.parallelize([distinct_users_rdd.count()])
#     A_rdd.repartition(1).saveAsTextFile(folder_name + 'task_2a')
#     return
#
#
# def task2b(sc, rev_rdd):     # B) What is the average number of the characters in a user review?
#     header = rev_rdd.first()
#     rev_rdd = rev_rdd.filter(lambda row: row != header)
#     review_length_rdd = rev_rdd.map(lambda field: len(base64.b64decode(field[3].encode('ascii')).decode('utf8')))
#     B_rdd = sc.parallelize([review_length_rdd.mean()])
#     B_rdd.repartition(1).saveAsTextFile(folder_name + 'task_2b')
#     return
#
# def task2c(sc):    # C) What is the business_id of the top 10 businesses with the most number of reviews?
#     sqlContext = SQLContext(sc)
#     rev = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema","true").option("delimiter", '\t').load('../data/yelp_top_reviewers_with_reviews.csv')
#     rev_df = rev.toDF('review_id', 'user_id', 'business_id', 'review_text', 'review_date')
#     rev_df.createOrReplaceTempView('review_table')
#     sql_df = sqlContext.sql(
#         "SELECT business_id, COUNT(review_id) FROM review_table GROUP BY business_id ORDER BY COUNT(review_id) DESC")
#     top_10_business_ids_rdd = sql_df.select('business_id').limit(10).rdd
#     top_10_business_ids_rdd = top_10_business_ids_rdd.map(toCSV)
#     C_rdd = sc.parallelize([top_10_business_ids_rdd.collect()])
#     C_rdd.repartition(1).saveAsTextFile(folder_name + 'task_2c')
#     return
#
#
# def task2d(sc, rev_rdd):    # D) Find the number of reviews per year
#     header = rev_rdd.first()
#     rev_rdd = rev_rdd.filter(lambda row: row != header)
#     id_year_rdd = rev_rdd.map(lambda fields: (get_date(fields[4]).year, fields[1]))
#     D_rdd = id_year_rdd.groupByKey().sortByKey().mapValues(len)
#     D_rdd.repartition(1).saveAsTextFile(folder_name + 'task_2d')
#     return
#
#
# def task2e(sc, rev_rdd):   # E) What is the time and date of the first and last review?
#     header = rev_rdd.first()
#     rev_rdd = rev_rdd.filter(lambda row: row != header)
#     min_max = [get_date(rev_rdd.map(lambda fields: fields[4]).min()),
#                get_date(rev_rdd.map(lambda fields: fields[4]).max())]
#     E_rdd = sc.parallelize(min_max)
#     E_rdd.repartition(1).saveAsTextFile(folder_name + 'task_2e')
#     return


def task2f(sc, rev_rdd):    # F) Calculate the Pearson correlation coefficient between the number of reviews by a user and the average number of the characters in the users reviews.
    header = rev_rdd.first()
    rev_rdd = rev_rdd.filter(lambda row: row != header)
    sqlContext = SQLContext(sc)
    decoded_df = rev_rdd.map(
        lambda fields: (fields[0], fields[1], base64.b64decode(fields[3].encode('ascii')).decode('utf8'))) \
        .toDF(['review_id', 'user_id', 'review_text'])
    decoded_df.createOrReplaceTempView('review_table')
    rev_stats_rdd = sqlContext.sql(
        "SELECT user_id, AVG (LENGTH (review_text)) as avg_text_lentgh, COUNT (review_id) as num_reviews"
        " FROM review_table GROUP BY user_id  order by avg_text_lentgh").rdd

    # AVG of avg_text_length per user
    avg_rev_length = rev_stats_rdd.map(lambda fields: fields[1]).mean()
    print avg_rev_length

    # AVG of avg nr of reviews per user
    avg_num_revs = rev_stats_rdd.map(lambda fields: fields[2]).mean()
    print(avg_num_revs)

    x_list = rev_stats_rdd.map(lambda fields: fields[1]).collect()
    y_list = rev_stats_rdd.map(lambda fields: fields[2]).collect()
    pearson_coeffisient = pearson_corr_coeff(x_list, y_list, avg_rev_length, avg_num_revs)
    print('PEARSON ',pearson_coeffisient)
    # F_rdd = sc.parallelize([pearson_coeffisient])
    # F_rdd.repartition(1).saveAsTextFile(folder_name + 'task_2f')

    # Check if coeff is correct
    # x_list = rev_stats_rdd.map(lambda fields: fields[1])
    # y_list = rev_stats_rdd.map(lambda fields: fields[2])
    # print("Correlation is: " + str(Statistics.corr(x_list, y_list, method="pearson")))
    return


def toCSV(rdd):
    for element in rdd:
        return ''.join(str(element))


def pearson_corr_coeff(x_list, y_list, avg_x, avg_y):
    A = [x - avg_x for x in x_list]
    AA = sum([a**2 for a in A])
    B = [y - avg_y for y in y_list]
    BB = sum([b ** 2 for b in B])
    AB = sum([a*b for a,b in zip(A,B)])
    coeffisient = AB/(math.sqrt(AA)*math.sqrt(BB))
    return coeffisient


def get_date(fields):
    date = datetime.fromtimestamp(float(fields))
    return date


def main():
    task2()




if __name__ == '__main__':
    main()
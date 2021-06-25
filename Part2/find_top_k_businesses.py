import base64
from pyspark import SQLContext
from pyspark.shell import spark


# Loading yelp_top_reviewers_with_reviews, splitting on tab
def load_reviews(sc):
    review_rdd = sc.textFile("../data/yelp_top_reviewers_with_reviews.csv").map(lambda line: line.split('\t'))
    return review_rdd


# Loading sentiment lexicon into rdd
def load_afinn():
    with open("../data/AFINN-111.txt") as f:
        line = f.readline()
        afinn_dict = {}
        while line:
            line = line.split()
            key = ' '.join(line[:-1])
            val = line[-1]
            afinn_dict.update({key: val})
            line = f.readline()
    return afinn_dict


# Loading stopwords into list. Using list instead of rdd here because there are only a small number of stopwords, therefore no point in using rdd
def load_stopwords():
    stopwords = [line.rstrip('\n') for line in open("../data/stopwords.txt")]
    return stopwords


# Decoding text-field from base64 and extracting only fields from original rdd: review_id, business_id and review_text
def decode_and_feature_extraction(rdd):
    # Removing header
    header = rdd.first()
    rev_rdd = rdd.filter(lambda row: row != header)

    # Selecting review_id, business_id and decoded review_text from original rdd
    decoded_rdd = rev_rdd.map(lambda fields: (fields[0], fields[2], base64.b64decode(fields[3].encode('ascii'))))
    return decoded_rdd


# Preparing rdd for polarity analysis (decode,
def prepare(rdd, stopwords):
    decoded_rdd = decode_and_feature_extraction(rdd)
    tokenized_rdd = decoded_rdd.map(lambda fields: (fields[0], fields[1], tokenize_and_remove_stopwords(fields[2].lower(), stopwords)))
    return tokenized_rdd


# Tokenizing a string into list of words. Removing stopwords and unecessary characters
def tokenize_and_remove_stopwords(review_text, stopwords):
    # There is a chance we might should consider not using this for-loop since it is time consuming
    for char in review_text:
        if char in "?.!/;:":
            review_text = review_text.replace(char, '')

    # Splitting text into words
    tokenized = review_text.rstrip().split()

    # Removing stopwords
    for word in tokenized:
        if word in stopwords:
            tokenized.remove(word)
    return tokenized


# Computing the polarity of each word in a word list
def polarity(word_list, sentiment_dict):
    polarity_score = 0
    for word in word_list:
        if word in sentiment_dict:
            polarity_score += int(sentiment_dict.get(word))
    return polarity_score


# Finding
def find_top_k_businesses(review_rdd, stopwords, sentiment_dict, sc, k):

    # Preparing rdd for polarity measure
    prepared_rdd = prepare(review_rdd, stopwords)

    # Mapping prepared rdd to contain 'review_id', 'business_id', 'polarity_score'
    rdd = prepared_rdd.map(
        lambda fields: (fields[0], fields[1], polarity(fields[2], sentiment_dict)))

    # SQL - Finding the top-k by summing polarity score for each business and order them in descending order
    sqlContext = SQLContext(sc)
    rev_df = rdd.toDF(['review_id', 'business_id', 'polarity_score'])
    rev_df.createOrReplaceTempView('review_table')
    sql_df = sqlContext.sql(
        "SELECT business_id, SUM(polarity_score) as score FROM review_table GROUP BY business_id ORDER BY score DESC ")
    #sql_df.show(100)
    top_k_business_ids = sql_df.select('business_id').limit(k).rdd.map(toCSV)

    # Saving to file
    result_rdd = sc.parallelize([top_k_business_ids.collect()])
    result_rdd.repartition(1).saveAsTextFile('../data/reults_part2/top_k_business_ids')
    return top_k_business_ids


# Transforming rdd-element into string to fit in csv
def toCSV(rdd):
    for element in rdd:
        return ''.join(str(element))


def main():
    # Creating a spark context
    sc = spark.sparkContext

    # Loading yelp reviews
    review_rdd = load_reviews(sc)

    # Loading sentinent dictionary
    afinn_dict = load_afinn()

    # Loading stopwords
    stopwords = load_stopwords()

    # Finding top k business ids based on polarity score
    top_k_business_ids = find_top_k_businesses(review_rdd, stopwords, afinn_dict, sc, 10)



if __name__ == '__main__':
    main()
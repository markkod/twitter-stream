import sys
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import requests
import requests_oauthlib

#keywords = ['corbyn', 'johnson', 'brexit', 'election', 'boris']

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(keyword=w[0], word_count=w[1]))
        # create a DF from the Row RDD
        keywords_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        keywords_df.registerTempTable("keywords")
        # get the top 10 hashtags from the table using SQL and print them
        keywords_counts_df = sql_context.sql(
            "select keyword, word_count from keywords order by word_count desc")
        keywords_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        #send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()
        print("Error: %s" % str(e))

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 10 seconds
ssc = StreamingContext(sc, 10)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


# split each tweet into words
tweet_words = dataStream.flatMap(lambda line: line.split(" "))
# filter out words which have at least 4 letters and map them to word, count pairs
found_keywords = tweet_words.filter(lambda w: len(w) > 3).map(lambda x: (x.lower(), 1))
# adding the count of each keyword to its last count
keyword_totals = found_keywords.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
keyword_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()


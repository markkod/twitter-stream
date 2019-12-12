import sys
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import requests
import requests_oauthlib
#Modified from: https://www.toptal.com/apache/apache-spark-streaming-twitter

keywords = ['corbyn', 'johnson', 'brexit', 'election', 'boris', 'vote', 'labour', 'tory', 'tories', 'conservatives', 'swinson', 'polls']

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
        # get the top keywords in ascending order from the table using SQL and print them
        keywords_counts_df = sql_context.sql(
            "select keyword, word_count from keywords order by word_count desc")
        keywords_counts_df.show()
    except:
        e = sys.exc_info()
        print("Error: %s" % str(e))

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
conf.set('spark.eventLog.enabled', 'true')
conf.set('spark.eventLog.dir', '/usr/local/Cellar/apache-spark/2.4.4/libexec/tmp/spark-events')
conf.set('spark.history.fs.logDirectory', '/usr/local/Cellar/apache-spark/2.4.4/libexec/tmp/spark-events')
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 1 seconds
ssc = StreamingContext(sc, 1)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


# split each tweet into words
tweet_words = dataStream.flatMap(lambda line: line.split(" "))
# filter out keywords map them to word, count pairs
found_keywords = tweet_words.filter(lambda w: w.lower() in keywords).map(lambda x: (x.lower(), 1))
# adding the count of each keyword to its last count
keyword_totals = found_keywords.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
keyword_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()


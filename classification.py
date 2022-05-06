import pyspark as ps
import pyspark.sql.functions as F
import numpy as np
import nltk
from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer, StopWordsRemover, RegexTokenizer
from nltk.corpus import stopwords
nltk.download('stopwords')


def main():
    path = 'tweets_sentiment_analysis.csv'
    #path = 's3://twitterworddashboard/tweets_sentiment_analysis_small.csv'

    # Loads sentiment sheet into df
    spark = SparkSession.builder \
        .master("local") \
        .appName("Print") \
        .getOrCreate()
    tweets_df = spark.read.option('header', False).csv(path)

    # Format tweet sentiment sheet
    tweets_df = tweets_df.drop('_c1', '_c2', '_c3', '_c4')
    tweets_df = tweets_df.withColumnRenamed('_c0', 'Sentiment') \
        .withColumnRenamed('_c5', 'Tweet')

    # Tweet text processing: remove stop words, remove user tags, web urls, numbers, symbols
    stopword_list = stopwords.words('english')
    tokenizer = RegexTokenizer(inputCol='Tweet', outputCol='Tweet Formatted')
    tweets_df = tokenizer.transform(tweets_df) \
        .drop('Tweet').withColumnRenamed('Tweet Formatted', 'Tweet')
    remover = StopWordsRemover(inputCol='Tweet', outputCol='Tweet Formatted', stopWords=stopword_list)
    tweets_df = remover.transform(tweets_df) \
        .drop('Tweet').withColumnRenamed('Tweet Formatted', 'Tweet')
    tweets_df = tweets_df.withColumn('Tweet', F.concat_ws(' ', 'Tweet'))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'@[a-zA-Z]+', '')) \
        .withColumn('Tweet', F.regexp_replace('Tweet', r'http\S+', '')) \
        .withColumn('Tweet', F.regexp_replace('Tweet', r'[^a-zA-Z ]', ''))
    tweets_df = tokenizer.transform(tweets_df) \
        .drop('Tweet').withColumnRenamed('Tweet Formatted', 'Tweet')
    tweets_df.show()

    # Vectorizes tweets into word counts
    cv = CountVectorizer(inputCol="Tweet", outputCol="Features", minDF=2.0)
    model = cv.fit(tweets_df)
    result = model.transform(tweets_df)


if __name__ == '__main__':
    main()

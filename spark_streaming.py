from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("TwitterWordDashboard") \
        .getOrCreate()

    tweets = spark.readStream.format('socket').option('host', '0.0.0.0').option('port', 5555).load()
    print(tweets)
#

if __name__ == '__main__':
    main()

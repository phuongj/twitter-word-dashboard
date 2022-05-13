import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = SparkSession \
        .builder \
        .appName("TwitterWordDashboard") \
        .getOrCreate()

    lines = spark \
        .readStream \
        .format('socket') \
        .option('host', '0.0.0.0') \
        .option('port', 9999) \
        .load()

    words = lines.select(
        F.explode(
            F.split(lines.value, ' ')
        ).alias('word')
    )

    wordCounts = words.groupBy("word").count()

    query = wordCounts \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    main()

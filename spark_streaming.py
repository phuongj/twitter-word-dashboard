import pyspark.sql.functions as F
from classification import process_text
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
            F.split(lines.value, 't_end')
        ).alias('tweet')
    )

    words = process_text(words)

    query = words \
        .writeStream \
        .format('console') \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    main()

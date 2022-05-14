import pyspark.sql.functions as F
from classification import process_text
from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidatorModel

path = 'C:/Users/Justin/Google Drive/College/SDSU/Masters/2022 Spring/CS649 Big Data/Project/twitter_word_dashboard/models/lsvc_model.py/'


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

    #
    words = process_text(words)

    #
    model = load_model(path)
    words = classify_text(words, model)

    query = words \
        .writeStream \
        .format('console') \
        .start()

    query.awaitTermination()


def load_model(model_path):
    return CrossValidatorModel.load(model_path)


def classify_text(df, model):
    df = model.transform(df)
    return df


if __name__ == '__main__':
    main()

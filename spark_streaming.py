import pyspark.sql.functions as F
from classification import process_text
from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidatorModel

# See readme for variable details
path = ''


def main():
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .getOrCreate()

    # Receive tweets from streaming client socket
    lines = spark \
        .readStream \
        .format('socket') \
        .option('host', '0.0.0.0') \
        .option('port', 9999) \
        .load()

    # Converts tweets to a dataframe, one tweet per row
    words = lines.select(
        F.explode(
            F.split(lines.value, 't_end')
        ).alias('tweet')
    )

    # Removes unnecessary characters from tweets using the same rules
    # as applied to the train/test set during classification
    words = process_text(words)

    # Classify tweets for sentiment
    model = load_model(path)
    words = classify_text(words, model)

    # Outputs classified tweets to the console in live batches
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

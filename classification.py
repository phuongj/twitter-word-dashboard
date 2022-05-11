import pyspark.sql.functions as F
import numpy as np
import nltk
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, LinearSVC, DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, HashingTF
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.types import IntegerType
from nltk.corpus import stopwords
nltk.download('stopwords')


def main():
    path = 'tweets_sentiment_analysis.csv'
    path = 's3://twitterworddashboard/tweets_sentiment_analysis.csv'

    # Loads sentiment sheet into df
    spark = SparkSession.builder \
        .master("local") \
        .appName("Print") \
        .getOrCreate()
    tweets_df = spark.read.option('header', False).csv(path)

    # Format tweet sentiment sheet
    tweets_df = tweets_df.drop('_c1', '_c2', '_c3', '_c4')
    tweets_df = tweets_df.withColumnRenamed('_c0', 'label') \
        .withColumnRenamed('_c5', 'tweet')
    tweets_df = tweets_df.withColumn('label', F.col('label').astype(IntegerType()))

    # Tweet text processing: remove stop words, remove user tags, web urls, numbers, symbols
    tweets_df = process_text(tweets_df)

    # Splits data into training and test sets
    tweets_df, unused_df = tweets_df.randomSplit([0.001, 0.999], 24)
    training, test = tweets_df.randomSplit([0.8, 0.2], 24)

    # Creates transformers and evaluator used in pipelines
    rtokenizer = RegexTokenizer(inputCol='tweet',
                                outputCol='words')
    hashingTF = HashingTF(inputCol=rtokenizer.getOutputCol(),
                          outputCol='features')
    evaluator = BinaryClassificationEvaluator()

    # Creates logistic regression pipeline and cross validator
    lr = LogisticRegression()
    lr_grid = ParamGridBuilder() \
        .addGrid(lr.maxIter, [5, 10]) \
        .addGrid(lr.regParam, [0.0, 0.1]) \
        .build()

    lr_pipeline = Pipeline(stages=[rtokenizer, hashingTF, lr])
    lr_cv = CrossValidator(estimator=lr_pipeline,
                           estimatorParamMaps=lr_grid,
                           evaluator=evaluator,
                           numFolds=3)
    lr_model = lr_cv.fit(training)
    print(evaluator.evaluate(lr_model.transform(test)))
    print('lr params: ' + str(lr_model.getEstimatorParamMaps()[np.argmax(lr_model.avgMetrics)]))

    # Output best lr model
    model_path = 's3://twitterworddashboard/lr_model.py'
    lr_model.write().overwrite().save(model_path)

    # Creates SVC pipeline and cross validator
    lsvc = LinearSVC()
    lsvc_grid = ParamGridBuilder() \
        .addGrid(lsvc.maxIter, [10]) \
        .addGrid(lsvc.regParam, [0.0]) \
        .build()

    lsvc_pipeline = Pipeline(stages=[rtokenizer, hashingTF, lsvc])
    lsvc_cv = CrossValidator(estimator=lsvc_pipeline,
                             estimatorParamMaps=lsvc_grid,
                             evaluator=evaluator,
                             numFolds=3)
    lsvc_model = lsvc_cv.fit(training)
    print(evaluator.evaluate((lsvc_model.transform(test))))
    print('lsvc params: ' + str(lsvc_model.getEstimatorParamMaps()[np.argmax(lsvc_model.avgMetrics)]))

    # Output best lsvc model
    model_path = 's3://twitterworddashboard/lsvc_model.py'
    lsvc_model.write().overwrite().save(model_path)

    # Creates random forest pipeline and cross validator
    dt = DecisionTreeClassifier()
    dt_grid = ParamGridBuilder() \
        .addGrid(dt.maxDepth, [3, 5]) \
        .addGrid(dt.maxBins, [2, 4]) \
        .build()

    dt_pipeline = Pipeline(stages=[rtokenizer, hashingTF, dt])
    dt_cv = CrossValidator(estimator=dt_pipeline,
                           estimatorParamMaps=dt_grid,
                           evaluator=evaluator,
                           numFolds=3)
    dt_model = dt_cv.fit(training)
    print(evaluator.evaluate(dt_model.transform(test)))
    print('dt params: ' + str(dt_model.getEstimatorParamMaps()[np.argmax(dt_model.avgMetrics)]))

    # Output best dt model
    model_path = 's3://twitterworddashboard/dt_model.py'
    dt_model.write().overwrite().save(model_path)


def process_text(df):
    stopword_list = stopwords.words('english')
    rtokenizer = RegexTokenizer(inputCol='tweet',
                                outputCol='tweet Formatted')
    remover = StopWordsRemover(inputCol='tweet',
                               outputCol='tweet Formatted',
                               stopWords=stopword_list)

    # Remove stop words
    df = rtokenizer.transform(df) \
        .drop('tweet').withColumnRenamed('tweet formatted', 'tweet')
    df = remover.transform(df) \
        .drop('tweet').withColumnRenamed('tweet formatted', 'tweet')

    # Remove user tags, web urls, numbers, symbols
    df = df.withColumn('tweet', F.concat_ws(' ', 'tweet'))
    df = df.withColumn('tweet', F.regexp_replace('tweet', r'@[a-zA-Z]+', '')) \
        .withColumn('tweet', F.regexp_replace('tweet', r'http\S+', '')) \
        .withColumn('tweet', F.regexp_replace('tweet', r'[^a-zA-Z ]', ''))

    # Remove rows with empty arrays
    df = rtokenizer.transform(df) \
        .drop('tweet').withColumnRenamed('tweet formatted', 'tweet')
    df = df.filter(F.size('tweet') > 0)
    df = df.withColumn('tweet', F.concat_ws(' ', 'tweet'))

    # Change nonzero label values to 1
    df = df.withColumn('label', F.when(df.label == 0, df.label).otherwise(1))

    return df


if __name__ == '__main__':
    main()

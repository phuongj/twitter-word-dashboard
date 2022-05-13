import pyspark.sql.functions as F
import numpy as np
import nltk
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, LinearSVC, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, HashingTF
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.types import IntegerType
from nltk.corpus import stopwords

nltk.download('stopwords')

path = 'tweets_sentiment_analysis.csv'
#path = 's3://twitterworddashboard/tweets_sentiment_analysis.csv'


def main():
    spark = SparkSession \
        .builder \
        .appName("Print") \
        .getOrCreate()

    # Loads sentiment sheet into df
    tweets_df = spark.read.option('header', False).csv(path)

    # Format tweet sentiment sheet
    tweets_df = tweets_df.drop('_c1', '_c2', '_c3', '_c4')
    tweets_df = tweets_df.withColumnRenamed('_c0', 'label') \
        .withColumnRenamed('_c5', 'tweet')
    tweets_df = tweets_df.withColumn('label', F.col('label').astype(IntegerType()))

    # Tweet text processing: remove stop words, remove user tags, web urls, numbers, symbols
    tweets_df = process_text(tweets_df)

    # Change nonzero label values to 1
    tweets_df = tweets_df.withColumn('label', F.when(tweets_df.label == 0, tweets_df.label).otherwise(1))

    # Splits data into training and test sets
    #tweets_df, unused_df = tweets_df.randomSplit([0.001, 0.999], 24)
    training, test = tweets_df.randomSplit([0.8, 0.2], 24)

    # Creates transformers and evaluator used in pipelines
    rtokenizer = RegexTokenizer(inputCol='tweet',
                                outputCol='words')
    hashingTF = HashingTF(inputCol=rtokenizer.getOutputCol(),
                          outputCol='features')
    evaluator = BinaryClassificationEvaluator()
    '''
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
    '''
    # Creates naive bayes pipeline and cross validator
    nb = NaiveBayes()
    nb_grid = ParamGridBuilder() \
        .addGrid(nb.modelType, ['multinomial']) \
        .build()

    nb_pipeline = Pipeline(stages=[rtokenizer, hashingTF, nb])
    nb_cv = CrossValidator(estimator=nb_pipeline,
                           estimatorParamMaps=nb_grid,
                           evaluator=evaluator,
                           numFolds=3)
    nb_model = nb_cv.fit(training)
    print(evaluator.evaluate(nb_model.transform(test)))
    print('nb params: ' + str(nb_model.getEstimatorParamMaps()[np.argmax(nb_model.avgMetrics)]))
    '''
    # Output best nb model
    model_path = 's3://twitterworddashboard/nb_model.py'
    nb_model.write().overwrite().save(model_path)
    '''


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

    return df


if __name__ == '__main__':
    main()

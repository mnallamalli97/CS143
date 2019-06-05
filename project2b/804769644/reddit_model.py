#imports
from __future__ import print_function
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

from pyspark.sql.functions import udf, from_unixtime, lower, col, unix_timestamp, regexp_replace
from pyspark.sql.types import StringType, ArrayType, IntegerType, DateType
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel

#taken from spec for task 7
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator


#function to generate ngrams
from cleantext import sanitize
def udf_sanitize2(text):
	allgrams = sanitize(text)
	unigrams, bigrams, trigrams = allgrams[1:]
	return unigrams.split(" ") + bigrams.split(" ") + trigrams.split(" ")

def main(context):
	"""Main function takes a Spark SQL context."""
	# YOUR CODE HERE
	# YOU MAY ADD OTHER FUNCTIONS AS NEEDED
	#task 1 - loading comments, submissions, and labelled data into PySpark
	try:
		comments = context.read.load("comments.parquet")
		submissions = context.read.load("submissions.parquet")
		labeled_data = context.read.load("labeled_data.parquet")
	except:
		comments = context.read.json("comments-minimal.json.bz2")
		comments.write.parquet("comments.parquet")
		submissions = context.read.json("submissions.json.bz2")
		submissions.write.parquet("submissions.parquet")
		labeled_data = context.read.load("labeled_data.csv", format = 'csv', sep = ',', header = "true")
		labeled_data.write.parquet("labeled_data.parquet")
	
	#task 2 - only get data associated with labeled data
	comments.createOrReplaceTempView("comments")
	labeled_data.createOrReplaceTempView("labeled_data")
	joined_data = context.sql("select labeled_data.Input_id, labeled_data.labeldjt, body from comments join labeled_data on id=Input_id")

	
	#task 4 - generating unigrams, bigrams, and trigrams for each comment
	#task 5 - putting all features in one column as an array of strings
	udf_grams = udf(udf_sanitize2, ArrayType(StringType()))
	joined_column = joined_data.withColumn('ngrams', udf_grams(joined_data.body))

	#task 6a - turn raw features into sparse feature vector
	#CITE - https://machinelearningmastery.com/prepare-text-data-machine-learning-scikit-learn/
	vectorizer = CountVectorizer(inputCol = 'ngrams', outputCol = 'features', minDF = 10, binary = True)
	tokenized = vectorizer.fit(joined_column)
	vector = tokenized.transform(joined_column)

	#task 6b - create positive and negative lebeled columns
	#CITE - https://changhsinlee.com/pyspark-udf/
	#CITE - https://stackoverflow.com/questions/47583007/if-statement-pyspark
	#vector.show()
	vector.createOrReplaceTempView("vector")
	positive_udf = udf(lambda z: 1 if z == 1 else 0, IntegerType())
	negative_udf = udf(lambda z: 1 if z == -1 else 0, IntegerType())
	vector = vector.withColumn("Positive", positive_udf(vector.labeldjt))
	vector = vector.withColumn("Negative", negative_udf(vector.labeldjt))
	if not os.path.exists("project2/pos.model") or not os.path.exists("project2/neg.model"):
		#task 7
		#CITE - taken from spec
		# Initialize two logistic regression models.
		# Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
		poslr = LogisticRegression(labelCol="Positive", featuresCol="features", maxIter=10)
		neglr = LogisticRegression(labelCol="Negative", featuresCol="features", maxIter=10)
		# This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
		posEvaluator = BinaryClassificationEvaluator(labelCol="Positive")
		negEvaluator = BinaryClassificationEvaluator(labelCol="Negative")
		# There are a few parameters associated with logistic regression. We do not know what they are a priori.
		# We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
		# We will assume the parameter is 1.0. Grid search takes forever.
		posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
		negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
		# We initialize a 5 fold cross-validation pipeline.
		posCrossval = CrossValidator(estimator=poslr, evaluator=posEvaluator, estimatorParamMaps=posParamGrid, numFolds=5)
		negCrossval = CrossValidator(estimator=neglr, evaluator=negEvaluator, estimatorParamMaps=negParamGrid, numFolds=5)
		# Although crossvalidation creates its own train/test sets for
		# tuning, we still need a labeled test set, because it is not
		# accessible from the crossvalidator (argh!)
		# Split the data 50/50
		posTrain, posTest = vector.randomSplit([0.5, 0.5])
		negTrain, negTest = vector.randomSplit([0.5, 0.5])
		# Train the models
		print("Training positive classifier...")
		posModel = posCrossval.fit(posTrain)
		print("Training negative classifier...")
		negModel = negCrossval.fit(negTrain)
		# Once we train the models, we don't want to do it again. We can save the models and load them again later.
		try:
			posModel.save("project2/pos.model")
			negModel.save("project2/neg.model")
		except:
			pass
	else:
		posModel = CrossValidatorModel.load("project2/pos.model")
		negModel = CrossValidatorModel.load("project2/neg.model")

	#task 8
	#READ IN FULL FILE COMMENTS-minimal.json.bz2
	#need - timestamp when comment was created
	#need - title of submission that comment was made on
	#need - state that the commenter is from
	comments = comments.withColumn("title_id", regexp_replace("link_id", r'^t3_', ''))
	comments = comments.join(submissions, comments.title_id == submissions.id) \
						.select(comments.id.alias("comment_id"), comments.title_id, comments.created_utc, comments.author_flair_text, submissions.title, comments.body, comments.score.alias("comment_score"), submissions.id.alias("submission_id"), submissions.score.alias("submission_score"))

	#task 9
	#CITE - https://stackoverflow.com/questions/49301373/pyspark-filter-dataframe-based-on-multiple-conditions
	#how to filter
	#remove comments that contain "/s" and start with "&gt"
	comments = comments.filter("body NOT LIKE '%/s%' and body NOT LIKE '&gt;%'")
	#comments.show()
	#repeating task 4, 5, and 6a
	comments = comments.sample(False, 0.02, None)
	comments = comments.withColumn('ngrams', udf_grams(comments.body))
	comments = tokenized.transform(comments)

	#threshold and label the probabilities
	#create pos and neg columns
	p_thresh = udf(lambda z: 1 if z[1] > 0.2 else 0, IntegerType())
	n_thresh = udf(lambda z: 1 if z[1] > 0.25 else 0, IntegerType())
	posResult = posModel.transform(comments)
	t = posResult.withColumn('pos', p_thresh(posResult.probability))
	t = t.select(col('comment_id'), t.features, t.created_utc, col('body'), t.author_flair_text, col('comment_score'), col('submission_id'), col('title'), col('submission_score'), col('ngrams'), col('pos'))
	negResult = negModel.transform(t)
	t = negResult.withColumn('neg', n_thresh(negResult.probability))
	commentsFINAL = t.select(col('comment_id'), col('created_utc'), col('body'), col('author_flair_text'), col('comment_score'), col('submission_id'), col('title'), col('submission_score'), col('ngrams'), col('pos'), col('neg'))
	#commentsFINAL.show()


	#task 10
	#10-1
	percent = commentsFINAL.select('pos').groupBy().avg()
	percent.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("10-1-pos.csv")
	percent = commentsFINAL.select('neg').groupBy().avg()
	percent.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("10-1-neg.csv")
	#10-2
	commentsFINAL = commentsFINAL.withColumn("date", from_unixtime(commentsFINAL.created_utc, "YYYY-MM-dd"))
	percent = commentsFINAL.groupBy("date").agg({"pos":"mean", "neg":"mean"})
	percent.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("10-2-date.csv")
	#10-3
	states = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware', 'district of columbia', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming']
	state_udf = udf(lambda state: state if state in states else None, StringType())
	commentsFINAL = commentsFINAL.withColumn("state", state_udf(lower(commentsFINAL.author_flair_text)))
	commentsFINAL = commentsFINAL.filter(commentsFINAL.state.isNotNull())
	percent = commentsFINAL.groupBy("state").agg({"pos":"mean", "neg":"mean"})
	percent.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("10-3-state.csv")
	#10-4
	percent = commentsFINAL.groupBy("comment_score").agg({"pos":"mean", "neg":"mean"})
	percent.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("10-4-cscore.csv")
	percent = commentsFINAL.groupBy("submission_score").agg({"pos":"mean", "neg":"mean"})
	percent.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("10-4-sscore.csv")

	#top ten positive and negative stories
	top_pos = commentsFINAL.groupBy('title').agg((F.sum('pos') / F.count('pos')).alias('P')).orderBy(F.desc('P')).limit(10)
	top_neg = commentsFINAL.groupBy('title').agg((F.sum('neg') / F.count('neg')).alias('P')).orderBy(F.desc('P')).limit(10)
	top_pos.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("top_pos_stories.csv")
	top_neg.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("top_neg_stories.csv")
	

if __name__ == "__main__":
	conf = SparkConf().setAppName("CS143 Project 2B")
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	sqlContext = SQLContext(sc)
	sc.addPyFile("cleantext.py")
	main(sqlContext)

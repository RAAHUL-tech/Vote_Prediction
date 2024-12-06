from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, hour, minute, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark Session
spark = SparkSession.builder.appName("ElectionPredictionTesting").getOrCreate()

# Load the saved Linear Regression model from HDFS
model_path = "hdfs://localhost:9000/election_data/linear_regression_models"
candidate_id = "940da220-a451-4cc9-acc5-ce8b8697542a"  # Replace with the candidate you're testing
model = LinearRegressionModel.load(f"{model_path}/lr_candidate_{candidate_id}.model")

# Load the test data
test_file_path = "hdfs://localhost:9000/election_data/voting_data2000.csv"
test_data = spark.read.csv(test_file_path, header=False, inferSchema=True)
test_data = test_data.toDF("voter_id", "candidate_id", "vote_time")

# Preprocess test data (same as during training)
test_data = test_data.withColumn("vote_time", to_timestamp("vote_time", "yyyy-MM-dd HH:mm:ss"))
test_data = test_data.filter(col("vote_time").isNotNull())
test_data = test_data.withColumn("hour", hour("vote_time")).withColumn("minute", minute("vote_time"))
test_data = test_data.filter(col("hour").isNotNull() & col("minute").isNotNull())
# Aggregate votes by candidate and time (modified for cumulative votes)
test_data = test_data.groupBy("hour", "minute", "candidate_id").count().withColumnRenamed("count", "votes")

# Create a Window specification to calculate cumulative votes for each candidate
window_spec = Window.partitionBy("candidate_id").orderBy("hour", "minute").rowsBetween(Window.unboundedPreceding,
                                                                                           Window.currentRow)

# Calculate cumulative votes
test_data = test_data.withColumn("cumulative_votes", F.sum("votes").over(window_spec))
test_data.show(truncate=False)
test_data.printSchema()
# Prepare the features (same as during training)
assembler = VectorAssembler(inputCols=["hour", "minute"], outputCol="features")
test_data = assembler.transform(test_data)

# Make predictions using the trained model
predictions = model.transform(test_data)

predictions = predictions.withColumn("scaled_votes", col("prediction"))

# Show the results (predictions with scaled votes)
predictions.show(truncate=False)
# Evaluate the model (if you have actual values for comparison)

# If the actual target variable is available, use it for evaluation
evaluator = RegressionEvaluator(labelCol="cumulative_votes", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

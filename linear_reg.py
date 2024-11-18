from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, minute, col, to_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("ElectionPredictionTrainingWithCV").getOrCreate()



# Step 1: Load and Process Historical Data
def load_and_process_file(file_path):
    data = spark.read.csv(file_path, header=False, inferSchema=True)
    data = data.toDF("voter_id", "candidate_id", "vote_time")

    # Convert vote_time to timestamp and extract hour and minute
    data = data.withColumn("vote_time", to_timestamp("vote_time", "yyyy-MM-dd HH:mm:ss"))
    data = data.filter(col("vote_time").isNotNull())
    data = data.withColumn("hour", hour("vote_time")).withColumn("minute", minute("vote_time"))
    data = data.filter(col("hour").isNotNull() & col("minute").isNotNull())

    # Aggregate votes by candidate and time (cumulative votes)
    votes_data = data.groupBy("hour", "minute", "candidate_id").count().withColumnRenamed("count", "votes")

    # Create a Window specification to calculate cumulative votes for each candidate
    window_spec = Window.partitionBy("candidate_id").orderBy("hour", "minute").rowsBetween(Window.unboundedPreceding,
                                                                                           Window.currentRow)

    # Calculate cumulative votes
    votes_data = votes_data.withColumn("cumulative_votes", F.sum("votes").over(window_spec))

    return votes_data


# Define file paths in HDFS
file_paths = [
    "hdfs://localhost:9000/election_data/voting_data2020.csv"
]

# Initialize the LinearRegression model
lr = LinearRegression(featuresCol="features", labelCol="scaled_votes")

# Initialize evaluator and param grid for cross-validation
evaluator = RegressionEvaluator(labelCol="scaled_votes", predictionCol="prediction", metricName="rmse")
param_grid = (ParamGridBuilder()
              .addGrid(lr.regParam, [0.01, 0.1, 0.5, 1.0])
              .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
              .build())

# Initialize CrossValidator
crossval = CrossValidator(estimator=lr,
                          estimatorParamMaps=param_grid,
                          evaluator=evaluator,
                          numFolds=5)  # Using 5-fold cross-validation

# Dictionary to store trained models
linear_regression_models = {}

# Train models for each candidate across all files
for file_path in file_paths:
    votes_data = load_and_process_file(file_path)
    candidate_ids = [row['candidate_id'] for row in votes_data.select("candidate_id").distinct().collect()]

    for candidate_id in candidate_ids:
        print(f"Training cross-validated model for candidate {candidate_id} from {file_path}...")

        try:
            candidate_data = votes_data.filter(votes_data.candidate_id == candidate_id)
            candidate_data = candidate_data.withColumn("scaled_votes", col("cumulative_votes"))

            # Assemble features
            assembler = VectorAssembler(inputCols=["hour", "minute"], outputCol="features")
            candidate_data = assembler.transform(candidate_data)

            # Train model with cross-validation
            cv_model = crossval.fit(candidate_data)
            best_model = cv_model.bestModel

            # Store the best model in dictionary
            linear_regression_models[candidate_id] = best_model
            print(f"Best model for candidate {candidate_id} trained successfully.")
        except Exception as e:
            print(f"Error training model for candidate {candidate_id}: {e}")

print("Training Complete")


# Save models locally and upload to HDFS
def save_and_upload_models(models, local_dir, hdfs_dir):
    os.makedirs(local_dir, exist_ok=True)
    for candidate_id, model in models.items():
        model_path = f"{local_dir}/lr_candidate_{candidate_id}.model"
        model.write().overwrite().save(model_path)
    os.system(f'hadoop fs -put -f {local_dir} {hdfs_dir}')


# Define local and HDFS paths
local_model_dir = "/tmp/linear_regression_models"
hdfs_model_dir = "hdfs://localhost:9000/election_data/"

# Save and upload models
save_and_upload_models(linear_regression_models, local_model_dir, hdfs_model_dir)

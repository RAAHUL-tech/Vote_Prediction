from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, minute, col, to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
import joblib
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("ElectionPredictionTraining").getOrCreate()

#Load and Process Historical Data from individual files
def load_and_process_file(file_path):
    data = spark.read.csv(file_path, header=False, inferSchema=True)
    data = data.toDF("voter_id", "candidate_id", "vote_time")

    # Convert vote_time to timestamp and extract hour and minute
    data = data.withColumn("vote_time", to_timestamp("vote_time", "yyyy-MM-dd HH:mm:ss"))
    data = data.filter(col("vote_time").isNotNull())
    data = data.withColumn("hour", hour("vote_time")).withColumn("minute", minute("vote_time"))
    data = data.filter(col("hour").isNotNull() & col("minute").isNotNull())

    # Aggregate votes by candidate and time
    votes_data = data.groupBy("hour", "minute", "candidate_id").count().withColumnRenamed("count", "votes")

    # Create a Window specification to calculate cumulative votes for each candidate
    window_spec = Window.partitionBy("candidate_id").orderBy("hour", "minute").rowsBetween(Window.unboundedPreceding,
                                                                                           Window.currentRow)

    # Calculate cumulative votes
    votes_data = votes_data.withColumn("cumulative_votes", F.sum("votes").over(window_spec))

    return votes_data

# List of file paths to process. Files are stored in HDFS
file_paths = [
    "hdfs://localhost:9000/election_data/voting_data2020.csv"
]

#Process each file individually and train an ARIMA model for each candidate
arima_models = {}

for file_path in file_paths:
    # Load and process data from the current file
    votes_data = load_and_process_file(file_path)

    # Convert Spark DataFrame to Pandas for ARIMA
    votes_pandas = votes_data.toPandas()
    #votes_pandas.to_csv("csv_file_path.csv", index=False)

    # Train ARIMA Model for Trend Prediction
    for candidate_id, data in votes_pandas.groupby("candidate_id"):
        try:
            # Prepare data for ARIMA
            data_arima = data[['hour', 'minute', 'cumulative_votes']].copy()

            # Scale votes before training
            data_arima['y'] = data_arima['cumulative_votes']
            data_arima['ds'] = pd.to_datetime(
                '2024-01-01 ' + data_arima['hour'].astype(str) + ':' + data_arima['minute'].astype(str))
            data_arima.to_csv("csv_file_path"+candidate_id+".csv", index=False)
            model = ARIMA(data_arima['y'], order=(5, 1, 0))
            model_fit = model.fit()

            # Store the trained ARIMA model for the candidate
            arima_models[candidate_id] = model_fit
        except Exception as e:
            print(f"Error training ARIMA model for candidate {candidate_id} in file {file_path}: {e}")

print("Training Complete")

# Save ARIMA models using joblib
os.makedirs("/tmp/arima_models", exist_ok=True)
for candidate_id, model_fit in arima_models.items():
    joblib.dump(model_fit, f"/tmp/arima_models/arima_candidate_{candidate_id}.joblib")

# Save models to HDFS
os.system('hadoop fs -put -f /tmp/arima_models hdfs://localhost:9000/election_data/')

import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, to_timestamp,sum as F_sum
from facenet_pytorch import InceptionResnetV1, MTCNN
import torch
import cv2
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import requests
from scipy.spatial.distance import cosine
import os
import joblib
from statsmodels.tsa.arima.model import ARIMA
from pyspark.ml.regression import LinearRegressionModel
import pandas as pd
from datetime import datetime, timedelta
from pyspark.ml.feature import VectorAssembler

# Kafka consumer setup
def consume_kafka_messages(topic_name='voting_data', group_id='voting_consumer_group'):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=group_id
    )
    return consumer

# Kafka producer setup
def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer

# Function to send data to Kafka
def send_to_kafka(producer, topic, message):
    try:
        producer.send(topic, value=message)
        producer.flush()
        print(f"Message sent to Kafka topic: {topic}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")


def load_facenet_model():
    mtcnn = MTCNN(keep_all=False)
    inception_resnet = InceptionResnetV1(pretrained='vggface2').eval()
    return mtcnn, inception_resnet



def preprocess_image(img_url):
    # Retrieve the image from the URL
    response = requests.get(img_url)
    if response.status_code == 200:
        image_array = np.asarray(bytearray(response.content), dtype=np.uint8)
        img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)  # Read image as BGR
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)  # Convert to RGB for FaceNet
        # Resize image to (160, 160) for FaceNet model
        img = cv2.resize(img, (160, 160))
        return img
    else:
        print(f"Failed to retrieve image from URL: {img_url}")
        return None

def extract_features(mtcnn, model, image_path):
    # Preprocess image from URL or path
    img = preprocess_image(image_path)

    if img is None:
        print("Image preprocessing failed.")
        return None

    # Detect face(s) in the image using MTCNN
    faces = mtcnn(img)
    if faces is None or len(faces) == 0:
        print("No face detected in the image")
        return None

    # Ensure the face tensor has three channels (1, 3, 160, 160)
    face = faces.unsqueeze(0)  # Add batch dimension
    # Pass the face through the model to get the embedding
    with torch.no_grad():
        embedding = model(face)

    return embedding

# Function to calculate the similarity between two images
def compare_faces(image_path1, image_path2, mtcnn, model):
    embedding1 = extract_features(mtcnn, model, image_path1)
    embedding2 = extract_features(mtcnn, model, image_path2)

    if embedding1 is None or embedding2 is None:
        print("One of the images did not have a detectable face.")
        return None

    # Calculate cosine similarity
    similarity = 1 - cosine(embedding1[0].numpy(), embedding2[0].numpy())
    return similarity



# Function to retrieve voter image from HDFS based on voter ID
def retrieve_voter_image_from_hdfs(voter_id, spark):
    # Initialize SparkSession
    hdfs_path = 'hdfs://localhost:9000/election_data/voter_data.csv'
    try:
        # Read the CSV file as a DataFrame with specified encoding
        voter_data_df = spark.read.csv(hdfs_path, header=True)  # Adjust encoding if needed

        # Filter the DataFrame to find the row with the specified voter_id
        voter_image_row = voter_data_df.filter(col("Voter_id") == voter_id).select("Picture").collect()

        # Check if a result was found
        if voter_image_row:
            voter_image = voter_image_row[0]["Picture"]
            return voter_image
        else:
            return None
    except:
        print("Error")


def train_arima_model(candidate_id, historical_data):
    """
    Train and save an ARIMA model for a specific candidate.
    """
    print(f"Training ARIMA model for candidate: {candidate_id}")
    model = ARIMA(historical_data['cumulative_votes'], order=(5, 1, 0)).fit()
    model_path = f"/tmp/arima_candidate_{candidate_id}.joblib"
    joblib.dump(model, model_path)
    os.system(f"hadoop fs -put -f {model_path} hdfs://localhost:9000/election_data/arima_models2/")
    return model

def load_arima_models(candidate_ids):
    """
    Load ARIMA models for all candidates from HDFS.
    Download the model files locally before loading them with joblib.
    """
    arima_models = {}

    for candidate_id in candidate_ids:
        hdfs_model_path = f"hdfs://localhost:9000/election_data/arima_models/arima_candidate_{candidate_id}.joblib"
        local_model_path = f"/tmp/arima_candidate_{candidate_id}.joblib"

        try:
            # Download the model file from HDFS to the local path
            os.system(f"hadoop fs -get {hdfs_model_path} {local_model_path}")

            # Load the model using joblib
            model = joblib.load(local_model_path)
            arima_models[candidate_id] = model
            print(f"Successfully loaded ARIMA model for candidate {candidate_id}.")

        except Exception as e:
            print(f"Failed to load ARIMA model for candidate {candidate_id}: {e}")

    return arima_models


def train_and_forecast(candidate_id, historical_data, arima_models, producer):
    """
    Train the ARIMA model for a candidate and forecast the next 10 minutes.
    """
    model = train_arima_model(candidate_id, historical_data)
    arima_models[candidate_id] = model
    forecast = model.forecast(steps=45)
    print(f"ARIMA Predictions for Candidate {candidate_id}: {forecast}")
    topic_name = f"data_{candidate_id}"
    forecast_data = forecast.tolist()
    print(forecast_data)
    historical_data['vote_time'] = historical_data['vote_time'].astype(str)
    historical_data_for_kafka = historical_data.to_dict(orient='records')
    print(historical_data_for_kafka)
    # Get the last timestamp from the historical data
    last_time_str = historical_data.iloc[-1]['vote_time']
    print("lsttime", last_time_str)
    last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
    print("lsttime", last_time)
    # Create timestamps for forecasted values
    forecast_interval = timedelta(minutes=1)
    forecast_with_timestamps = []

    for i, value in enumerate(forecast):
        next_time = last_time + (i + 1) * forecast_interval
        forecast_with_timestamps.append({'time': next_time.strftime("%Y-%m-%d %H:%M:%S"), 'votes': value})

    kafka_message = {
        'candidate_id': candidate_id,
        'forecast': forecast_with_timestamps,
        'historical_data': historical_data_for_kafka
    }
    print("Kafka_message",kafka_message)
    send_to_kafka(producer, topic_name, kafka_message)
    return forecast


def make_predictions(spark, debug_dir):
    # Load test data (only time data)
    test_file_path = "voting_data/voting_data2000.csv"
    test_data = spark.read.csv(test_file_path, header=False, inferSchema=True)
    test_data = test_data.toDF("voter_id", "candidate_id", "vote_time")
    columns_to_drop = ["voter_id", "candidate_id"]
    test_data = test_data.drop(*columns_to_drop)

    # Preprocess test data (extract hour and minute)
    test_data = test_data.withColumn("vote_time", to_timestamp("vote_time", "yyyy-MM-dd HH:mm:ss"))
    test_data = test_data.filter(col("vote_time").isNotNull())
    test_data = test_data.withColumn("hour", hour("vote_time")).withColumn("minute", minute("vote_time"))
    test_data = test_data.filter(col("hour").isNotNull() & col("minute").isNotNull())

    # Aggregate votes by hour and minute (count votes per time slot)
    test_data_aggregated = test_data.groupBy("hour", "minute").agg(F_sum("vote_time").alias("votes"))

    # Prepare the features for prediction
    assembler = VectorAssembler(inputCols=["hour", "minute"], outputCol="features")
    test_data_aggregated = assembler.transform(test_data_aggregated)

    # Path for the saved models
    model_path = "hdfs://localhost:9000/election_data/linear_regression_models"

    # Define candidate IDs (list all available candidates)
    candidate_ids = [
        "940da220-a451-4cc9-acc5-ce8b8697542a",
        "4714f68f-d08a-4f68-8a92-0be8f94687c0",
        "bd7098b3-4a02-4c6c-87a5-b5c96a5619a0"
    ]

    # Process each candidate
    for candidate_id in candidate_ids:
        print(f"Processing candidate: {candidate_id}")

        # Load the model for the candidate
        candidate_model_path = f"{model_path}/lr_candidate_{candidate_id}.model"
        model = LinearRegressionModel.load(candidate_model_path)

        # Make predictions using the trained model
        predictions = model.transform(test_data_aggregated)

        # Add scaled_votes column (rename prediction to scaled_votes)
        predictions = predictions.withColumn("scaled_votes", col("prediction"))

        # Save the predictions to a CSV file
        output_path = os.path.join(debug_dir, f"predictions_candidate_{candidate_id}.csv")
        predictions.select("hour", "minute", "scaled_votes") \
            .write.csv(output_path, header=True, mode="overwrite")

        print(f"Predictions for candidate {candidate_id} saved to {output_path}")

def main():
    debug_dir = "debug_data"
    os.makedirs(debug_dir, exist_ok=True)
    consumer = consume_kafka_messages()
    spark = SparkSession.builder \
        .appName("ElectionPrediction") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    producer = create_kafka_producer()
    make_predictions(spark, debug_dir)
    mtcnn, inception_resnet = load_facenet_model()  # Load the pre-trained FaceNet model
    candidate_ids = ["940da220-a451-4cc9-acc5-ce8b8697542a", "4714f68f-d08a-4f68-8a92-0be8f94687c0", "bd7098b3-4a02-4c6c-87a5-b5c96a5619a0"]
    arima_models = load_arima_models(candidate_ids)
    cumulative_votes = {candidate_id: [] for candidate_id in candidate_ids}
    running_totals = {cid: 0 for cid in candidate_ids}
    last_executed_hour = None
    print("Waiting for messages from Kafka...")

    for message in consumer:
        # Process each Kafka message
        record = message.value
        voter_id = record.get('Voter_id')
        candidate_id = record.get('Candidate_id')
        vote_time = record.get('Vote_Time')
        voter_image = record.get('Voter_image')

        print(f"Processing vote record for voter_id: {voter_id}")

        # Retrieve the second voter image path from HDFS
        voter_image_path_hdfs = retrieve_voter_image_from_hdfs(voter_id, spark)

        # Now perform face verification using the FaceNet model
        if voter_image_path_hdfs:
            print(f"Voter image for {voter_id} found in HDFS: {voter_image_path_hdfs}")
            similarity = compare_faces(voter_image, voter_image_path_hdfs, mtcnn, inception_resnet)
            if similarity is not None:
                print(f"Cosine similarity between the images: {similarity}")

                # Decide if they are a match (threshold can be adjusted)
                if similarity >= 0.7:  # Adjust the threshold as needed
                    print(f"Voter images match for {voter_id}.")
                else:
                    print(f"Voter images do not match for {voter_id}.")
            else:
                print(f"Face detection failed for one or both images for {voter_id}.")

        # Update cumulative votes
        vote_time_obj = datetime.strptime(vote_time, "%Y-%m-%d %H:%M:%S")
        running_totals[candidate_id] += 1
        cumulative_votes[candidate_id].append((vote_time_obj, running_totals[candidate_id]))
        # Every 10 minutes, train and forecast
        if vote_time_obj.minute == 0 and vote_time_obj.hour != 8 and vote_time_obj.hour != last_executed_hour:
            print("Training ARIMA models for all candidates...")
            last_executed_hour = vote_time_obj.hour
            # Prepare historical data for each candidate
            with ThreadPoolExecutor() as executor:
                futures = {}
                for cid in candidate_ids:
                    print("Training ARIMA models for candidate", cid)
                    historical_data = pd.DataFrame(cumulative_votes[cid], columns=["vote_time", "cumulative_votes"])
                    historical_data["vote_time"] = pd.to_datetime(historical_data["vote_time"])
                    historical_data = historical_data.set_index("vote_time").resample("T").last()  # Take the last cumulative value
                    historical_data["cumulative_votes"] = historical_data["cumulative_votes"].fillna(method="ffill")  # Forward-fill cumulative values
                    historical_data = historical_data.reset_index()
                    # Validate cumulative votes to ensure correctness
                    if (historical_data["cumulative_votes"].diff() < 0).any():
                        print(f"Warning: Cumulative votes decrease for candidate {cid}. Check input data!")
                    # Save historical data to CSV for debugging
                    historical_csv_path = os.path.join(debug_dir, f"historical_data_{cid}.csv")
                    historical_data.to_csv(historical_csv_path, index=False)
                    futures[cid] = executor.submit(train_and_forecast, cid, historical_data, arima_models, producer)

                # Collect forecasts with enhanced debugging
                forecasts = {}
                print("future.items:", futures.items())
                for cid, future in futures.items():
                    try:
                        result = future.result()
                        if result is None or result.empty:
                            print(f"Warning: No forecast data returned for candidate {cid}")
                        else:
                            print(f"Forecast for candidate {cid}: {result}")
                            if isinstance(result, pd.Series):
                                forecasts[cid] = result.tolist()
                            else:
                                print(f"Unexpected data type for forecast of candidate {cid}: {type(result)}")
                    except Exception as e:
                        print(f"Error occurred while fetching forecast for candidate {cid}: {e}")

                # Convert forecasts to DataFrame and save to CSV
                if forecasts:
                    forecast_csv_path = os.path.join(debug_dir, "forecasts.csv")
                    # Flatten the forecasts
                    flattened_forecasts = []
                    for cid, forecast in forecasts.items():
                        for i, value in enumerate(forecast):
                            flattened_forecasts.append({"candidate_id": cid, "minute": i + 1, "forecast": value})
                    forecast_df = pd.DataFrame(flattened_forecasts)
                    forecast_df.to_csv(forecast_csv_path, index=False)
                    print(f"Forecasts for the next 45 minutes saved to {forecast_csv_path}")
                else:
                    print("No forecast data available to save!")

        try:
            consumer.commit()
            print(f"Committed offset for voter_id: {voter_id}")
        except KafkaError as e:
            print(f"Failed to commit offset for voter_id {voter_id}: {e}")

    spark.stop()
    producer.close()

if __name__ == "__main__":
    main()

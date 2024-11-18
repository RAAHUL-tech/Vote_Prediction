import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from facenet_pytorch import InceptionResnetV1, MTCNN
import torch
import cv2
import numpy as np
import requests
from scipy.spatial.distance import cosine


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
        print("Processed Image shape:", img.shape)
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
def retrieve_voter_image_from_hdfs(voter_id, hdfs_path='hdfs://localhost:9000/election_data/voter_data.csv'):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("RealTimeElectionVoting") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

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

    finally:
        # Stop SparkSession after task completion
        spark.stop()


def main():
    consumer = consume_kafka_messages()
    mtcnn, inception_resnet = load_facenet_model()  # Load the pre-trained FaceNet model

    print("Waiting for messages from Kafka...")

    for message in consumer:
        # Process each Kafka message
        record = message.value
        voter_id = record.get('Voter_id')
        voter_image_path_kafka = record.get('Voter_image')

        print(f"Processing vote record for voter_id: {voter_id}")

        # Retrieve the second voter image path from HDFS
        voter_image_path_hdfs = retrieve_voter_image_from_hdfs(voter_id)

        if voter_image_path_hdfs:
            print(f"Voter image for {voter_id} found in HDFS: {voter_image_path_hdfs}")
        else:
            print(f"Voter image for {voter_id} not found in HDFS.")

        # Now perform face verification using the FaceNet model
        if voter_image_path_hdfs:
            similarity = compare_faces(voter_image_path_kafka, voter_image_path_hdfs, mtcnn, inception_resnet)
            if similarity is not None:
                print(f"Cosine similarity between the images: {similarity}")

                # Decide if they are a match (threshold can be adjusted)
                if similarity >= 0.7:  # Adjust the threshold as needed
                    print(f"Voter images match for {voter_id}.")
                else:
                    print(f"Voter images do not match for {voter_id}.")
            else:
                print(f"Face detection failed for one or both images for {voter_id}.")

        # Simulate processing delay
        time.sleep(1)
        try:
            consumer.commit()
            print(f"Committed offset for voter_id: {voter_id}")
        except KafkaError as e:
            print(f"Failed to commit offset for voter_id {voter_id}: {e}")


if __name__ == "__main__":
    main()

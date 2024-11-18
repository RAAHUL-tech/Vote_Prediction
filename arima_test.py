from pyspark.sql import SparkSession
import joblib
import pandas as pd
import os
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA

# ARIMA model testing
# Initialize Spark Session
spark = SparkSession.builder.appName("ElectionPredictionTesting").getOrCreate()

#Load ARIMA Models from HDFS
def load_arima_models():
    arima_models = {}
    arima_models_path = "hdfs://localhost:9000/election_data/arima_models"
    # List all files in the arima models directory
    files = os.popen(f'hadoop fs -ls {arima_models_path}').read().splitlines()

    for file in files:
        if file.endswith(".joblib"):
            model_path = file.split(" ")[-1]  # Get the full path
            # Load the model from HDFS
            model_data = os.popen(f'hadoop fs -get {model_path} /tmp').read()
            model = joblib.load(f'/tmp/{model_path.split("/")[-1]}')
            candidate_id = model_path.split("_")[-1].split(".")[0]
            arima_models[candidate_id] = model

    return arima_models


arima_models = load_arima_models()
print("Models loaded")

# Sample Input Data for Prediction . This data comes from kafka
real_time_vote = {
    "time": "2024-01-01 09:00:00",
    "candidate_id": "4714f68f-d08a-4f68-8a92-0be8f94687c0",
    "voter_id": "a9de86e2-d375-40f5-94b2-19a5e2be0a04"
}
vote_time_str = real_time_vote["time"].split(" ")[1]
# Convert the sample data to datetime format
vote_time = datetime.strptime(vote_time_str, "%H:%M:%S").time()

# Prepare the historical time series for ARIMA prediction
# For simplicity, assume we have the historical vote data for the candidate as `historical_data`
# This should be a time series of cumulative votes per timestamp for each candidate
historical_data = pd.DataFrame({
    'ds': [vote_time],  # Historical data (one timestamp)
    'y': [0]  # Placeholder for the initial votes (replace with actual values if available)
})

# Make Predictions with ARIMA Models for Each Candidate
arima_predictions = {}

for candidate_id, model in arima_models.items():
    # Use the historical cumulative votes for prediction (assuming 'y' is the cumulative votes)
    # In real scenario, replace historical_data with actual historical votes of the candidate
    historical_votes = historical_data['y']

    # Fit the ARIMA model on historical data (can use the ARIMA model's pre-fitted model)
    model_fit = model

    # Predict the future (next 10 minutes)
    forecast = model_fit.forecast(steps=10)

    # Extract the predicted votes (yhat)
    predicted_votes_scaled = forecast
    predicted_votes = predicted_votes_scaled  # Inverse scaling if needed

    arima_predictions[candidate_id] = predicted_votes.tolist()

    # Save forecast to CSV
    if candidate_id == list(arima_models.keys())[0]:
        # Save header for the first candidate
        pd.DataFrame(predicted_votes, columns=["predicted_votes"]).to_csv("candidate_forecast.csv", index=False)
    else:
        # Append without header for subsequent candidates
        pd.DataFrame(predicted_votes, columns=["predicted_votes"]).to_csv("candidate_forecast.csv", mode='a', index=False, header=False)

#Print the ARIMA Model Predictions
print("ARIMA Model Predictions for Future Votes:")
for candidate_id, predicted_votes in arima_predictions.items():
    print(f"Candidate {candidate_id} predicted votes for next 10 minutes: {predicted_votes}")

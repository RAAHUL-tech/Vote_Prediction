import pandas as pd
import matplotlib.pyplot as plt
import re

# List of cumulative vote file paths
cumulative_votes_paths = [
    "debug_data/historical_data_940da220-a451-4cc9-acc5-ce8b8697542a.csv",
    "debug_data/historical_data_4714f68f-d08a-4f68-8a92-0be8f94687c0.csv",
    "debug_data/historical_data_bd7098b3-4a02-4c6c-87a5-b5c96a5619a0.csv",
]

# Load cumulative votes data from all files
cumulative_data_list = []
for file_path in cumulative_votes_paths:
    # Extract candidate_id from the file path using regex
    match = re.search(r"historical_data_(.+)\.csv", file_path)
    if match:
        candidate_id = match.group(1)
    else:
        raise ValueError(f"Could not extract candidate_id from file path: {file_path}")

    # Load the data and add candidate_id as a new column
    data = pd.read_csv(file_path)
    data['vote_time'] = pd.to_datetime(data['vote_time'], format='%Y-%m-%d %H:%M:%S')
    data['candidate_id'] = candidate_id
    cumulative_data_list.append(data)

# Concatenate all cumulative data into a single DataFrame
cumulative_data = pd.concat(cumulative_data_list)

# Load forecast data
forecast_path = "debug_data/forecasts.csv"  # Path to forecast file
forecast_data = pd.read_csv(forecast_path)

# Convert forecast times relative to the last cumulative vote time
forecast_start_times = (
    cumulative_data.groupby("candidate_id")["vote_time"].max().to_dict()
)
forecast_data["forecast_time"] = forecast_data.apply(
    lambda row: forecast_start_times[row["candidate_id"]]
                + pd.to_timedelta(row["minute"], unit="m"),
    axis=1,
)

# Plot for each candidate
candidates = forecast_data["candidate_id"].unique()

for candidate in candidates:
    plt.figure(figsize=(12, 6))

    # Filter data for the candidate
    candidate_cumulative = cumulative_data[cumulative_data["candidate_id"] == candidate]
    candidate_forecast = forecast_data[forecast_data["candidate_id"] == candidate]

    # Plot past votes
    plt.plot(
        candidate_cumulative["vote_time"],
        candidate_cumulative["cumulative_votes"],
        label="Past Votes",
        color="blue",
        linewidth=2,
    )

    # Plot future votes
    plt.plot(
        candidate_forecast["forecast_time"],
        candidate_forecast["forecast"],
        label="Forecasted Votes",
        color="orange",
        linestyle="--",
        linewidth=2,
    )

    # Add labels, legend, and title
    plt.title(f"Votes Over Time for Candidate {candidate}")
    plt.xlabel("Time")
    plt.ylabel("Cumulative Votes")
    plt.legend()
    plt.grid()
    plt.tight_layout()

    # Show plot
    plt.show()

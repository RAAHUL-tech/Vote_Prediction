import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import json
import time

# Initialize session_state variables
def initialize_session_state():
    if 'historical_data' not in st.session_state:
        st.session_state.historical_data = {
            'candidate_1': [],
            'candidate_2': [],
            'candidate_3': []
        }
    if 'forecast_data' not in st.session_state:
        st.session_state.forecast_data = {
            'candidate_1': [],
            'candidate_2': [],
            'candidate_3': []
        }

# Function to consume data from Kafka topics
def consume_data(brokers, topics):
    """
    Consumes and updates historical and forecast data from Kafka topics for all candidates.
    Updates session_state variables.
    """
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=brokers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id="election_data_consumer"
    )

    print("Listening for messages on topics:", topics)

    for message in consumer:
        topic = message.topic
        data = message.value
        candidate_id = data.get("candidate_id", "Unknown")
        forecast = data.get("forecast", [])
        historical = data.get("historical_data", [])

        # Update the session state historical and forecast data
        if candidate_id == "940da220-a451-4cc9-acc5-ce8b8697542a":
            st.session_state.historical_data['candidate_1'] = historical
            st.session_state.forecast_data['candidate_1'] = forecast
        elif candidate_id == "4714f68f-d08a-4f68-8a92-0be8f94687c0":
            st.session_state.historical_data['candidate_2'] = historical
            st.session_state.forecast_data['candidate_2'] = forecast
        elif candidate_id == "bd7098b3-4a02-4c6c-87a5-b5c96a5619a0":
            st.session_state.historical_data['candidate_3'] = historical
            st.session_state.forecast_data['candidate_3'] = forecast

# Function to plot data
def plot_data():
    # Plotting historical and forecasted data for all candidates
    fig, ax = plt.subplots(figsize=(10, 6))

    for candidate in st.session_state.historical_data:
        # Plot historical data
        historical_df = pd.DataFrame(st.session_state.historical_data[candidate])
        if not historical_df.empty:
            historical_df['vote_time'] = pd.to_datetime(historical_df['vote_time'])
            ax.plot(historical_df['vote_time'], historical_df['cumulative_votes'], label=f"{candidate} Historical", marker='o')

        # Plot forecasted data (continuing from the last point of historical data)
        forecast_df = pd.DataFrame(st.session_state.forecast_data[candidate])
        if not forecast_df.empty:
            forecast_df['time'] = pd.to_datetime(forecast_df['time'])
            ax.plot(forecast_df['time'], forecast_df['votes'], label=f"{candidate} Forecasted", linestyle='--', marker='x')

    ax.set_title("Voting Data: Historical vs Forecasted")
    ax.set_xlabel("Time")
    ax.set_ylabel("Votes")
    ax.legend()

    # Display the plot
    st.pyplot(fig)

    # Find the candidate with the largest forecasted vote at the last point
    forecast_last_values = {}
    for candidate, forecast_df in st.session_state.forecast_data.items():
        if forecast_df:
            forecast_last_values[candidate] = pd.DataFrame(forecast_df)['votes'].iloc[-1]

    if forecast_last_values:
        winning_candidate = max(forecast_last_values, key=forecast_last_values.get)
        winning_votes = int(forecast_last_values[winning_candidate])
        st.write(f"The winning candidate (based on forecasted votes) is: {winning_candidate} with {winning_votes} votes.")


def plot_linear_reg_data():
    file_paths = [
        'linear_reg_plot_data/predictions_candidate_940da220-a451-4cc9-acc5-ce8b8697542a.csv/part-00000-f4a13847-a97a-4eaa-bc16-e56f071ae2e8-c000.csv',
        'linear_reg_plot_data/predictions_candidate_4714f68f-d08a-4f68-8a92-0be8f94687c0.csv/part-00000-461f9ee2-054d-452a-af2a-41bda54aa763-c000.csv',
        'linear_reg_plot_data/predictions_candidate_bd7098b3-4a02-4c6c-87a5-b5c96a5619a0.csv/part-00000-417238c1-d876-4b4d-9174-82152ed9d5b6-c000.csv'
    ]

    candidate_names = [f"Candidate {i + 1}" for i in range(len(file_paths))]

    st.title("Linear Regression Voting Data Visualization")

    # Dropdown menus for each candidate
    graph_options = ['Scatter Plot', 'Box Plot', 'Heatmap']
    selections = {
        candidate: st.selectbox(f"Select Graph for {candidate}", graph_options, index=0)
        for candidate in candidate_names
    }

    for candidate, file_path in zip(candidate_names, file_paths):
        st.subheader(candidate)

        # Load and preprocess data
        voting_data = pd.read_csv(file_path)
        voting_data['Vote_Time'] = pd.to_datetime(
            voting_data['hour'].astype(str).str.zfill(2) + ':' +
            voting_data['minute'].astype(str).str.zfill(2) + ':00',
            format='%H:%M:%S'
        )
        voting_data = voting_data.sort_values(by='Vote_Time')
        voting_data = voting_data.rename(columns={'scaled_votes': 'Vote_Count'})
        voting_data['Hour'] = voting_data['Vote_Time'].dt.hour
        voting_data['Minute'] = voting_data['Vote_Time'].dt.minute

        # Generate the selected graph
        if selections[candidate] == 'Scatter Plot':
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.scatter(voting_data['Vote_Time'], voting_data['Vote_Count'], color='orange', label=candidate)
            ax.set_title('Scatter Plot')
            ax.set_xlabel('Time')
            ax.set_ylabel('Number of Votes')
            ax.legend()
            st.pyplot(fig)

        elif selections[candidate] == 'Box Plot':
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.boxplot([voting_data[voting_data['Hour'] == h]['Vote_Count'] for h in range(24)], labels=range(24))
            ax.set_title('Box Plot')
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Number of Votes')
            st.pyplot(fig)

        elif selections[candidate] == 'Heatmap':
            heatmap_data = voting_data.pivot_table(index='hour', columns='Minute', values='Vote_Count', aggfunc='sum')
            fig, ax = plt.subplots(figsize=(12, 8))
            sns.heatmap(heatmap_data, cmap='coolwarm', annot=False, ax=ax)
            ax.set_title('Heatmap')
            ax.set_xlabel('Minute of Hour')
            ax.set_ylabel('Hour of Day')
            st.pyplot(fig)

# Streamlit app
def main():
    st.title("Election Voting Trends: Historical vs Forecasted")
    st.write("Analysis based on historical trends")
    plot_linear_reg_data()
    st.write("Compare the historical voting data with the forecasted data for each candidate.")
    # Initialize session state variables
    initialize_session_state()

    # Kafka details
    brokers = ["localhost:9092"]  # Replace with your broker addresses
    candidate_ids = ["940da220-a451-4cc9-acc5-ce8b8697542a", "4714f68f-d08a-4f68-8a92-0be8f94687c0", "bd7098b3-4a02-4c6c-87a5-b5c96a5619a0"]
    topics = [f"data_{candidate_id}" for candidate_id in candidate_ids]

    # Consume data and update the UI in a loop
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=brokers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id="election_data_consumer"
    )
    st.write("Listening for real-time updates...")
    for message in consumer:
        data = message.value
        candidate_id = data.get("candidate_id", "Unknown")
        forecast = data.get("forecast", [])
        historical = data.get("historical_data", [])

        if candidate_id == "940da220-a451-4cc9-acc5-ce8b8697542a":
            st.session_state.historical_data['candidate_1'] = historical
            st.session_state.forecast_data['candidate_1'] = forecast
        elif candidate_id == "4714f68f-d08a-4f68-8a92-0be8f94687c0":
            st.session_state.historical_data['candidate_2'] = historical
            st.session_state.forecast_data['candidate_2'] = forecast
        elif candidate_id == "bd7098b3-4a02-4c6c-87a5-b5c96a5619a0":
            st.session_state.historical_data['candidate_3'] = historical
            st.session_state.forecast_data['candidate_3'] = forecast

        # Continuously update the plot
        plot_data()
        time.sleep(1)  # Prevent excessive reruns

if __name__ == "__main__":
    main()

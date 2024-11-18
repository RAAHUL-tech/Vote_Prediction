import pandas as pd
import random
import datetime
from kafka import KafkaProducer
import json
import time


# Load voters and candidates data
def load_data(voters_filename='voter_data.csv', candidates_filename='candidate_data.csv'):
    voters = pd.read_csv(voters_filename)
    candidates = pd.read_csv(candidates_filename)
    return voters, candidates


# Generate synthetic voting data for one day with sequential time
def generate_voting_data(voters, candidates, num_votes=9000):
    voting_records = []
    start_time = datetime.datetime.strptime('2024-01-01 08:00:00', "%Y-%m-%d %H:%M:%S")

    for i in range(num_votes):
        voter_id = voters.iloc[i % len(voters)]['Voter_id']
        candidate_id = random.choice(candidates['Candidate_id'])

        # Increment the time by 1 second for each vote
        vote_time = start_time + datetime.timedelta(seconds=i*3)

        # Generate a placeholder for voter_image
        voter_image = voters.iloc[i % len(voters)]['Picture']

        voting_records.append({
            'Voter_id': voter_id,
            'Candidate_id': candidate_id,
            'Vote_Time': vote_time.strftime("%Y-%m-%d %H:%M:%S"),
            'Voter_image': voter_image
        })

    return pd.DataFrame(voting_records)


# Kafka Producer setup
def send_to_kafka(voting_data, topic_name='voting_data'):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    count = 0
    # Send each record to Kafka with a delay of 10 seconds
    for _, record in voting_data.iterrows():
        if count>1:
            break
        producer.send(topic_name, record.to_dict())
        print(f"Sent to Kafka: {record.to_dict()}")
        time.sleep(10)  # Delay of 10 seconds between each message
        count+=1

    producer.flush()
    producer.close()


def main():
    # Load the voter and candidate data
    voters, candidates = load_data()

    # Generate synthetic voting data
    synthetic_voting_data = generate_voting_data(voters, candidates, num_votes=9000)
    # Send each row to the Kafka topic
    send_to_kafka(synthetic_voting_data)


if __name__ == "__main__":
    main()

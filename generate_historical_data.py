import pandas as pd
import random
import datetime


# Load voters and candidates data
def load_data(voters_filename='voter_data.csv', candidates_filename='candidate_data.csv'):
    voters = pd.read_csv(voters_filename)
    candidates = pd.read_csv(candidates_filename)
    return voters, candidates


# Generate synthetic voting data for one day
def generate_voting_data(voters, candidates, num_votes=9000):
    voting_records = []

    # Define the start and end times for the voting period
    start_time = datetime.datetime.strptime('1960-01-01 08:00:00', "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime('1960-01-01 17:00:00', "%Y-%m-%d %H:%M:%S")

    # Generate random voting times between 8 AM and 5 PM
    for i in range(num_votes):
        voter_id = voters.iloc[i]['Voter_id']
        candidate_id = random.choice(candidates['Candidate_id'])
        # Generate a random time within the specified voting period
        vote_time = start_time + (end_time - start_time) * random.random()

        voting_records.append({
            'Voter_id': voter_id,
            'Candidate_id': candidate_id,
            'Vote_Time': vote_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    return pd.DataFrame(voting_records)


# Save synthetic voting data to CSV
def save_voting_data(voting_data, filename='voting_data1960.csv'):
    voting_data.to_csv(filename, index=False)
    print(f"Saved synthetic voting data to {filename}")


def main():
    voters, candidates = load_data()
    synthetic_voting_data = generate_voting_data(voters, candidates, num_votes=9000)
    save_voting_data(synthetic_voting_data)


if __name__ == "__main__":
    main()

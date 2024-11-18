import requests
import csv
import os

PARTIES = ["Democrat", "Republic", "Other"]
def fetch_random_user():
    response = requests.get('https://randomuser.me/api/')
    if response.status_code == 200:
        return response.json()['results'][0]
    else:
        print("Error fetching data:", response.status_code)
        return None


def write_to_csv(voter_data, i, filename='candidate_data.csv'):
    # Check if the file already exists to write headers only once
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write headers if the file does not exist
        if not file_exists:
            writer.writerow([
                'Candidate_id', 'Gender', 'Title', 'First Name', 'Last Name',
                'party_affiliation', 'Picture'
            ])

        # Write voter data
        writer.writerow([
            voter_data['login']['uuid'],
            voter_data['gender'],
            voter_data['name']['title'],
            voter_data['name']['first'],
            voter_data['name']['last'],
            PARTIES[i % 3],
            voter_data['picture']['large']
        ])


def main():
    num_users_to_generate = 3
    for i in range(num_users_to_generate):
        voter = fetch_random_user()
        if voter:
            write_to_csv(voter, i)
            print(f"Inserted voter: {voter['name']['first']} {voter['name']['last']}")


if __name__ == "__main__":
    main()

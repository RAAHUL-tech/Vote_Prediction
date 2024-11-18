import requests
import csv
import os


def fetch_random_user():
    response = requests.get('https://randomuser.me/api/')
    if response.status_code == 200:
        return response.json()['results'][0]
    else:
        print("Error fetching data:", response.status_code)
        return None


def write_to_csv(voter_data, filename='voter_data.csv'):
    # Check if the file already exists to write headers only once
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write headers if the file does not exist
        if not file_exists:
            writer.writerow([
                'Voter_id', 'Gender', 'Title', 'First Name', 'Last Name',
                'Street Number', 'Street Name', 'City',
                'State', 'Country', 'Postcode',
                'Email', 'DOB',
                'Phone', 'Cell', 'SSN', 'Picture'
            ])

        # Write voter data
        writer.writerow([
            voter_data['login']['uuid'],
            voter_data['gender'],
            voter_data['name']['title'],
            voter_data['name']['first'],
            voter_data['name']['last'],
            voter_data['location']['street']['number'],
            voter_data['location']['street']['name'],
            voter_data['location']['city'],
            voter_data['location']['state'],
            voter_data['location']['country'],
            voter_data['location']['postcode'],
            voter_data['email'],
            voter_data['dob']['date'][:10],
            voter_data['phone'],
            voter_data['cell'],
            voter_data['id']['value'],
            voter_data['picture']['large']
        ])


def main():
    num_users_to_generate = 10000
    for _ in range(num_users_to_generate):
        voter = fetch_random_user()
        if voter:
            write_to_csv(voter)
            print(f"Inserted voter: {voter['name']['first']} {voter['name']['last']}")


if __name__ == "__main__":
    main()

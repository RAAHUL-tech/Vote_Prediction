import pandas as pd

#Plot the trend
# Load the uploaded CSV file
file_path = 'synthetic_voting_data.csv'
voting_data = pd.read_csv(file_path)

import matplotlib.pyplot as plt

# Convert Vote_Time to datetime format
voting_data['Vote_Time'] = pd.to_datetime(voting_data['Vote_Time'])

# Sort by Vote_Time for accurate plotting
voting_data = voting_data.sort_values(by='Vote_Time')

# Group by Candidate_id and Vote_Time, then count the number of votes at each time
vote_counts = voting_data.groupby(['Candidate_id', 'Vote_Time']).size().reset_index(name='Vote_Count')

# Generate plots for each candidate
unique_candidates = vote_counts['Candidate_id'].unique()

plt.figure(figsize=(12, 8))
for i, candidate_id in enumerate(unique_candidates, 1):
    # Filter data for the specific candidate
    candidate_data = vote_counts[vote_counts['Candidate_id'] == candidate_id]

    # Plotting the number of votes over time
    plt.subplot(3, 1, i)
    plt.plot(candidate_data['Vote_Time'], candidate_data['Vote_Count'], label=f'Candidate {i}')
    plt.title(f'Votes Over Time for Candidate {i}')
    plt.xlabel('Time')
    plt.ylabel('Number of Votes')
    plt.legend()

plt.tight_layout()
plt.show()

# Calculate cumulative votes for each candidate over time
vote_counts['Total_Votes'] = vote_counts.groupby('Candidate_id')['Vote_Count'].cumsum()

# Plotting cumulative votes over time for each candidate
plt.figure(figsize=(12, 8))
for i, candidate_id in enumerate(unique_candidates, 1):
    # Filter data for the specific candidate
    candidate_data = vote_counts[vote_counts['Candidate_id'] == candidate_id]

    # Plotting cumulative votes over time
    plt.subplot(3, 1, i)
    plt.plot(candidate_data['Vote_Time'], candidate_data['Total_Votes'], label=f'Candidate {i}')
    plt.title(f'Cumulative Votes Over Time for Candidate {i}')
    plt.xlabel('Time')
    plt.ylabel('Total Votes')
    plt.legend()

plt.tight_layout()
plt.show()


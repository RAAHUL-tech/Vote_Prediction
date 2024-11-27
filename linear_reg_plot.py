import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

file_path = 'debug_data/predictions_candidate_940da220-a451-4cc9-acc5-ce8b8697542a.csv/part-00000-c946dcf4-ae16-49cb-b924-ce857a8ffe33-c000.csv'
voting_data = pd.read_csv(file_path)
# Create a 'Vote_Time' column by combining 'hour' and 'minute', assuming seconds as '00'
voting_data['Vote_Time'] = pd.to_datetime(
    voting_data['hour'].astype(str).str.zfill(2) + ':' +
    voting_data['minute'].astype(str).str.zfill(2) + ':00',
    format='%H:%M:%S'
)

# Sort by 'Vote_Time' for accurate plotting
voting_data = voting_data.sort_values(by='Vote_Time')

# Rename 'scaled_votes' to 'Vote_Count' for consistency
voting_data = voting_data.rename(columns={'scaled_votes': 'Vote_Count'})


#  simple line plot
plt.figure(figsize=(10, 6))
plt.plot(voting_data['Vote_Time'], voting_data['Vote_Count'], label='Candidate 1')
plt.title('Simple Line Plot')
plt.xlabel('Time')
plt.ylabel('Number of Votes')
plt.legend()
plt.tight_layout()
plt.show()

# Bar chart
plt.figure(figsize=(10, 6))
plt.bar(voting_data['Vote_Time'], voting_data['Vote_Count'], color='skyblue', label='Candidate 1')
plt.title('Bar Chart')
plt.xlabel('Time')
plt.ylabel('Number of Votes')
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()

# Area chart
plt.figure(figsize=(10, 6))
plt.fill_between(voting_data['Vote_Time'], voting_data['Vote_Count'], color='skyblue', alpha=0.5, label='Candidate 1')
plt.title('Area Chart')
plt.xlabel('Time')
plt.ylabel('Number of Votes')
plt.legend()
plt.tight_layout()
plt.show()

# Scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(voting_data['Vote_Time'], voting_data['Vote_Count'], color='orange', label='Candidate 1')
plt.title('Scatter Plot')
plt.xlabel('Time')
plt.ylabel('Number of Votes')
plt.legend()
plt.tight_layout()
plt.show()

# Step chart
plt.figure(figsize=(10, 6))
plt.step(voting_data['Vote_Time'], voting_data['Vote_Count'], where='mid', label='Candidate 1', color='green')
plt.title('Step Chart')
plt.xlabel('Time')
plt.ylabel('Number of Votes')
plt.legend()
plt.tight_layout()
plt.show()

# Histogram
plt.figure(figsize=(10, 6))
plt.hist(voting_data['Vote_Time'].dt.hour, bins=24, color='purple', alpha=0.7, label='Vote Frequency')
plt.title('Histogram')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Votes')
plt.legend()
plt.tight_layout()
plt.show()

# Box plot
voting_data['Hour'] = voting_data['Vote_Time'].dt.hour
plt.figure(figsize=(10, 6))
plt.boxplot([voting_data[voting_data['Hour'] == h]['Vote_Count'] for h in range(24)], labels=range(24))
plt.title('Box Plot')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Votes')
plt.tight_layout()
plt.show()

# Heatmap
voting_data['Minute'] = voting_data['Vote_Time'].dt.minute
heatmap_data = voting_data.pivot_table(index='hour', columns='Minute', values='Vote_Count', aggfunc='sum')
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_data, cmap='coolwarm', annot=False)
plt.title('HeatMap')
plt.xlabel('Minute of Hour')
plt.ylabel('Hour of Day')
plt.tight_layout()
plt.show()



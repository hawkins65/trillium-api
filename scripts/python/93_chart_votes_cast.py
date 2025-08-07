import requests
import matplotlib.pyplot as plt

# URL to fetch the JSON data
url = "https://api.trillium.so/epoch_data/"

# Fetch the JSON data
response = requests.get(url)
data = response.json()

# Extract epoch, median votes cast, and total votes cast
epochs = []
median_votes_cast = []
total_votes_cast = []

for entry in data:
    epochs.append(entry['epoch'])
    median_votes_cast.append(entry['median_votes_cast'])
    total_votes_cast.append(entry['total_votes_cast'])

# Create two separate bar charts for median and total votes cast
def create_charts(epochs, median_votes_cast, total_votes_cast):
    # Create figure and axis objects
    fig, ax = plt.subplots(2, 1, figsize=(10, 10))

    # Median votes cast chart
    ax[0].bar(epochs, median_votes_cast, color='blue')
    ax[0].set_xlabel('Epochs')
    ax[0].set_ylabel('Median Votes Cast')
    ax[0].set_title('Median Votes Cast per Epoch')

    # Total votes cast chart
    ax[1].bar(epochs, total_votes_cast, color='green')
    ax[1].set_xlabel('Epochs')
    ax[1].set_ylabel('Total Votes Cast')
    ax[1].set_title('Total Votes Cast per Epoch')

    # Adjust layout
    plt.tight_layout()

    # Save charts as image files
    fig.savefig("votes_cast_charts.png")

    print("Charts saved as 'votes_cast_charts.png'")

# Generate and save the charts
create_charts(epochs, median_votes_cast, total_votes_cast)

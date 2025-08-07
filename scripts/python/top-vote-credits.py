import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from db_config import db_params  # Using the existing database configuration

# Function to construct the database URL using SQLAlchemy format
def get_db_url():
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# Function to retrieve and process the data
def analyze_validator_credits(top_n=40):
    # Create SQLAlchemy engine
    print("Creating SQLAlchemy engine...")
    engine = create_engine(get_db_url())

    # SQL query to retrieve relevant data for the last 10 epochs
    query = """
    SELECT 
        vs.identity_pubkey,
        vs.epoch,
        vs.epoch_credits
    FROM 
        validator_stats vs
    WHERE 
        vs.epoch IN (
            SELECT DISTINCT epoch
            FROM validator_stats
            ORDER BY epoch DESC
            LIMIT 10
        );
    """

    print("Executing SQL query...")
    # Load data into a pandas DataFrame
    df = pd.read_sql(query, engine)

    print("Processing data...")
    # Calculate the average epoch_credits for each validator over the last 10 epochs
    validator_avg_credits = df.groupby('identity_pubkey')['epoch_credits'].mean().reset_index()

    # Calculate overall average and median epoch_credits across all validators for the 10 epochs
    overall_avg = df['epoch_credits'].mean()
    overall_median = df['epoch_credits'].median()

    # Filter validators whose average epoch_credits are above the overall average and median
    above_norm_validators = validator_avg_credits[
        (validator_avg_credits['epoch_credits'] > overall_avg) &
        (validator_avg_credits['epoch_credits'] > overall_median)
    ].sort_values(by='epoch_credits', ascending=False).head(top_n).reset_index(drop=True)

    # Optionally, retrieve associated names from validator_info
    validator_list = tuple(above_norm_validators['identity_pubkey'].tolist())

    validator_info_query = f"""
    SELECT identity_pubkey, name
    FROM validator_info
    WHERE identity_pubkey IN {validator_list};
    """

    print("Fetching validator info...")
    validator_info_df = pd.read_sql(validator_info_query, engine)

    # Merge to get the names
    final_result = above_norm_validators.merge(validator_info_df, on='identity_pubkey', how='left')

    # Format numbers with commas and no decimals for display purposes
    final_result['epoch_credits'] = final_result['epoch_credits'].apply(lambda x: f"{int(x):,}")

    print("Saving results to text file...")
    # Save the result to a text file
    final_result.to_csv("validator_avg_credits_above_norm.txt", sep="\t", index=False)

    print("Plotting results...")
    # Plotting the results with adjusted y-axis
    plt.figure(figsize=(16, 10))  # Increase figure size to better fit x-axis labels
    final_result['epoch_credits_int'] = final_result['epoch_credits'].apply(lambda x: int(x.replace(',', '')))
    
    # Set y-axis limits to start just below the smallest value
    y_min = final_result['epoch_credits_int'].min() * 0.98
    y_max = final_result['epoch_credits_int'].max() * 1.02
    
    final_result.plot(kind='bar', x='identity_pubkey', y='epoch_credits_int', legend=False, color='green', alpha=0.7)
    plt.ylim(y_min, y_max)

    plt.title(f'Top {top_n} Validators by Average Epoch Credits')
    plt.xlabel('Identity Pubkey')
    plt.ylabel('Average Epoch Credits')
    plt.xticks(rotation=90, ha='right')  # Rotate x-axis labels for better readability

    plt.tight_layout()  # Adjust layout to accommodate labels and decorations
    print("Saving chart...")
    plt.savefig('validator_avg_credits_above_norm_chart.png')
    plt.show()

    print("Script completed successfully.")
    # Return the final result
    return final_result

# Example usage:
if __name__ == "__main__":
    print("Starting script...")
    top_n = 40  # Number of top validators to analyze
    result = analyze_validator_credits(top_n)
    print(result)

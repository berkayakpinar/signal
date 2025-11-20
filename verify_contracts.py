import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_API_KEY")

if not url or not key:
    print("Supabase URL and API Key must be set in the .env file.")
    exit()

supabase: Client = create_client(url, key)

def fetch_history_contracts():
    try:
        print("Fetching 10000 snapshots...")
        response = supabase.table("snapshots").select("contract, snapshot_minute").order("snapshot_minute", desc=True).limit(10000).execute()
        if response.data:
            df = pd.DataFrame(response.data)
            print(f"Fetched {len(df)} rows.")
            
            # Group by contract and find the latest snapshot time for each
            latest_snapshots = df.groupby('contract')['snapshot_minute'].max().reset_index()
            print(f"Found {len(latest_snapshots)} unique contracts.")
            
            # Sort by latest snapshot time descending
            latest_snapshots = latest_snapshots.sort_values('snapshot_minute', ascending=False)
            
            # Take top 50
            top_50_contracts = latest_snapshots['contract'].head(50).tolist()
            print("Top 5 contracts:", top_50_contracts[:5])
            return top_50_contracts
    except Exception as e:
        print(f"Error fetching history contracts: {e}")
    return []

if __name__ == "__main__":
    contracts = fetch_history_contracts()
    print(f"Returned {len(contracts)} contracts.")

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

def fetch_history_contracts_paginated():
    try:
        all_contracts = set()
        batch_size = 1000
        max_batches = 10  # Fetch up to 10,000 rows max
        
        print(f"Starting pagination fetch (Batch size: {batch_size}, Max batches: {max_batches})...")
        
        for i in range(max_batches):
            start = i * batch_size
            end = start + batch_size - 1
            print(f"Fetching batch {i+1}: range({start}, {end})")
            
            response = supabase.table("snapshots") \
                .select("contract, snapshot_minute") \
                .order("snapshot_minute", desc=True) \
                .range(start, end) \
                .execute()
            
            if not response.data:
                print("No more data returned.")
                break
            
            df = pd.DataFrame(response.data)
            unique_in_batch = df['contract'].unique()
            new_contracts = [c for c in unique_in_batch if c not in all_contracts]
            all_contracts.update(unique_in_batch)
            
            print(f"Batch {i+1}: Fetched {len(df)} rows. Found {len(new_contracts)} new unique contracts. Total unique: {len(all_contracts)}")
            
            if len(all_contracts) >= 50:
                print("Found 50+ unique contracts. Stopping.")
                break
        
        sorted_contracts = sorted(list(all_contracts), reverse=True)
        return sorted_contracts[:50]
        
    except Exception as e:
        print(f"Error fetching history contracts: {e}")
    return []

if __name__ == "__main__":
    contracts = fetch_history_contracts_paginated()
    print(f"\nFinal Result: Returned {len(contracts)} contracts.")
    print("Top 5:", contracts[:5])

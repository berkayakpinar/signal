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

def diagnose_contracts():
    try:
        print("Fetching 10,000 snapshots...")
        response = supabase.table("snapshots").select("contract, snapshot_minute").order("snapshot_minute", desc=True).limit(10000).execute()
        if response.data:
            df = pd.DataFrame(response.data)
            unique_contracts = df['contract'].unique()
            print(f"Fetched {len(df)} rows.")
            print(f"Found {len(unique_contracts)} unique contracts in the last 10,000 snapshots.")
            print("Unique contracts found:", unique_contracts)
            
            # Check if we can fetch more
            print("\nFetching 50,000 snapshots to compare...")
            response_large = supabase.table("snapshots").select("contract, snapshot_minute").order("snapshot_minute", desc=True).limit(50000).execute()
            if response_large.data:
                df_large = pd.DataFrame(response_large.data)
                unique_contracts_large = df_large['contract'].unique()
                print(f"Fetched {len(df_large)} rows.")
                print(f"Found {len(unique_contracts_large)} unique contracts in the last 50,000 snapshots.")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    diagnose_contracts()

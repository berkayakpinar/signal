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

def inspect_contract_names():
    try:
        print("Fetching unique contracts...")
        # Fetch a good number to see variety
        all_contracts = set()
        batch_size = 1000
        for i in range(5): # Fetch 5000 rows
            start = i * batch_size
            end = start + batch_size - 1
            response = supabase.table("snapshots").select("contract").order("snapshot_minute", desc=True).range(start, end).execute()
            if response.data:
                df = pd.DataFrame(response.data)
                all_contracts.update(df['contract'].unique())
            else:
                break
        
        print(f"Found {len(all_contracts)} unique contracts.")
        print("Sample Contracts:")
        for c in sorted(list(all_contracts)):
            print(c)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_contract_names()

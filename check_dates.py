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

def check_date_range():
    try:
        print("Fetching 50,000 snapshots to check date range...")
        # Fetch only snapshot_minute to be lightweight
        response = supabase.table("snapshots").select("snapshot_minute").order("snapshot_minute", desc=True).limit(50000).execute()
        if response.data:
            df = pd.DataFrame(response.data)
            df['snapshot_minute'] = pd.to_datetime(df['snapshot_minute'])
            
            min_date = df['snapshot_minute'].min()
            max_date = df['snapshot_minute'].max()
            unique_dates = df['snapshot_minute'].dt.date.unique()
            
            print(f"Fetched {len(df)} rows.")
            print(f"Date Range: {min_date} to {max_date}")
            print(f"Unique Dates Found ({len(unique_dates)}):")
            for d in sorted(unique_dates, reverse=True):
                print(d)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_date_range()

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

def verify_market_structure():
    try:
        print("Fetching market structure...")
        all_contracts = set()
        batch_size = 1000
        max_batches = 30
        
        for i in range(max_batches):
            start = i * batch_size
            end = start + batch_size - 1
            print(f"Fetching batch {i+1}...")
            
            response = supabase.table("snapshots") \
                .select("contract, snapshot_minute") \
                .order("snapshot_minute", desc=True) \
                .range(start, end) \
                .execute()
            
            if not response.data:
                break
            
            df = pd.DataFrame(response.data)
            unique_in_batch = df['contract'].unique()
            all_contracts.update(unique_in_batch)
            
            temp_dates = set()
            for c in all_contracts:
                if c.startswith("PH") and len(c) >= 8:
                    temp_dates.add(c[2:8])
            
            if len(temp_dates) >= 4:
                print("Found 4+ dates, stopping fetch.")
                break
        
        print(f"Total unique contracts found: {len(all_contracts)}")
        
        contract_dates = {}
        for contract in all_contracts:
            try:
                if contract.startswith("PH") and len(contract) >= 8:
                    date_part = contract[2:8]
                    full_date_str = f"20{date_part[:2]}-{date_part[2:4]}-{date_part[4:]}"
                    
                    if full_date_str not in contract_dates:
                        contract_dates[full_date_str] = []
                    contract_dates[full_date_str].append(contract)
            except:
                continue
        
        sorted_dates = sorted(contract_dates.keys(), reverse=True)
        top_3_dates = sorted_dates[:3]
        
        print(f"Top 3 Dates: {top_3_dates}")
        for d in top_3_dates:
            print(f"  {d}: {len(contract_dates[d])} contracts")
            print(f"  Sample: {contract_dates[d][:3]}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_market_structure()

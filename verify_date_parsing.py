def verify_date_parsing():
    contracts = [
        "PH25112123", "PH25112122", "PH25112023", "PH25112022", 
        "PH25111923", "INVALID_NAME", "PH251121"
    ]
    
    contract_dates = {}
    
    print("Testing Date Parsing Logic:")
    for contract in contracts:
        try:
            # Extract YYMMDD part (index 2 to 8)
            if len(contract) >= 8 and contract.startswith("PH"):
                date_part = contract[2:8]
                # Convert to readable date string (YYYY-MM-DD)
                full_date_str = f"20{date_part[:2]}-{date_part[2:4]}-{date_part[4:]}"
                
                if full_date_str not in contract_dates:
                    contract_dates[full_date_str] = []
                contract_dates[full_date_str].append(contract)
                print(f"Parsed {contract} -> {full_date_str}")
            else:
                raise ValueError("Invalid format")
        except:
            if "Others" not in contract_dates:
                contract_dates["Others"] = []
            contract_dates["Others"].append(contract)
            print(f"Parsed {contract} -> Others")

    print("\nGrouped Results:")
    for date, contract_list in sorted(contract_dates.items(), reverse=True):
        print(f"{date}: {contract_list}")

if __name__ == "__main__":
    verify_date_parsing()

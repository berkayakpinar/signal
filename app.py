import streamlit as st
import functions
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pandas as pd
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
import pytz
from streamlit_plotly_events import plotly_events

# Set page config as the first Streamlit command
st.set_page_config(layout="wide")

# Dynamic Auto-refresh logic
# Data arrives at minutes ending in 2 or 6 (02, 06, 12, 16, 22, 26...)
# We refresh 20 seconds after these minutes.
def get_next_refresh_interval():
    istanbul_tz = pytz.timezone('Europe/Istanbul')
    now = datetime.now(istanbul_tz)
    
    target_minutes = [2, 6, 12, 16, 22, 26, 32, 36, 42, 46, 52, 56]
    target_second = 20
    
    next_refresh = None
    
    for minute in target_minutes:
        try:
            candidate = now.replace(minute=minute, second=target_second, microsecond=0)
            if candidate > now:
                next_refresh = candidate
                break
        except ValueError:
            continue
            
    if next_refresh is None:
        # Move to next hour
        next_hour = now + pd.Timedelta(hours=1)
        # Ensure we don't carry over minutes/seconds that might cause issues, though replace handles it
        next_refresh = next_hour.replace(minute=target_minutes[0], second=target_second, microsecond=0)
        
    seconds_until = (next_refresh - now).total_seconds()
    return max(1000, int(seconds_until * 1000))

# Load environment variables
load_dotenv()

# Initialize Supabase
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_API_KEY")

if not url or not key:
    st.error("Supabase URL and API Key must be set in the .env file.")
    st.stop()

supabase: Client = create_client(url, key)

# Initialize Redis
try:
    r = functions.connect_to_redis()
except Exception as e:
    st.error(f"Failed to connect to Redis: {e}")
    st.stop()

# Display Last Updated Time
istanbul_tz = pytz.timezone('Europe/Istanbul')

# Get active contracts from Redis
try:
    active_contracts = functions.get_active_contracts(r)
except Exception as e:
    st.error(f"Error fetching active contracts: {e}")
    active_contracts = []
except Exception as e:
    st.error(f"Error fetching active contracts: {e}")
    active_contracts = []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_latest_signals(contracts):
    latest_data_list = []
    for contract in contracts:
        try:
            # Fetch ONLY the latest signal for each contract
            response = supabase.table("signals").select("contract, tradeSignal, timeSignal, snapshot_minute").eq("contract", contract).order("snapshot_minute", desc=True).limit(1).execute()
            if response.data:
                latest_data_list.extend(response.data)
        except Exception as e:
            print(f"Error fetching latest data for {contract}: {e}")
    
    if not latest_data_list:
        return pd.DataFrame()
    
    df = pd.DataFrame(latest_data_list)
    
    # Process snapshot_minute
    if 'snapshot_minute' in df.columns:
        df['snapshot_minute'] = pd.to_datetime(df['snapshot_minute'])
        try:
            df['snapshot_minute'] = df['snapshot_minute'].dt.tz_convert('Europe/Istanbul')
        except TypeError:
            df['snapshot_minute'] = df['snapshot_minute'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
        
        df = df.sort_values(by='snapshot_minute', ascending=False)
        
    return df

@st.cache_data(ttl=60, show_spinner=False)
def fetch_contract_history(contract):
    try:
        # Fetch recent history (e.g., 1000 rows) for a SPECIFIC contract
        response = supabase.table("signals").select("contract, tradeSignal, timeSignal, snapshot_minute").eq("contract", contract).order("snapshot_minute", desc=True).limit(1000).execute()
        if response.data:
            df = pd.DataFrame(response.data)
            # Process snapshot_minute
            if 'snapshot_minute' in df.columns:
                df['snapshot_minute'] = pd.to_datetime(df['snapshot_minute'])
                try:
                    df['snapshot_minute'] = df['snapshot_minute'].dt.tz_convert('Europe/Istanbul')
                except TypeError:
                    df['snapshot_minute'] = df['snapshot_minute'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
            return df
    except Exception as e:
        print(f"Error fetching history for {contract}: {e}")
    
    return pd.DataFrame()

@st.cache_data(ttl=60, show_spinner=False)
def fetch_recent_trade_signals(limit=1000):
    try:
        # Fetch recent OPEN_LONG and OPEN_SHORT signals
        response = supabase.table("signals").select("contract, tradeSignal, timeSignal, snapshot_minute").in_("tradeSignal", ["OPEN_LONG", "OPEN_SHORT"]).order("snapshot_minute", desc=True).limit(limit).execute()
        if response.data:
            df = pd.DataFrame(response.data)
            # Process snapshot_minute
            if 'snapshot_minute' in df.columns:
                df['snapshot_minute'] = pd.to_datetime(df['snapshot_minute'])
                try:
                    df['snapshot_minute'] = df['snapshot_minute'].dt.tz_convert('Europe/Istanbul')
                except TypeError:
                    df['snapshot_minute'] = df['snapshot_minute'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
            return df
    except Exception as e:
        print(f"Error fetching recent trade signals: {e}")
    
    return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_structure():
    try:
        all_contracts = set()
        batch_size = 1000
        max_batches = 30  # Fetch up to 30,000 rows to ensure we cover 3 days
        
        # We need to fetch enough data to find the last 3 days.
        # Since we order by time, we just keep fetching until we have 3 distinct dates.
        
        for i in range(max_batches):
            start = i * batch_size
            end = start + batch_size - 1
            
            response = supabase.table("signals") \
                .select("contract, snapshot_minute") \
                .order("snapshot_minute", desc=True) \
                .range(start, end) \
                .execute()
            
            if not response.data:
                break
            
            df = pd.DataFrame(response.data)
            unique_in_batch = df['contract'].unique()
            all_contracts.update(unique_in_batch)
            
            # Check if we have enough dates
            # Quick check: extract dates from what we have so far
            temp_dates = set()
            for c in all_contracts:
                if c.startswith("PH") and len(c) >= 8:
                    temp_dates.add(c[2:8])
            
            # If we have found contracts for more than 3 days, we can probably stop
            if len(temp_dates) >= 4: 
                break
        
        # Process all found contracts
        contract_dates = {}
        for contract in all_contracts:
            try:
                # Extract YYMMDD part (index 2 to 8)
                if contract.startswith("PH") and len(contract) >= 8:
                    date_part = contract[2:8]
                    # Convert to readable date string (YYYY-MM-DD)
                    full_date_str = f"20{date_part[:2]}-{date_part[2:4]}-{date_part[4:]}"
                    
                    if full_date_str not in contract_dates:
                        contract_dates[full_date_str] = []
                    contract_dates[full_date_str].append(contract)
            except:
                continue
        
        # Filter: Keep only the top 3 most recent dates
        sorted_dates = sorted(contract_dates.keys(), reverse=True)
        top_3_dates = sorted_dates[:3]
        
        final_structure = {d: contract_dates[d] for d in top_3_dates}
        return final_structure
        
    except Exception as e:
        print(f"Error fetching market structure: {e}")
    return {}

# Status Checks
redis_status = "Connected"
redis_color = "#28a745" # Green
try:
    r.ping()
except Exception:
    redis_status = "Disconnected"
    redis_color = "#dc3545" # Red

supabase_status = "Connected"
supabase_color = "#28a745" # Green
if not supabase:
    supabase_status = "Disconnected"
    supabase_color = "#dc3545" # Red

# Top Layout: Title left, Status right
top_col1, top_col2 = st.columns([3, 1])

with top_col1:
    st.title("Signal Reader")
    st.write(f"Last Updated: {datetime.now(istanbul_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    auto_refresh = st.toggle("Auto Refresh", value=True)

with top_col2:
    st.markdown(
        f"""
        <div style="display: flex; justify-content: flex-end; gap: 10px; padding-top: 20px;">
            <span style="background-color: {redis_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold;">Redis: {redis_status}</span>
            <span style="background-color: {supabase_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold;">Supabase: {supabase_status}</span>
        </div>
        """,
        unsafe_allow_html=True
    )

if auto_refresh:
    refresh_interval = get_next_refresh_interval()
    st_autorefresh(interval=refresh_interval, key="dynamic_refresh")

# Define styling function
def color_trade_signal(val):
    color = ''
    if val == 'OPEN_LONG':
        color = 'background-color: #CD5C5C; color: white' # IndianRed
    elif val == 'OPEN_SHORT':
        color = 'background-color: #3CB371; color: white' # MediumSeaGreen
    return color

# Define formatting function for Time Signal
def format_time_signal(val):
    if pd.isna(val):
        return "N/A"
    try:
        # Format as 0.30 (2 decimal places)
        return f"{float(val):.2f}"
    except ValueError:
        return val

# Create Tabs
# Create Tabs
tab_dashboard, tab_timeline, tab_snapshots = st.tabs(["Dashboard", "Timeline", "Snapshots"])

with tab_dashboard:
    if active_contracts:
        # Selection mechanism and buttons in one row
        active_contracts.sort()
    
        # Create columns for layout: Selector (large), Refresh (wider)
        col_sel, col_refresh = st.columns([3, 1])
    
        with col_sel:
            selected_contract = st.selectbox("Select Contract for Details", active_contracts)
        
        with col_refresh:
            st.write("") # Spacer
            st.write("")
            if st.button("Refresh Data", use_container_width=True):
                fetch_latest_signals.clear()
                fetch_contract_history.clear()
                st.rerun()



        # Create two columns with 2:3 ratio
        col1, col2 = st.columns([2, 3])

        with col1:
            # Fetch ONLY latest signals for the left column
            with st.spinner('Fetching summary...'):
                latest_signals = fetch_latest_signals(active_contracts)

            if not latest_signals.empty:
                # Sort by contract ASCENDING (Old to New)
                latest_signals = latest_signals.sort_values(by='contract', ascending=True)

                # Check for alerts
                alert_signals = latest_signals[latest_signals['tradeSignal'].isin(['OPEN_SHORT', 'OPEN_LONG'])]
                if not alert_signals.empty:
                    for index, row in alert_signals.iterrows():
                        st.toast(f"⚠️ {row['contract']}: {row['tradeSignal']}!", icon="🚨")
                
                    # Play sound (beep)
                    html_string = """
                    <audio autoplay>
                    <source src="https://www.soundjay.com/buttons/sounds/beep-07.mp3" type="audio/mpeg">
                    </audio>
                    """
                    st.markdown(html_string, unsafe_allow_html=True)

                # Get the latest snapshot minute
                latest_snapshot_str = "Unknown"
                if 'snapshot_minute' in latest_signals.columns:
                    latest_snapshot_val = latest_signals['snapshot_minute'].max()
                    latest_snapshot_str = latest_snapshot_val.strftime('%d.%m %H:%M')
            
                st.markdown(f"<h3 style='text-align: center;'>Latest ({latest_snapshot_str})</h3>", unsafe_allow_html=True)
            
                # View Mode Toggle
                view_mode = st.radio("View Mode", ["List", "Heatmap"], horizontal=True, label_visibility="collapsed")
            
                if view_mode == "Heatmap":
                    import plotly.express as px
                
                    # Create a dummy column for equal sizing
                    latest_signals['size'] = 1
                
                    # Create Treemap
                    fig_map = px.treemap(
                        latest_signals,
                        path=['contract'],
                        values='size',
                        color='timeSignal',
                        color_continuous_scale=[(0, "green"), (0.5, "gray"), (1, "red")],
                        range_color=[-1, 1],
                        hover_data={'contract': True, 'timeSignal': ':.2f', 'tradeSignal': True, 'size': False},
                        title=""
                    )
                
                    fig_map.update_layout(
                        margin=dict(t=0, l=0, r=0, b=0),
                        height=600,
                        template="plotly_dark"
                    )
                
                    # Update text info
                    fig_map.data[0].textinfo = "label+text+value"
                    fig_map.data[0].texttemplate = "%{label}<br>%{customdata[1]:.2f}"
                
                    st.plotly_chart(fig_map, use_container_width=True)
                
                else:
                    # Select specific columns
                    cols_to_show_left = ['contract', 'timeSignal', 'tradeSignal']
                    cols_to_show_left = [c for c in cols_to_show_left if c in latest_signals.columns]
                
                    # Apply styling and formatting
                    styled_latest = latest_signals[cols_to_show_left].style.map(color_trade_signal, subset=['tradeSignal'])
                    if 'timeSignal' in latest_signals.columns:
                        styled_latest = styled_latest.format({'timeSignal': format_time_signal})
                
                    # Center align text and headers
                    styled_latest = styled_latest.set_properties(**{'text-align': 'center'})
                    styled_latest = styled_latest.set_table_styles([
                        {'selector': 'th', 'props': [('text-align', 'center')]}
                    ])
                
                    # Set height to align with right column (approximate: Header+KPIs+Chart+Table)
                    st.dataframe(styled_latest, use_container_width=True, height=1050)
            else:
                st.info("No signals found for any active contracts.")

        with col2:
            # Custom CSS to center metrics and other elements in the right column
            st.markdown("""
            <style>
            [data-testid="stMetric"] {
                justify-content: center;
                text-align: center;
            }
            [data-testid="stMetricLabel"] {
                justify-content: center;
            }
            [data-testid="stMetricValue"] {
                justify-content: center;
            }
            </style>
            """, unsafe_allow_html=True)

            st.markdown(f"<h3 style='text-align: center;'>{selected_contract}</h3>", unsafe_allow_html=True)
        
            if selected_contract:
                # Fetch history for the SELECTED contract on demand
                with st.spinner(f'Fetching details for {selected_contract}...'):
                    contract_data = fetch_contract_history(selected_contract)
            
                if not contract_data.empty:
                    # --- KPIs ---
                    kpi1, kpi2, kpi3 = st.columns(3)
                
                    latest_row = contract_data.iloc[0]
                
                    with kpi1:
                        st.metric("Current Signal", latest_row.get('tradeSignal', 'N/A'))
                    with kpi2:
                        # Format KPI as well
                        ts_val = latest_row.get('timeSignal', 'N/A')
                        st.metric("Time Signal", format_time_signal(ts_val) if ts_val != 'N/A' else 'N/A')
                    with kpi3:
                        snapshot_val = latest_row.get('snapshot_minute', 'N/A')
                        if isinstance(snapshot_val, pd.Timestamp):
                            snapshot_str = snapshot_val.strftime('%H:%M')
                        else:
                            snapshot_str = str(snapshot_val)
                        st.metric("Last Update", snapshot_str)
                
                    # --- Trend Chart (Plotly) ---
                    st.markdown("<div style='text-align: center; color: gray; font-size: 0.8em;'>Time Signal Trend</div>", unsafe_allow_html=True)
                    if 'timeSignal' in contract_data.columns:
                        import plotly.graph_objects as go
                    
                        # Ensure timeSignal is numeric for charting
                        chart_data = contract_data[['snapshot_minute', 'timeSignal']].copy()
                        chart_data['timeSignal'] = pd.to_numeric(chart_data['timeSignal'], errors='coerce')
                    
                        # Create Plotly Figure
                        fig = go.Figure()
                    
                        # Add Line Trace
                        fig.add_trace(go.Scatter(
                            x=chart_data['snapshot_minute'],
                            y=chart_data['timeSignal'],
                            mode='lines',
                            name='Time Signal',
                            line=dict(color='#1f77b4', width=2)
                        ))
                    
                        # Add Threshold Lines
                        # +0.30 (Red) - OPEN_LONG limit
                        fig.add_hline(y=0.30, line_dash="dash", line_color="red", annotation_text="OPEN_LONG (0.30)", annotation_position="top right")
                    
                        # -0.30 (Green) - OPEN_SHORT limit
                        fig.add_hline(y=-0.30, line_dash="dash", line_color="green", annotation_text="OPEN_SHORT (-0.30)", annotation_position="bottom right")
                    
                        # Update Layout
                        fig.update_layout(
                            template="plotly_dark",
                            xaxis_title="Time",
                            yaxis_title="Time Signal",
                            yaxis=dict(
                                range=[-0.7, 0.7],
                                tickmode='array',
                                tickvals=[-0.6, -0.3, 0, 0.3, 0.6],
                                zeroline=True,
                                zerolinecolor='gray'
                            ),
                            margin=dict(l=20, r=20, t=30, b=20),
                            height=400
                        )
                    
                        st.plotly_chart(fig, use_container_width=True)

                    # --- Detailed Table ---
                    # Select specific columns
                    cols_to_show_right = ['snapshot_minute', 'timeSignal', 'tradeSignal']
                    cols_to_show_right = [c for c in cols_to_show_right if c in contract_data.columns]
                
                    # Apply styling and formatting
                    styled_details = contract_data[cols_to_show_right].style.map(color_trade_signal, subset=['tradeSignal'])
                    if 'timeSignal' in contract_data.columns:
                        styled_details = styled_details.format({'timeSignal': format_time_signal})
                
                    # Center align text and headers
                    styled_details = styled_details.set_properties(**{'text-align': 'center'})
                    styled_details = styled_details.set_table_styles([
                        {'selector': 'th', 'props': [('text-align', 'center')]}
                    ])
                
                    st.dataframe(styled_details, use_container_width=True, height=500)
                else:
                    st.info(f"No data found for {selected_contract}")
            else:
                st.info("Select a contract to view details.")
    else:
        st.warning("No active contracts found.")

with tab_timeline:
    st.subheader("Signal Timeline (OPEN_LONG / OPEN_SHORT)")
    
    with st.spinner("Fetching timeline data..."):
        timeline_df = fetch_recent_trade_signals(limit=2000)
        
    if not timeline_df.empty:
        import plotly.express as px
        
        # Define colors to match existing scheme (Long=Red, Short=Green)
        color_map = {
            "OPEN_LONG": "#dc3545",  # Red
            "OPEN_SHORT": "#28a745"  # Green
        }
        
        # Ensure timeSignal is numeric
        timeline_df['timeSignal'] = pd.to_numeric(timeline_df['timeSignal'], errors='coerce')

        # Calculate Excess Strength
        def calculate_excess(val):
            if pd.isna(val):
                return 0
            if val > 0:
                return val - 0.3
            elif val < 0:
                return val + 0.3
            return 0

        timeline_df['excess_strength'] = timeline_df['timeSignal'].apply(calculate_excess)

        fig_timeline = px.scatter(
            timeline_df,
            x="snapshot_minute",
            y="excess_strength",
            color="tradeSignal",
            color_discrete_map=color_map,
            hover_data=["contract", "tradeSignal", "timeSignal", "excess_strength"],
            title="Recent Trade Signals"
        )
        
        fig_timeline.update_traces(marker=dict(size=12, line=dict(width=1, color='DarkSlateGrey'), opacity=0.7))
        
        # Add zero line to represent the threshold
        fig_timeline.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Threshold", annotation_position="bottom right")

        fig_timeline.update_layout(
            template="plotly_dark",
            height=600,
            xaxis_title="Time",
            yaxis_title="Excess Strength (Signal - Threshold)",
            legend_title="Signal"
        )
        
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        st.markdown("### Detailed Signal List")
        # Show detailed table
        display_cols = ["snapshot_minute", "contract", "tradeSignal", "timeSignal", "excess_strength"]
        st.dataframe(
            timeline_df[display_cols].style.format({
                "timeSignal": "{:.2f}", 
                "excess_strength": "{:.2f}",
                "snapshot_minute": lambda t: t.strftime("%Y-%m-%d %H:%M")
            }).map(color_trade_signal, subset=['tradeSignal']),
            use_container_width=True,
            height=400
        )
    else:
        st.info("No recent trade signals found.")



with tab_snapshots:
    @st.fragment
    def render_snapshots_tab():
        # st.subheader("Market Snapshots") removed
        
        market_structure = fetch_market_structure()
        
        selected_snap_contract = None
        selected_snap_minute = None
        
        available_minutes = []

        if market_structure:
            # Layout for selectors
            col_date, col_contract = st.columns([1, 1])
            
            with col_date:
                sorted_dates = sorted(market_structure.keys(), reverse=True)
                selected_date = st.selectbox("Select Date", sorted_dates, key="snap_date")
            
            with col_contract:
                if selected_date:
                    available_contracts = sorted(market_structure[selected_date], reverse=False)
                    selected_snap_contract = st.selectbox("Select Contract", available_contracts, key="snap_contract")
            
            if selected_snap_contract:
                # Fetch available minutes for this contract
                @st.cache_data(ttl=60, show_spinner=False)
                def fetch_snapshot_minutes(contract):
                    try:
                        response = supabase.table("snapshots").select("snapshot_minute").eq("contract", contract).order("snapshot_minute", desc=True).execute()
                        if response.data:
                            minutes = [row['snapshot_minute'] for row in response.data]
                            return minutes
                    except Exception as e:
                        print(f"Error fetching snapshot minutes: {e}")
                    return []

                available_minutes = fetch_snapshot_minutes(selected_snap_contract)

                # Create mapping from HH:MM to full timestamp
                minute_map = {}
                for m in available_minutes:
                    try:
                        dt = pd.to_datetime(m)
                        try:
                            dt = dt.tz_convert('Europe/Istanbul')
                        except TypeError:
                            dt = dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                        
                        fmt = dt.strftime('%d %H:%M')
                        minute_map[fmt] = m
                    except:
                        continue

                # Initialize or Update Session State for Minute
                # If contract changed, reset minute to latest
                if 'snap_query_contract' not in st.session_state or st.session_state.snap_query_contract != selected_snap_contract:
                    st.session_state.snap_query_contract = selected_snap_contract
                    if available_minutes:
                        st.session_state.snap_query_minute = available_minutes[0]
                    else:
                        st.session_state.snap_query_minute = None
                
                selected_snap_minute = st.session_state.snap_query_minute
        
        # Check if we have a valid query
        if selected_snap_contract and selected_snap_minute:
            

            with st.spinner("Fetching snapshot data..."):
                # Fetch Data
                try:
                    # Fetch Specific Snapshot Data (Board, Depth, Remaining Time)
                    response = supabase.table("snapshots").select("board, depth, remaining_time_sec").eq("contract", selected_snap_contract).eq("snapshot_minute", selected_snap_minute).single().execute()
                    
                    # Fetch Latest Snapshot Data (Trades) - Get up to 10 snapshots to find one with trades
                    latest_response = supabase.table("snapshots").select("trades").eq("contract", selected_snap_contract).order("snapshot_minute", desc=True).limit(10).execute()
                    
                    if response.data:
                        data = response.data
                        board = data.get('board', {})
                        depth = data.get('depth', {})
                        remaining_time_sec = data.get('remaining_time_sec', 0)
                        
                        # Use trades from the latest snapshot that has non-empty trades
                        trades = []
                        if latest_response.data and len(latest_response.data) > 0:
                            # Loop through snapshots to find the first one with trades
                            for snapshot in latest_response.data:
                                trades_data = snapshot.get('trades')
                                if trades_data is not None and isinstance(trades_data, list) and len(trades_data) > 0:
                                    trades = trades_data
                                    break
                        
                        # --- Statistics Section ---
                        st.markdown(f"<h3 style='text-align: center;'>{selected_snap_contract}</h3>", unsafe_allow_html=True)
                        st.markdown("---")
                        
                        # 1. Calculate Metrics
                        
                        # Snapshot Time
                        snapshot_time_str = "N/A"
                        try:
                            st_dt = pd.to_datetime(selected_snap_minute)
                            try:
                                st_dt = st_dt.tz_convert('Europe/Istanbul')
                            except TypeError:
                                st_dt = st_dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                            snapshot_time_str = st_dt.strftime('%d %H:%M')
                        except:
                            pass
                            
                        # Remaining Time (HH:MM)
                        remaining_time_str = "N/A"
                        if remaining_time_sec is not None:
                            try:
                                val = float(remaining_time_sec)
                                if val < 60:
                                    remaining_time_str = "00:00"
                                else:
                                    hours = int(val // 3600)
                                    minutes = int((val % 3600) // 60)
                                    remaining_time_str = f"{hours:02d}:{minutes:02d}"
                            except:
                                pass
                            
                        # PTF (MCP)
                        ptf = board.get('mcp', 0)
                        
                        # AOF (Average Price)
                        aof = board.get('averagePrice', 0)
                        
                        # Imbalance
                        imbalance_str = "N/A"
                        bids = depth.get('bid', [])
                        asks = depth.get('ask', [])
                        
                        total_bid_vol = 0
                        total_ask_vol = 0
                        
                        if bids:
                            df_bids_temp = pd.DataFrame(bids)
                            if not df_bids_temp.empty:
                                df_bids_temp = df_bids_temp.iloc[:, :2]
                                df_bids_temp.columns = ['price', 'volume']
                                total_bid_vol = pd.to_numeric(df_bids_temp['volume'], errors='coerce').sum()
                        
                        if asks:
                            df_asks_temp = pd.DataFrame(asks)
                            if not df_asks_temp.empty:
                                df_asks_temp = df_asks_temp.iloc[:, :2]
                                df_asks_temp.columns = ['price', 'volume']
                                total_ask_vol = pd.to_numeric(df_asks_temp['volume'], errors='coerce').sum()
                                
                        if (total_bid_vol + total_ask_vol) > 0:
                            imbalance = (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol)
                            imbalance_str = f"{imbalance:.2%}"
                            
                        # Price Change (Weighted Avg of last 50MWh - PTF)
                        price_change_str = "N/A"
                        if trades and ptf:
                            df_trades_calc = pd.DataFrame(trades)
                            df_trades_calc = df_trades_calc.rename(columns={'p': 'price', 'q': 'volume', 't': 'timestamp'})
                            df_trades_calc['price'] = pd.to_numeric(df_trades_calc['price'], errors='coerce')
                            df_trades_calc['volume'] = pd.to_numeric(df_trades_calc['volume'], errors='coerce')
                            df_trades_calc['timestamp'] = pd.to_numeric(df_trades_calc['timestamp'], errors='coerce')
                            
                            # Filter trades to include only those before or at the snapshot time
                            try:
                                snap_dt = pd.to_datetime(selected_snap_minute)
                                if snap_dt.tzinfo is None:
                                    snap_dt = snap_dt.tz_localize('UTC')
                                else:
                                    snap_dt = snap_dt.tz_convert('UTC')
                                
                                # Create temp datetime column for filtering (UTC)
                                df_trades_calc['dt_utc'] = pd.to_datetime(df_trades_calc['timestamp'], unit='s', utc=True)
                                
                                # Filter
                                df_trades_calc = df_trades_calc[df_trades_calc['dt_utc'] <= snap_dt]
                            except Exception as e:
                                print(f"Error filtering trades for price change: {e}")
                            
                            # Sort by timestamp descending (latest first)
                            df_trades_calc = df_trades_calc.sort_values('timestamp', ascending=False)
                            
                            cumulative_vol = 0
                            weighted_sum = 0
                            
                            for index, row in df_trades_calc.iterrows():
                                vol = row['volume']
                                price = row['price']
                                
                                if pd.isna(vol) or pd.isna(price):
                                    continue
                                    
                                needed = 50 - cumulative_vol
                                if needed <= 0:
                                    break
                                    
                                take = min(vol, needed)
                                weighted_sum += take * price
                                cumulative_vol += take
                                
                            if cumulative_vol > 0:
                                weighted_avg = weighted_sum / cumulative_vol
                                price_change = weighted_avg - ptf
                                price_change_str = f"{price_change:.2f}"
                        
                        # Display Metrics
                        m1, m2, m3, m4, m5, m6 = st.columns(6)
                        m1.metric("Snapshot Time", snapshot_time_str, help="Verinin kaydedildiği an.")
                        m2.metric("Remaining Time", remaining_time_str, help="Kontrat kapanışına kalan süre (HH:MM)")
                        m3.metric("PTF", f"{ptf:.2f}" if isinstance(ptf, (int, float)) else ptf, help="Piyasa Takas Fiyatı")
                        m4.metric("AOF", f"{aof:.2f}" if isinstance(aof, (int, float)) else aof, help="Seçilen ana kadar gerçekleşen eşleşmelerin ağırlıklı ortalama fiyatı")
                        m5.metric("Hacim Dengesi", imbalance_str, help="Alıcı ve Satıcı hacmi arasındaki dengesizlik (Sıfır dengede. Pozitifse Alıcı baskın, Negatifse Satıcı baskın)")
                        m6.metric("Fiyat Değişimi", price_change_str, help="Son 50MWh hacimli eşleşmenin ağırlıklı ortalama fiyatının PTF'den farkı")
                        
                        st.markdown("---")
                        
                        # --- Layout ---
                        col_left, col_right = st.columns([2, 1])
                        
                        # --- Left Column: Trades ---
                        with col_left:
                            st.markdown(f"### Trades ({selected_snap_contract})")
                            
                            if trades:
                                df_trades = pd.DataFrame(trades)
                                # Rename columns: p->price, q->volume, t->timestamp
                                df_trades = df_trades.rename(columns={'p': 'price', 'q': 'volume', 't': 'timestamp'})
                                
                                # Process timestamp
                                df_trades['timestamp'] = pd.to_numeric(df_trades['timestamp'], errors='coerce')
                                df_trades['price'] = pd.to_numeric(df_trades['price'], errors='coerce')
                                df_trades['volume'] = pd.to_numeric(df_trades['volume'], errors='coerce')
                                
                                df_trades['timestamp'] = pd.to_datetime(df_trades['timestamp'], unit='s')
                                try:
                                    df_trades['timestamp'] = df_trades['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                                except TypeError:
                                    df_trades['timestamp'] = df_trades['timestamp'].dt.tz_convert('Europe/Istanbul')
                                
                                # Sort Ascending (Oldest to Newest) for Graph
                                df_trades = df_trades.sort_values('timestamp', ascending=True)
                                df_trades['formatted_time'] = df_trades['timestamp'].dt.strftime('%d:%m %H:%M')
                                
                                # Fetch Signals for Overlay
                                @st.cache_data(ttl=60, show_spinner=False)
                                def fetch_snap_signals(contract):
                                    try:
                                        response = supabase.table("signals").select("contract, tradeSignal, timeSignal, snapshot_minute").eq("contract", contract).order("snapshot_minute", desc=False).execute()
                                        if response.data:
                                            df = pd.DataFrame(response.data)
                                            if 'snapshot_minute' in df.columns:
                                                df['snapshot_minute'] = pd.to_datetime(df['snapshot_minute'])
                                                try:
                                                    df['snapshot_minute'] = df['snapshot_minute'].dt.tz_convert('Europe/Istanbul')
                                                except TypeError:
                                                    df['snapshot_minute'] = df['snapshot_minute'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                                            return df
                                    except Exception as e:
                                        print(f"Error fetching signal history for {contract}: {e}")
                                    return pd.DataFrame()

                                signal_df = fetch_snap_signals(selected_snap_contract)

                                # Add 'snapshot' column (Next Snapshot Minute) BEFORE Charting
                                if available_minutes:
                                    try:
                                        df_snaps = pd.DataFrame({'snap_min': available_minutes})
                                        df_snaps['snap_min'] = pd.to_datetime(df_snaps['snap_min'])
                                        try:
                                            df_snaps['snap_min'] = df_snaps['snap_min'].dt.tz_convert('Europe/Istanbul')
                                        except TypeError:
                                            df_snaps['snap_min'] = df_snaps['snap_min'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                                        
                                        df_snaps = df_snaps.sort_values('snap_min')
                                        
                                        # Merge to find next snapshot (direction='forward')
                                        df_trades = pd.merge_asof(
                                            df_trades,
                                            df_snaps,
                                            left_on='timestamp',
                                            right_on='snap_min',
                                            direction='forward'
                                        )
                                        
                                        if 'snap_min' in df_trades.columns:
                                            df_trades['snapshot'] = df_trades['snap_min'].dt.strftime('%d %H:%M')
                                        else:
                                            df_trades['snapshot'] = "-"
                                    except Exception as e:
                                        print(f"Error adding snapshot column: {e}")
                                        df_trades['snapshot'] = "-"
                                else:
                                    df_trades['snapshot'] = "-"

                                # Plotly Combo Chart
                                import plotly.graph_objects as go
                                from plotly.subplots import make_subplots
                                
                                fig_trades = make_subplots(specs=[[{"secondary_y": True}]])
                                
                                # Volume Bar
                                fig_trades.add_trace(go.Bar(
                                    x=df_trades['formatted_time'],
                                    y=df_trades['volume'],
                                    name='Volume',
                                    marker_color='rgba(128, 128, 128, 0.5)',
                                    opacity=0.6
                                ), secondary_y=True)
                                
                                # Price Line
                                fig_trades.add_trace(go.Scatter(
                                    x=df_trades['formatted_time'],
                                    y=df_trades['price'],
                                    mode='lines',
                                    name='Price',
                                    line=dict(color='#00BFFF', width=2),
                                    fill='tozeroy',
                                    fillcolor='rgba(0, 191, 255, 0.1)',
                                    customdata=df_trades['snapshot'],
                                    hovertemplate='<b>Price</b><br>Time: %{x}<br>Price: %{y}<br>Snapshot: %{customdata}<extra></extra>'
                                ), secondary_y=False)

                                # Add Signal Overlay
                                if not signal_df.empty:
                                    # Filter for actual trades
                                    sig_trades = signal_df[signal_df['tradeSignal'].isin(['OPEN_LONG', 'OPEN_SHORT'])]
                                    
                                    if not sig_trades.empty:
                                        sig_trades = sig_trades.sort_values('snapshot_minute')
                                        
                                        # Use merge_asof to find the nearest price for each trade
                                        merged_trades = pd.merge_asof(
                                            sig_trades, 
                                            df_trades, 
                                            left_on='snapshot_minute',
                                            right_on='timestamp',
                                            direction='nearest',
                                            tolerance=pd.Timedelta('5min')
                                        )
                                        
                                        # Format timestamp for matched trades
                                        merged_trades['formatted_time'] = merged_trades['timestamp'].dt.strftime('%d:%m %H:%M')
                                        
                                        # Separate Long and Short
                                        longs = merged_trades[merged_trades['tradeSignal'] == 'OPEN_LONG']
                                        shorts = merged_trades[merged_trades['tradeSignal'] == 'OPEN_SHORT']
                                        
                                        if not longs.empty:
                                            fig_trades.add_trace(go.Scatter(
                                                x=longs['formatted_time'],
                                                y=longs['price'],
                                                mode='markers',
                                                name='OPEN_LONG',
                                                marker=dict(
                                                    color='#FF4B4B',
                                                    size=18, 
                                                    symbol='triangle-up',
                                                    line=dict(width=2, color='white')
                                                ),
                                                hovertemplate='<b>OPEN_LONG</b><br>Time: %{x}<br>Price: %{y}<br>Signal: %{customdata[0]:.2f}<extra></extra>',
                                                customdata=longs[['timeSignal', 'snapshot']]
                                            ), secondary_y=False)
                                            
                                        if not shorts.empty:
                                            fig_trades.add_trace(go.Scatter(
                                                x=shorts['formatted_time'],
                                                y=shorts['price'],
                                                mode='markers',
                                                name='OPEN_SHORT',
                                                marker=dict(
                                                    color='#00CC96',
                                                    size=18, 
                                                    symbol='triangle-down',
                                                    line=dict(width=2, color='white')
                                                ),
                                                hovertemplate='<b>OPEN_SHORT</b><br>Time: %{x}<br>Price: %{y}<br>Signal: %{customdata[0]:.2f}<extra></extra>',
                                                customdata=shorts[['timeSignal', 'snapshot']]
                                            ), secondary_y=False)
                                
                                # Calculate dynamic Y-axis range
                                y_min = df_trades['price'].min()
                                y_max = df_trades['price'].max()
                                y_padding = (y_max - y_min) * 0.1 if y_max != y_min else y_max * 0.01
                                
                                # Add PTF horizontal line
                                if ptf:
                                    fig_trades.add_hline(y=ptf, line_dash="dash", line_color="white", annotation_text=f"MCP: {ptf:.2f}", annotation_position="right")
                                
                                fig_trades.update_layout(
                                    # title removed
                                    template="plotly_dark",
                                    xaxis=dict(
                                        title="Time",
                                        showgrid=False,
                                        rangeslider=dict(visible=False),
                                        type="category",
                                        tickangle=45,
                                        nticks=20
                                    ),
                                    yaxis=dict(
                                        title="Price",
                                        showgrid=True,
                                        gridcolor='rgba(255, 255, 255, 0.1)',
                                        zeroline=False,
                                        range=[y_min - y_padding, y_max + y_padding]
                                    ),
                                    yaxis2=dict(
                                        title="Volume",
                                        showgrid=False,
                                        zeroline=False,
                                        showticklabels=False,
                                        overlaying="y",
                                        side="right"
                                    ),
                                    height=700,
                                    hovermode="x unified",
                                    legend=dict(
                                        orientation="v",
                                        yanchor="top",
                                        y=1,
                                        xanchor="left",
                                        x=0.01,
                                        bgcolor="rgba(0,0,0,0.5)"
                                    ),
                                    margin=dict(l=20, r=20, t=60, b=20),
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    hoverlabel=dict(
                                        bgcolor="#262730",
                                        font_color="white",
                                        font_size=14,
                                        bordercolor="rgba(255, 255, 255, 0.3)"
                                    )
                                )
                                
                                selected_points = plotly_events(
                                    fig_trades,
                                    click_event=True,
                                    hover_event=False,
                                    select_event=False,
                                    override_height=700
                                )
                                
                                if selected_points:
                                    for point in selected_points:
                                        # point contains: x, y, curveNumber, pointIndex
                                        curve_num = point.get('curveNumber')
                                        point_idx = point.get('pointIndex')
                                        
                                        snapshot_val = None
                                        
                                        # Trace 0 (Volume) and Trace 1 (Price) use df_trades
                                        if curve_num in [0, 1]:
                                            if point_idx < len(df_trades):
                                                snapshot_val = df_trades.iloc[point_idx]['snapshot']
                                        
                                        if snapshot_val:
                                            # snapshot_val is dd HH:MM, we need full timestamp
                                            full_snapshot = minute_map.get(snapshot_val)
                                            
                                            if full_snapshot and full_snapshot != st.session_state.snap_query_minute:
                                                st.session_state.snap_query_minute = full_snapshot
                                                st.rerun()
                                
                                # Trades Table (Show Newest First)
                                st.dataframe(df_trades.sort_values('timestamp', ascending=False)[['formatted_time', 'price', 'volume', 'snapshot']], use_container_width=True, height=300)
                            else:
                                st.info("No trades found for this snapshot.")

                        # --- Right Column: Depth ---
                        with col_right:
                            st.markdown(f"### Depth ({snapshot_time_str})")
                            
                            mcp = board.get('mcp')
                            
                            bids = depth.get('bid', []) # Buyers (Green)
                            asks = depth.get('ask', []) # Sellers (Red)
                            
                            if bids or asks:
                                # Process Bids
                                if bids:
                                    df_bids = pd.DataFrame(bids)
                                    df_bids = df_bids.iloc[:, :2]
                                    df_bids.columns = ['price', 'volume']
                                    df_bids['price'] = pd.to_numeric(df_bids['price'], errors='coerce')
                                    df_bids['volume'] = pd.to_numeric(df_bids['volume'], errors='coerce')
                                    df_bids = df_bids.sort_values('price', ascending=False) # High to Low
                                    df_bids['cumulative_volume'] = df_bids['volume'].cumsum()
                                else:
                                    df_bids = pd.DataFrame(columns=['price', 'volume', 'cumulative_volume'])
                                
                                # Process Asks
                                if asks:
                                    df_asks = pd.DataFrame(asks)
                                    df_asks = df_asks.iloc[:, :2]
                                    df_asks.columns = ['price', 'volume']
                                    df_asks['price'] = pd.to_numeric(df_asks['price'], errors='coerce')
                                    df_asks['volume'] = pd.to_numeric(df_asks['volume'], errors='coerce')
                                    df_asks = df_asks.sort_values('price', ascending=True) # Low to High
                                    df_asks['cumulative_volume'] = df_asks['volume'].cumsum()
                                else:
                                    df_asks = pd.DataFrame(columns=['price', 'volume', 'cumulative_volume'])
                                
                                # Plotly Depth Chart
                                fig_depth = go.Figure()
                                
                                # Bids Area (Green)
                                if not df_bids.empty:
                                    fig_depth.add_trace(go.Scatter(
                                        x=df_bids['price'],
                                        y=df_bids['cumulative_volume'],
                                        mode='lines',
                                        name='Bids',
                                        fill='tozeroy',
                                        line_shape='hv',
                                        line=dict(color='#28a745'), # Green
                                        fillcolor='rgba(40, 167, 69, 0.2)'
                                    ))
                                
                                # Asks Area (Red)
                                if not df_asks.empty:
                                    fig_depth.add_trace(go.Scatter(
                                        x=df_asks['price'],
                                        y=df_asks['cumulative_volume'],
                                        mode='lines',
                                        name='Asks',
                                        fill='tozeroy',
                                        line_shape='hv',
                                        line=dict(color='#dc3545'), # Red
                                        fillcolor='rgba(220, 53, 69, 0.2)'
                                    ))
                                
                                # MCP Line
                                if mcp:
                                    fig_depth.add_vline(x=mcp, line_dash="dash", line_color="white", annotation_text=f"MCP: {mcp}")
                                
                                fig_depth.update_layout(
                                    template="plotly_dark",
                                    height=700,
                                    margin=dict(l=10, r=10, t=30, b=10),
                                    xaxis_title="Price",
                                    yaxis_title="Cumulative Volume",
                                    legend=dict(
                                        orientation="v",
                                        yanchor="top",
                                        y=1,
                                        xanchor="left",
                                        x=0.01,
                                        bgcolor="rgba(0,0,0,0.5)"
                                    )
                                )
                                st.plotly_chart(fig_depth, use_container_width=True)
                                
                                # Unified Depth Table
                                # Structure: Alış (Hacim, Fiyat) | Satış (Fiyat, Hacim)
                                
                                # Reset index to align rows
                                df_bids_disp = df_bids[['volume', 'price']].reset_index(drop=True)
                                df_asks_disp = df_asks[['price', 'volume']].reset_index(drop=True)
                                
                                # Combine into one DataFrame
                                df_combined = pd.concat([df_bids_disp, df_asks_disp], axis=1)
                                
                                # Create MultiIndex Columns
                                df_combined.columns = pd.MultiIndex.from_tuples([
                                    ('Alış', 'Hacim'), ('Alış', 'Fiyat'),
                                    ('Satış', 'Fiyat'), ('Satış', 'Hacim')
                                ])
                                
                                # Apply Styling
                                def highlight_depth(row):
                                    styles = [''] * 4
                                    # Alış Columns (0, 1) - Very Subtle Green (Almost Black)
                                    if pd.notna(row[('Alış', 'Fiyat')]):
                                        # Very dark green background, standard green text
                                        styles[0] = 'background-color: #051408; color: #4caf50' 
                                        styles[1] = 'background-color: #051408; color: #4caf50'
                                    
                                    # Satış Columns (2, 3) - Very Subtle Red (Almost Black)
                                    if pd.notna(row[('Satış', 'Fiyat')]):
                                        # Very dark red background, standard red text
                                        styles[2] = 'background-color: #140505; color: #ff5252'
                                        styles[3] = 'background-color: #140505; color: #ff5252'
                                        
                                    return styles

                                
                                st.dataframe(
                                    df_combined.style.apply(highlight_depth, axis=1).format("{:.2f}"), 
                                    use_container_width=True, 
                                    height=300
                                )
                                
                            else:
                                st.info("No depth data found.")

                    else:
                        st.warning("No data found for the selected snapshot.")
                except Exception as e:
                    st.error(f"Error fetching snapshot: {e}")

    render_snapshots_tab()

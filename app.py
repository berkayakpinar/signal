import streamlit as st
import functions
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pandas as pd
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
import pytz

# Set page config as the first Streamlit command
st.set_page_config(layout="wide")

# Auto-refresh every 5 minutes (300000 milliseconds)
count = st_autorefresh(interval=300000, limit=None, key="fizzbuzzcounter")

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
            response = supabase.table("signals").select("*").eq("contract", contract).order("snapshot_minute", desc=True).limit(1).execute()
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
        response = supabase.table("signals").select("*").eq("contract", contract).order("snapshot_minute", desc=True).limit(1000).execute()
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
        response = supabase.table("signals").select("*").in_("tradeSignal", ["OPEN_LONG", "OPEN_SHORT"]).order("snapshot_minute", desc=True).limit(limit).execute()
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
    st_autorefresh(interval=300000, key="datarefresh")
    st.write(f"Last Updated: {datetime.now(istanbul_tz).strftime('%Y-%m-%d %H:%M:%S')}")

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
tab_dashboard, tab_timeline, tab_history = st.tabs(["Dashboard", "Timeline", "History"])

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

with tab_history:
    st.subheader("Contract History Analysis")
    
    # 1. Fetch Unique Contracts for Dropdown
    @st.cache_data(ttl=60, show_spinner=False)
    def fetch_history_contracts():
        try:
            all_contracts = set()
            batch_size = 1000
            max_batches = 10  # Fetch up to 10,000 rows max
            
            for i in range(max_batches):
                # Fetch in batches using range
                start = i * batch_size
                end = start + batch_size - 1
                
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
                
                # If we have enough unique contracts, we can stop
                # We want top 50, but we need to be sure they are the *latest* 50.
                # Since we are fetching ordered by time desc, the first 50 unique ones we encounter
                # are by definition the 50 most recent unique contracts.
                if len(all_contracts) >= 50:
                    break
            
            # Convert to list and sort
            # Note: The order of insertion in a set is not guaranteed, but we want to display them 
            # sorted by name or by recency? User asked for "50 most recent".
            # The loop above finds the 50 most recent. Let's return them sorted by name for the dropdown,
            # or keep them in recency order? Usually dropdowns are easier to use if sorted by name.
            # But "most recent" implies recency is important.
            # Let's sort by name for now as per previous behavior, but we KNOW they are the top 50 recent.
            
            sorted_contracts = sorted(list(all_contracts), reverse=True)
            return sorted_contracts[:50]
            
        except Exception as e:
            print(f"Error fetching history contracts: {e}")
        return []

    history_contracts = fetch_history_contracts()
    
    selected_history_contract = None
    if history_contracts:
        selected_history_contract = st.selectbox("Select Contract to Analyze", history_contracts)
        
    if selected_history_contract:
        # 2. Fetch Snapshot Data (Price History from Trades)
        @st.cache_data(ttl=60, show_spinner=False)
        def fetch_snapshot_history(contract):
            try:
                # Fetch ONLY the LATEST snapshot for the contract
                response = supabase.table("snapshots").select("trades").eq("contract", contract).order("snapshot_minute", desc=True).limit(1).execute()
                if response.data:
                    trades_data = response.data[0].get('trades', [])
                    if trades_data:
                        df = pd.DataFrame(trades_data)
                        # Rename columns for clarity: p->price, q->volume, t->timestamp
                        df = df.rename(columns={'p': 'price', 'q': 'volume', 't': 'timestamp'})
                        
                        # Convert types
                        df['price'] = pd.to_numeric(df['price'], errors='coerce')
                        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                        
                        # Convert timestamp (Unix epoch in seconds)
                        # Assuming 't' is in seconds. If it's huge, might be ms.
                        # Sample: 1762268426.502 -> 2025... seems like seconds
                        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                        
                        try:
                            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                        except TypeError:
                            df['timestamp'] = df['timestamp'].dt.tz_convert('Europe/Istanbul')
                            
                        return df.sort_values('timestamp')
            except Exception as e:
                print(f"Error fetching snapshot history for {contract}: {e}")
            return pd.DataFrame()

        # 3. Fetch Signal Data (Overlay)
        @st.cache_data(ttl=60, show_spinner=False)
        def fetch_history_signals(contract):
            try:
                response = supabase.table("signals").select("*").eq("contract", contract).order("snapshot_minute", desc=False).execute()
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

        with st.spinner(f"Loading history for {selected_history_contract}..."):
            price_df = fetch_snapshot_history(selected_history_contract)
            signal_df = fetch_history_signals(selected_history_contract)
        
        if not price_df.empty:
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
            
            # Format timestamp for X-axis labels (dd:mm hh:mm:ss)
            price_df['formatted_time'] = price_df['timestamp'].dt.strftime('%d:%m %H:%M:%S')
            
            # Create Figure with Secondary Y-Axis
            fig_hist = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add Volume Bar (Secondary Axis)
            fig_hist.add_trace(go.Bar(
                x=price_df['formatted_time'],
                y=price_df['volume'],
                name='Volume',
                marker_color='rgba(128, 128, 128, 0.5)', # Gray with opacity
                opacity=0.6
            ), secondary_y=True)

            # Add Price Line (Primary Axis)
            fig_hist.add_trace(go.Scatter(
                x=price_df['formatted_time'],
                y=price_df['price'],
                mode='lines',
                name='Price',
                line=dict(color='#00BFFF', width=2), # Deep Sky Blue
                fill='tozeroy',
                fillcolor='rgba(0, 191, 255, 0.1)' # Transparent Blue Fill
            ), secondary_y=False)
            
            # Add Signal Overlay
            if not signal_df.empty:
                # Filter for actual trades
                trades = signal_df[signal_df['tradeSignal'].isin(['OPEN_LONG', 'OPEN_SHORT'])]
                
                if not trades.empty:
                    trades = trades.sort_values('snapshot_minute')
                    price_df = price_df.sort_values('timestamp')
                    
                    # Use merge_asof to find the nearest price for each trade
                    # Note: signal time is 'snapshot_minute', price time is 'timestamp'
                    merged_trades = pd.merge_asof(
                        trades, 
                        price_df, 
                        left_on='snapshot_minute',
                        right_on='timestamp',
                        direction='nearest',
                        tolerance=pd.Timedelta('5min')
                    )
                    
                    # Format timestamp for matched trades to align with x-axis
                    merged_trades['formatted_time'] = merged_trades['timestamp'].dt.strftime('%d:%m %H:%M:%S')
                    
                    # Separate Long and Short for colors
                    longs = merged_trades[merged_trades['tradeSignal'] == 'OPEN_LONG']
                    shorts = merged_trades[merged_trades['tradeSignal'] == 'OPEN_SHORT']
                    
                    if not longs.empty:
                        fig_hist.add_trace(go.Scatter(
                            x=longs['formatted_time'], # Use the formatted time
                            y=longs['price'],
                            mode='markers',
                            name='OPEN_LONG',
                            marker=dict(
                                color='#FF4B4B', # Streamlit Red
                                size=18, 
                                symbol='triangle-up',
                                line=dict(width=2, color='white')
                            ),
                            hovertemplate='<b>OPEN_LONG</b><br>Time: %{x}<br>Price: %{y}<br>Signal: %{customdata:.2f}<extra></extra>',
                            customdata=longs['timeSignal']
                        ), secondary_y=False)
                        
                    if not shorts.empty:
                        fig_hist.add_trace(go.Scatter(
                            x=shorts['formatted_time'],
                            y=shorts['price'],
                            mode='markers',
                            name='OPEN_SHORT',
                            marker=dict(
                                color='#00CC96', # Bright Green
                                size=18, 
                                symbol='triangle-down',
                                line=dict(width=2, color='white')
                            ),
                            hovertemplate='<b>OPEN_SHORT</b><br>Time: %{x}<br>Price: %{y}<br>Signal: %{customdata:.2f}<extra></extra>',
                            customdata=shorts['timeSignal']
                        ), secondary_y=False)

            # Calculate dynamic Y-axis range
            y_min = price_df['price'].min()
            y_max = price_df['price'].max()
            y_padding = (y_max - y_min) * 0.1 if y_max != y_min else y_max * 0.01
            
            # Update Layout for Premium Look
            fig_hist.update_layout(
                title=dict(
                    text=f"Price Action & Signals: {selected_history_contract}",
                    font=dict(size=20, color='white')
                ),
                template="plotly_dark",
                xaxis=dict(
                    title="Time",
                    showgrid=False,
                    rangeslider=dict(visible=True),
                    type="category", # Equal spacing
                    tickangle=45,
                    nticks=20
                ),
                yaxis=dict(
                    title="Price",
                    showgrid=True,
                    gridcolor='rgba(255, 255, 255, 0.1)',
                    zeroline=False,
                    range=[y_min - y_padding, y_max + y_padding] # Dynamic range
                ),
                yaxis2=dict(
                    title="Volume",
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False, # Hide volume labels to keep it clean
                    overlaying="y",
                    side="right"
                ),
                height=700,
                hovermode="x unified",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=60, b=20),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Show raw data toggle
            if st.checkbox("Show Raw Data"):
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    st.write("Price Data")
                    st.dataframe(price_df, use_container_width=True)
                with col_d2:
                    st.write("Signal Data")
                    st.dataframe(signal_df, use_container_width=True)

        else:
            st.warning(f"No price history found for {selected_history_contract}")
        
    else:
        st.info("No history contracts found.")

import time
import streamlit as st
import pandas as pd
from api import get_all_markets, get_dexes

st.set_page_config(
    page_title="HIP-3 Markets Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("HIP-3 Markets Dashboard")
st.caption("Hyperliquid builder-deployed perpetuals — all data from public API")

# --- Auto-refresh toggle ---
col_refresh, col_spacer = st.columns([1, 5])
with col_refresh:
    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)



# --- Data fetching ---
@st.cache_data(ttl=30)
def load_data() -> pd.DataFrame:
    return get_all_markets()


@st.cache_data(ttl=30)
def load_dexes() -> list[dict]:
    return get_dexes()


with st.spinner("Fetching markets..."):
    df = load_data()
    dexes = load_dexes()

if df.empty:
    st.error("No market data returned from the API. Please try again.")
    st.stop()

# --- Summary metrics ---
total_dexes = df["dex"].nunique()
total_markets = len(df)
total_volume = df["day_ntl_vlm"].sum()
total_oi = df["open_interest"].sum()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total DEXes", total_dexes)
m2.metric("Total Markets", total_markets)
m3.metric("Total 24h Volume", f"${total_volume:,.0f}")
m4.metric("Total Open Interest", f"${total_oi:,.0f}")

st.divider()

# --- Formatters (used throughout) ---
def fmt_price(x):
    if pd.isna(x) or x is None:
        return "—"
    return f"${x:,.4f}" if x < 10 else f"${x:,.2f}"

def fmt_usd(x):
    if pd.isna(x) or x is None:
        return "—"
    if x >= 1_000_000:
        return f"${x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x / 1_000:.1f}K"
    return f"${x:,.0f}"

def fmt_pct(x):
    if pd.isna(x) or x is None:
        return "—"
    return f"{x:.1f}%"

def fmt_funding(x):
    if pd.isna(x) or x is None:
        return "—"
    return f"{float(x) * 100:.4f}%"

def fmt_age(x):
    if pd.isna(x) or x is None:
        return "N/A"
    return str(int(x))

# --- Newest markets ---
st.subheader("Newest Markets")
newest = (
    df[df["listing_date"].notna()]
    .sort_values("listing_date", ascending=False)
    .head(5)
)
new_cols = {
    "ticker": "Ticker",
    "dex_full_name": "DEX",
    "listing_date": "Listed",
    "market_age_days": "Age (days)",
    "mark_px": "Mark Price",
    "day_ntl_vlm": "24h Volume ($)",
    "open_interest": "Open Interest ($)",
}
new_df = newest[list(new_cols.keys())].copy()
new_df.rename(columns=new_cols, inplace=True)
new_df["Listed"] = new_df["Listed"].dt.strftime("%Y-%m-%d %H:%M UTC")
new_df["Mark Price"] = new_df["Mark Price"].apply(fmt_price)
new_df["24h Volume ($)"] = new_df["24h Volume ($)"].apply(fmt_usd)
new_df["Open Interest ($)"] = new_df["Open Interest ($)"].apply(fmt_usd)
st.dataframe(new_df, use_container_width=True, hide_index=True)

st.divider()

# --- DEX filter ---
all_dexes = sorted(df["dex_full_name"].unique().tolist())
selected_dexes = st.multiselect(
    "Filter by DEX",
    options=all_dexes,
    default=all_dexes,
    placeholder="Select DEXes...",
)

filtered = df[df["dex_full_name"].isin(selected_dexes)].copy() if selected_dexes else df.copy()

# --- Markets table ---
st.subheader(f"Markets ({len(filtered)})")

display_cols = {
    "ticker": "Ticker",
    "dex_full_name": "DEX",
    "mark_px": "Mark Price",
    "day_ntl_vlm": "24h Volume ($)",
    "open_interest": "Open Interest ($)",
    "oi_cap": "OI Cap ($)",
    "oi_cap_pct": "OI Cap %",
    "funding": "Funding",
    "max_leverage": "Max Lev",
    "growth_mode": "Growth Mode",
    "market_age_days": "Age (days)",
}

table_df = filtered[list(display_cols.keys())].copy()
table_df.rename(columns=display_cols, inplace=True)

table_df["Mark Price"] = table_df["Mark Price"].apply(fmt_price)
table_df["24h Volume ($)"] = table_df["24h Volume ($)"].apply(fmt_usd)
table_df["Open Interest ($)"] = table_df["Open Interest ($)"].apply(fmt_usd)
table_df["OI Cap ($)"] = table_df["OI Cap ($)"].apply(fmt_usd)
table_df["OI Cap %"] = table_df["OI Cap %"].apply(fmt_pct)
table_df["Funding"] = table_df["Funding"].apply(fmt_funding)
table_df["Age (days)"] = table_df["Age (days)"].apply(fmt_age)

st.dataframe(table_df, use_container_width=True, hide_index=True)

# --- Per-DEX breakdown ---
st.divider()
st.subheader("Per-DEX Breakdown")

dex_meta = {d.get("name"): d for d in dexes}

for dex_name in sorted(filtered["dex"].unique()):
    dex_rows = filtered[filtered["dex"] == dex_name]
    full_name = dex_rows["dex_full_name"].iloc[0]
    deployer = dex_rows["deployer"].iloc[0] if "deployer" in dex_rows.columns else ""
    dex_volume = dex_rows["day_ntl_vlm"].sum()
    dex_oi = dex_rows["open_interest"].sum()
    market_count = len(dex_rows)

    label = f"{full_name} — {market_count} markets | Vol: {fmt_usd(dex_volume)} | OI: {fmt_usd(dex_oi)}"
    with st.expander(label):
        if deployer:
            st.caption(f"Deployer: `{deployer}`")

        sub_cols = {
            "ticker": "Ticker",
            "mark_px": "Mark Price",
            "day_ntl_vlm": "24h Volume ($)",
            "open_interest": "Open Interest ($)",
            "oi_cap": "OI Cap ($)",
            "oi_cap_pct": "OI Cap %",
            "funding": "Funding",
            "max_leverage": "Max Lev",
            "growth_mode": "Growth Mode",
            "market_age_days": "Age (days)",
        }
        sub_df = dex_rows[list(sub_cols.keys())].copy()
        sub_df.rename(columns=sub_cols, inplace=True)

        sub_df["Mark Price"] = sub_df["Mark Price"].apply(fmt_price)
        sub_df["24h Volume ($)"] = sub_df["24h Volume ($)"].apply(fmt_usd)
        sub_df["Open Interest ($)"] = sub_df["Open Interest ($)"].apply(fmt_usd)
        sub_df["OI Cap ($)"] = sub_df["OI Cap ($)"].apply(fmt_usd)
        sub_df["OI Cap %"] = sub_df["OI Cap %"].apply(fmt_pct)
        sub_df["Funding"] = sub_df["Funding"].apply(fmt_funding)
        sub_df["Age (days)"] = sub_df["Age (days)"].apply(fmt_age)

        st.dataframe(sub_df, use_container_width=True, hide_index=True)

st.caption(f"Data cached for 30s. Last loaded at current session time.")

if auto_refresh:
    time.sleep(30)
    st.rerun()

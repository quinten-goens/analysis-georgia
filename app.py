import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import plotly.graph_objects as go

st.set_page_config(page_title="UGTB Flight Comparison", layout="wide")
st.title("UGTB Flight Comparison: OPDI vs APDF — March 2026")

# --- Load data ---------------------------------------------------------------
@st.cache_data
def load_data():
    # OPDI flights filtered to UGTB
    opdi = pq.read_table("data/filtered/flights_UGTB.parquet").to_pandas()
    opdi["dof"] = pd.to_datetime(opdi["dof"]).dt.date

    # Departures: adep == UGTB
    opdi_dep = opdi[opdi["adep"] == "UGTB"].groupby("dof").size().rename("OPDI")
    # Arrivals: ades == UGTB
    opdi_arr = opdi[opdi["ades"] == "UGTB"].groupby("dof").size().rename("OPDI")

    # APDF reference
    apdf = pq.read_table("data/reference/apdf_UGTB_202603.parquet").to_pandas()
    apdf["date"] = pd.to_datetime(apdf["MVT_TIME_UTC"]).dt.date

    apdf_dep = apdf[apdf["SRC_PHASE"] == "DEP"].groupby("date").size().rename("APDF")
    apdf_arr = apdf[apdf["SRC_PHASE"] == "ARR"].groupby("date").size().rename("APDF")

    deps = pd.concat([opdi_dep, apdf_dep], axis=1).fillna(0).astype(int)
    arrs = pd.concat([opdi_arr, apdf_arr], axis=1).fillna(0).astype(int)

    deps.index.name = "date"
    arrs.index.name = "date"

    return deps.reset_index(), arrs.reset_index()


deps, arrs = load_data()

# --- KPI row -----------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("OPDI Departures", int(deps["OPDI"].sum()))
col2.metric("APDF Departures", int(deps["APDF"].sum()))
col3.metric("OPDI Arrivals", int(arrs["OPDI"].sum()))
col4.metric("APDF Arrivals", int(arrs["APDF"].sum()))

# --- Charts ------------------------------------------------------------------
def comparison_chart(df, title):
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["OPDI"], name="OPDI", marker_color="#1f77b4"))
    fig.add_trace(go.Bar(x=df["date"], y=df["APDF"], name="APDF", marker_color="#ff7f0e"))
    fig.update_layout(
        title=title,
        barmode="group",
        xaxis_title="Date",
        yaxis_title="Number of flights",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
    )
    return fig


left, right = st.columns(2)
with left:
    st.plotly_chart(comparison_chart(deps, "Departures (UGTB)"), use_container_width=True)
with right:
    st.plotly_chart(comparison_chart(arrs, "Arrivals (UGTB)"), use_container_width=True)

# --- Difference table --------------------------------------------------------
st.subheader("Daily Differences (OPDI − APDF)")

diff = deps[["date"]].copy()
diff["DEP diff"] = deps["OPDI"] - deps["APDF"]
diff["ARR diff"] = arrs["OPDI"] - arrs["APDF"]

st.dataframe(diff, use_container_width=True, hide_index=True)

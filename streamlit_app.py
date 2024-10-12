import streamlit as st
import duckdb
import pandas as pd
import numpy as np
from plotly.express import choropleth_mapbox
import plotly.express as px
import json

st.set_page_config(layout="wide")

# Initialize session state
if "selected_cbsa" not in st.session_state:
    st.session_state.selected_cbsa = None

# create an in-memory database
con = duckdb.connect(database=":memory:")

# load data into the database
con.execute(
    """CREATE OR REPLACE TABLE hpi_tract AS 
    SELECT *,
    left(tract, 5) as fips
    FROM read_csv('./data/csv/hpi_at_bdl_tract.csv', nullstr='.')"""
)
con.execute(
    """CREATE OR REPLACE TABLE hpi_zip 
    AS SELECT *
    FROM read_csv('./data/csv/hpi_zip5.csv', nullstr='.')"""
)
con.execute(
    """CREATE OR REPLACE TABLE zip_cbsa AS SELECT *
    FROM read_csv('./data/csv/us_zip5_cbsa.csv', nullstr='.')"""
)
con.execute(
    """CREATE OR REPLACE TABLE zip_attr AS
    SELECT * FROM read_csv('./data/csv/us_zip5_attr.csv', nullstr='.')"""
)
con.execute(
    """CREATE OR REPLACE TABLE zip_pop AS SELECT *
    FROM read_csv('./data/csv/us_zip5_population.csv', nullstr='.')"""
)
con.execute(
    """CREATE OR REPLACE TABLE fips_cbsa AS SELECT *,
    (fipsstatecode || fipscountycode) as fips
    FROM read_csv('./data/csv/us_fips_cbsa.csv', nullstr='.')"""
)
con.execute(
    """CREATE TABLE cbsa AS SELECT * 
    FROM read_csv('./data/csv/us_cbsas.csv', nullstr='.')"""
)

with open("./data/json/geojson-counties-fips.json", "r", encoding="utf8") as f:
    geojson_counties = json.load(f)


@st.cache_data(ttl=600)
def run_query(query):
    try:
        df_raw = con.sql(query).df()
        return df_raw
    except Exception as e:
        st.error(f"Error running query: {e}")
        return pd.DataFrame()


# Display data
# st.write(con.sql("SELECT * FROM hpi LIMIT 10"))
# st.write(con.sql("SELECT * FROM cbsa LIMIT 10"))
# st.write(con.sql("SELECT * FROM zip_cbsa LIMIT 10"))
# st.write(con.sql("SELECT * FROM zip_attr LIMIT 10"))
# st.write(con.sql("SELECT * FROM zip_pop LIMIT 10"))


st.header("GFC Explorer")


if not st.session_state.selected_cbsa:
    st.write(
        """
        ⬅️ First, choose a CBSA from the dropdown menu on the left. ⬅️
        """
    )

# Sidebar
with st.sidebar:
    previous_selection = st.session_state.selected_cbsa
    # sorted by population descending
    cbsa_options = run_query("""
                             SELECT DISTINCT c."CBSA Name" as cbsa_name 
                             FROM cbsa c
                             JOIN zip_cbsa zc ON c."CBSA Code" = zc.CBSA
                             JOIN zip_pop zp ON zc.ZIP = zp.ZIP
                             GROUP BY c."CBSA Name"
                             ORDER BY SUM(TRY_CAST(zp.POPULATION AS INTEGER)) DESC
                             """)
    selected_cbsa = st.selectbox(
        "Select a CBSA",
        cbsa_options,
    )

    # Update session state
    if selected_cbsa != previous_selection:
        st.session_state.selected_cbsa = selected_cbsa
        # st.write(f"Debug: CBSA selection changed to {selected_cbsa}")

    out_df = run_query(
        f"""
        with hpi_per_tract as (
            SELECT
                cbsa."CBSA Name" as cbsa_name, 
                hpi.fips as fips,
                avg(zip_attr.lat) as latitude,
                avg(zip_attr.lng) as longitude,
                sum(distinct TRY_CAST(zip_attr.population AS INTEGER)) as population,
                min(TRY_CAST(hpi.HPI AS FLOAT)) as min_hpi, 
                max(TRY_CAST(hpi.HPI AS FLOAT)) as max_hpi
            FROM hpi_tract hpi
            left join fips_cbsa on hpi.fips = fips_cbsa.fips
            left join zip_attr on hpi.fips = zip_attr.county_fips
            left join cbsa on fips_cbsa.cbsacode = cbsa."CBSA Code"
            WHERE 1=1
            AND cbsa."CBSA Name" ILIKE '{st.session_state.selected_cbsa}'
            AND TRY_CAST(hpi.YEAR AS INTEGER) BETWEEN 2005 AND 2013
            AND TRY_CAST(hpi.HPI AS FLOAT) > 0
            GROUP BY 1,2
            )
            select 
                hpi_per_tract.*,
                hmin.YEAR as min_year,
                hmax.YEAR as max_year,
                (min_hpi/max_hpi - 1) as hpi_loss
            from hpi_per_tract
            left join hpi_tract hmin on hpi_per_tract.fips = hmin.fips 
                and TRY_CAST(hmin.HPI AS FLOAT) = hpi_per_tract.min_hpi
            left join hpi_tract hmax on hpi_per_tract.fips = hmax.fips 
                and TRY_CAST(hmax.HPI AS FLOAT) = hpi_per_tract.max_hpi
            """
    )


# Main Content
with st.container():
    st.write("For the selected CBSA, the following views are available:")
    if st.session_state.selected_cbsa:
        col1, col2 = st.columns(2)

        with col1:
            # output a df in a sortable/filterable grid table
            st.dataframe(out_df)

            # output a line chart showing average of HPI across all fips by year
            hpi_by_year = run_query(f"""
                SELECT YEAR::int as year, AVG(TRY_CAST(HPI AS FLOAT)) as avg_hpi
                FROM hpi_zip hpi
                JOIN zip_cbsa ON hpi."Five-Digit ZIP Code" = zip_cbsa.ZIP
                JOIN cbsa ON zip_cbsa.CBSA = cbsa."CBSA Code"
                WHERE cbsa."CBSA Name" ILIKE '{st.session_state.selected_cbsa}'
                AND YEAR::int BETWEEN 2005 AND 2013
                AND TRY_CAST(HPI AS FLOAT) > 0
                GROUP BY YEAR::int
                ORDER BY YEAR::int
            """)

            # Calculate y-axis range
            y_min = hpi_by_year["avg_hpi"].min() * 0.95  # 5% below the minimum value
            y_max = hpi_by_year["avg_hpi"].max() * 1.05  # 5% above the maximum value

            # Find the maximum and minimum points
            max_point = hpi_by_year.loc[hpi_by_year["avg_hpi"].idxmax()]
            min_point = hpi_by_year.loc[hpi_by_year["avg_hpi"].idxmin()]

            # Calculate percentage loss
            percent_loss = (
                (min_point["avg_hpi"] - max_point["avg_hpi"])
                / max_point["avg_hpi"]
                * 100
            )

            # Create the line chart using Plotly
            fig = px.line(
                hpi_by_year,
                x="year",
                y="avg_hpi",
                title=f"Average HPI for {st.session_state.selected_cbsa}",
            )

            # Add vertical line and annotation
            fig.add_shape(
                type="line",
                x0=max_point["year"],
                y0=max_point["avg_hpi"],
                x1=max_point["year"],
                y1=min_point["avg_hpi"],
                line=dict(color="red", width=2, dash="dash"),
            )

            fig.add_annotation(
                x=max_point["year"],
                y=min_point["avg_hpi"],  # Position at the bottom of the line
                text=f"{percent_loss:.1f}% loss",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="red",
                ax=40,
                ay=-40,  # Adjust this value to move the annotation up or down
                yanchor="top",  # Anchor the text to the top so it appears below the arrow
            )

            # Update the layout to set the y-axis range
            fig.update_layout(
                yaxis_range=[y_min, y_max],
                xaxis_title="Year",
                yaxis_title="Average HPI",
            )

            # Display the chart
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # plotting a map with plotly for each using fips + loss from HPI Max to HPI Min

            # set center lat and lon to the mean of the latitude and longitude columns
            center_lat = out_df["latitude"].mean()
            center_lon = out_df["longitude"].mean()

            fig = choropleth_mapbox(
                out_df,
                geojson=geojson_counties,
                locations="fips",
                color="hpi_loss",
                color_continuous_scale="RdYlGn",
                range_color=(-0.5, 0),  # Adjust this range based on your data
                mapbox_style="carto-positron",
                zoom=8,
                opacity=0.7,
                labels={"hpi_loss": "HPI Loss"},
            )
            fig.update_layout(
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                mapbox=dict(
                    bearing=0,
                    center=dict(lat=center_lat, lon=center_lon),
                    pitch=0,
                    zoom=7,  # Increased from 2 to 7 for a closer default zoom
                ),
            )

            st.plotly_chart(fig, use_container_width=True)

            # Calculate total population for the CBSA
            total_population = out_df["population"].sum()

            # Format the population with commas for readability
            formatted_population = f"{total_population:,}"

            # Make a dynamic text box with a few bullet points saying the min and max year and the loss from max hpi to min hpi
            min_year = out_df["min_year"].min()
            max_year = out_df["max_year"].max()
            avg_hpi_loss = out_df["hpi_loss"].mean()

            st.markdown(f"""
            ### Key Statistics for {st.session_state.selected_cbsa}
            
            - **Total Metro Area Population:** {formatted_population}
            - **Minimum HPI Year:** {min_year}
            - **Maximum HPI Year:** {max_year}
            - **Average HPI Loss:** {avg_hpi_loss:.2%}
            """)

    else:
        st.write("No CBSA selected")

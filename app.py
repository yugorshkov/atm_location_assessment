import geopandas
import streamlit as st
from streamlit_folium import st_folium
from src.mc import create_minio_client

st.set_page_config(
    page_title="Bankamatika",
    page_icon=":atm:",
)
st.title(":bank: Оценка места размещения банкоматов")

minio_client = create_minio_client()
url = minio_client.presigned_get_object(
    "atm-location-assessment", "krd-h3-atm-score.geojson"
)
gdf = geopandas.read_file(url)

st.subheader("test")
st.write(gdf.head())


m = gdf.explore(
    column="location_score",
    cmap="PuBu",
    # tooltip="location_score",
    # popup=True,
    tiles="OpenStreetMap",
    style_kwds={"fillOpacity": 0.7},
)
st_data = st_folium(m, width=725)

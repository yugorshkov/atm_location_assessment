import folium
import geopandas
import streamlit as st
from streamlit_folium import st_folium
from src.mc import create_minio_client


def main(gdf):
    st.set_page_config(page_title="Bankamatika", page_icon=":atm:")
    st.title(":bank: Bankamatika")
    st.subheader("Оценка места размещения банкомата")

    col1, col2 = st.columns(2)
    with col1:
        city = st.selectbox("Выберите город", ("Краснодар",))
    with col2:
        access_mode = st.radio(
            "Время работы", ("круглосуточно", "до 23:00", "до 19:00"), index=1
        )
    d = {"круглосуточно": 100, "до 23:00": 90, "до 19:00": 38}

    gdf["access_mode"] = d[access_mode] * 0.2
    gdf["location_score"] = (
        gdf["placement"] + gdf["access_mode"] + gdf["population"] + gdf["pois"]
    )

    m = gdf.explore(
        column="location_score",
        cmap="PuBu",
        scheme="naturalbreaks",
        k=5,
        tooltip="location_score",
        # tooltip_kwds=dict(labels=False),
        # popup=True,
        tiles="OpenStreetMap",
        tooltip_kwds={},
        style_kwds={"fillOpacity": 0.7},
        name="h3_cells",
    )

    st_data = st_folium(m, width=725, zoom=11)
    st.write(gdf.head())


if __name__ == "__main__":
    minio_client = create_minio_client()
    url = minio_client.presigned_get_object(
        "atm-location-assessment", "krd-h3-atm-score.geojson"
    )
    gdf = geopandas.read_file(url)
    main(gdf)

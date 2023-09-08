import geopandas
import streamlit as st
from streamlit_folium import st_folium
from src.mc import create_minio_client


def atms_app():
    st.set_page_config(page_title="Bankamatika", page_icon=":atm:")
    st.title(":bank: Bankamatika")
    st.subheader("Оценка мест размещения банкоматов")

    col1, col2 = st.columns(2)
    with col1:
        cities = {
            "Краснодар": "krasnodar",
            "Новороссийск": "novorossiysk",
            "Армавир": "armavir",
            "Ростов-на-Дону": "rostov",
        }
        city = st.selectbox("Выберите город:", cities)
    with col2:
        working_hours = st.radio(
            "Ожидаемый режим работы устройства:",
            ("круглосуточно", "до 23:00", "до 19:00"),
            index=1,
        )
    st.info(
        """Город разделён на шестиугольные зоны - гексы. Чем интенсивнее заливка, 
        тем больше эта территория подходит для размещения банкоматов.""",
        icon="ℹ️",
    )
    minio_client = create_minio_client()
    url = minio_client.presigned_get_object(
        "atm-location-assessment", f"{cities[city]}-h3-atm-score.geojson"
    )
    gdf = geopandas.read_file(url)
    access_mode_score = {"круглосуточно": 100, "до 23:00": 90, "до 19:00": 38}
    access_mode_weight = 0.2
    gdf["access_mode"] = access_mode_score[working_hours] * access_mode_weight
    gdf["location_score"] = (
        gdf["placement"] + gdf["access_mode"] + gdf["population"] + gdf["pois"]
    ).round(2)

    m = gdf.explore(
        column="location_score",
        cmap="PuBu",
        scheme="Quantiles",
        k=5,
        legend_kwds={"caption": "Баллы", "scale": False},
        tooltip="location_score",
        tooltip_kwds={"aliases": ["Оценка территории: "]},
        popup=["location_score"],
        popup_kwds={
            "aliases": [
                "Эффективность размещения банкомата в этой области по 100-бальной шкале:"
            ]
        },
        style_kwds={"fillOpacity": 0.7},
        name="h3_cells",
    )
    st_folium(m, width=725, zoom=11, returned_objects=[])


if __name__ == "__main__":
    atms_app()

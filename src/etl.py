import warnings
warnings.filterwarnings("ignore")

import folium
import geopandas
import h3pandas
import json
import numpy as np
import os
import pandas as pd
import pyrosm
from dotenv import load_dotenv
from minio import Minio
from pathlib import Path
from prefect import flow, task


# @task
def process_apartment_buildings_data(url: str, h3_res: int = 8) -> pd.DataFrame:
    """Считываем данные из хранилища, выполняем обработку для расчёта 
    численности жителей многоквартирных домов и вычисляем индекс h3.
    """
    gdf = geopandas.read_file(url)
    attrs = ["RMC", "RMC_LIVE", "INHAB", "AREA", "AREA_LIVE", "geometry"]
    gdf = gdf[attrs]

    gdf = gdf.replace({"": np.nan})
    num_col_format = lambda x: str(x).replace(" ", "").replace(",", ".")
    gdf["RMC"] = gdf["RMC"].apply(num_col_format)
    gdf["RMC_LIVE"] = gdf["RMC_LIVE"].apply(num_col_format)
    gdf["AREA"] = gdf["AREA"].apply(num_col_format)
    gdf["AREA_LIVE"] = gdf["AREA_LIVE"].apply(num_col_format)
    col_types = {attr: "float" for attr in attrs[:-1]}
    gdf = gdf.astype(col_types)

    check_rmc_na = (gdf["RMC"].isna()) | (gdf["RMC"] >= gdf["RMC_LIVE"])
    gdf["RMC_LIVE"] = np.where(check_rmc_na, gdf["RMC_LIVE"], gdf["RMC"])

    temp = gdf[gdf["INHAB"].notna()]
    residents_in_apartment = (temp["INHAB"] / temp["RMC_LIVE"]).mean().round(2)
    gdf["inhab_calc"] = gdf["RMC_LIVE"] * residents_in_apartment
    residents_in_house = gdf["inhab_calc"].mean()
    gdf["inhab_calc"] = gdf["inhab_calc"].fillna(residents_in_house)
    gdf["inhab_calc"] = gdf[["INHAB", "inhab_calc"]].max(axis=1)
    gdf["inhab_calc"] = gdf["inhab_calc"].astype(int)

    gdf = gdf[["inhab_calc", "geometry"]].rename(columns={"inhab_calc": "population"})
    dfh3 = gdf.h3.geo_to_h3_aggregate(h3_res, return_geometry=False)
    return dfh3


# @task(retries=3, retry_delay_seconds=5)
def get_osm_data(region: str, city_name: str, osm_city_id: str) -> str:
    """Загружаем файл карт OSM. Обрезаем файл по границам города и 
    фильтруем объекты карт по тэгу.
    """
    osm_data_path = f"data/pois-in-{city_name}.osm.pbf"
    os.system(f"src/get_osm_data {region} {city_name} {osm_city_id} {osm_data_path}")
    return osm_data_path


# @task
def get_pois(fp: str, h3_res: int = 8) -> pd.DataFrame:
    osm = pyrosm.OSM(fp)
    with open("data/osm_tags_filter.json") as f:
        custom_filter = json.load(f)
    pois = osm.get_data_by_custom_criteria(custom_filter=custom_filter)
    pois["geometry"] = pois.centroid
    pois = pois[["id", "geometry"]]
    pois = pois.h3.geo_to_h3_aggregate(h3_res, "count", return_geometry=False).rename(
        columns={"id": "pois"}
    )
    return pois


def evaluate_locations(
    gdf: geopandas.GeoDataFrame, group: int, mode: int
) -> geopandas.GeoDataFrame:
    placement_object_type = {1: 100, 2: 90, 3: 0}
    access_mode = {24: 100, 23: 90, 19: 38}
    criterion_weight = {
        "placement_object_type": 0.3,
        "access_mode": 0.2,
        "population": 0.3,
        "customer_traffic_sources": 0.2,
    }
    gdf["placement_object_type"] = (
        placement_object_type[group] * criterion_weight["placement_object_type"]
    )
    gdf["access_mode"] = access_mode[mode] * criterion_weight["access_mode"]
    gdf["population"] = (
        np.where(gdf["population"] * 0.007 > 100, 100, gdf["population"] * 0.007)
        * criterion_weight["population"]
    )
    gdf["customer_traffic_sources"] = (
        np.where(gdf["pois"] * 1.25 > 100, 100, gdf["pois"] * 1.25)
        * criterion_weight["customer_traffic_sources"]
    )
    gdf["location_score"] = (
        gdf["placement_object_type"]
        + gdf["access_mode"]
        + gdf["population"]
        + gdf["customer_traffic_sources"]
    )
    return gdf


def make_choropleth_map(gdf: geopandas.GeoDataFrame, col: str) -> folium.Map:
    m = gdf.explore(
        column=col, cmap="PuBu", tiles="OpenStreetMap", style_kwds={"fillOpacity": 0.7}
    )
    return m


# @flow
def main():
    load_dotenv()
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    client = Minio(endpoint, access_key, secret_key, secure=False)
    object_name = "myhouse_RU-CITY-016_points_matched.geojson"

    apartment_buildings_data = process_apartment_buildings_data(
        client.presigned_get_object(bucket_name, object_name)
    )

    region, city_name, osm_city_id = "south", "krasnodar", "r7373058"
    pois = get_pois(get_osm_data(region, city_name, osm_city_id))
    gdfh3 = (
        pd.concat([apartment_buildings_data, pois], axis=1)
        .fillna(0)
        .h3.h3_to_geo_boundary()
    )

    print(evaluate_locations(gdfh3, 2, 23))


if __name__ == "__main__":
    main()

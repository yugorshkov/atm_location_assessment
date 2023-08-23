import geopandas
import h3pandas
import json
import numpy as np
import os
import pandas as pd
import pyrosm
from collections import namedtuple
from dotenv import load_dotenv
from mc import create_minio_client, upload_to_minio
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import NamedTuple


AVG_RESIDENTS_IN_APARTMENT = 2.11 # 1.87 - 2.11
AVG_RESIDENTS_IN_HOUSE = 185 # 185 - 220


City = namedtuple("City", ["name", "osm_id", "region", "apartment_buildings_data"])


@task
def process_apartment_buildings_data(
    url: str, residents_in_apartment: float, h3_res: int = 8
) -> pd.DataFrame:
    """Считываем данные из хранилища, выполняем обработку для расчёта
    численности жителей многоквартирных домов в ячейке h3.
    """
    gdf = geopandas.read_file(url)
    attrs = ["RMC", "RMC_LIVE", "INHAB", "geometry"]
    gdf = gdf[attrs]

    gdf = gdf.replace({"": np.nan})
    num_col_format = lambda x: str(x).replace(" ", "").replace(",", ".")
    for col in ["RMC", "RMC_LIVE"]:
        gdf[col] = gdf[col].apply(num_col_format)
    col_types = {attr: "float" for attr in attrs[:-1]}
    gdf = gdf.astype(col_types)

    mask = ((gdf["RMC_LIVE"] > 1000) & (gdf["RMC_LIVE"] > gdf["RMC"])) | gdf[
        "RMC_LIVE"
    ].isna()
    gdf["RMC_LIVE"] = np.where(mask, gdf["RMC"], gdf["RMC_LIVE"])

    gdf["inhab_calc"] = gdf["RMC_LIVE"] * residents_in_apartment
    residents_in_house = gdf["inhab_calc"].mean()
    gdf["inhab_calc"] = gdf["inhab_calc"].fillna(residents_in_house)
    gdf["inhab_calc"] = gdf[["INHAB", "inhab_calc"]].max(axis=1)
    gdf["inhab_calc"] = gdf["inhab_calc"].astype(int)

    gdf = gdf[["inhab_calc", "geometry"]].rename(columns={"inhab_calc": "population"})
    dfh3 = gdf.h3.geo_to_h3_aggregate(h3_res, return_geometry=False)
    return dfh3


@task
def estimate_population(fp: str, h3_res: int):
    osm = pyrosm.OSM(fp)
    buildings = osm.get_buildings(custom_filter={'building': ['apartments']})
    buildings["geometry"] = buildings.centroid
    buildings["population"] = AVG_RESIDENTS_IN_HOUSE
    buildings = buildings[["population", "geometry"]]
    buildings = buildings.h3.geo_to_h3_aggregate(h3_res, return_geometry=False)
    return buildings


@task(retries=3, retry_delay_seconds=[10, 20, 40])
def get_osm_data(city: NamedTuple) -> str:
    """Скачиваем данные OSM."""
    local_prefix = "data"
    local_file = f"{city.region}-fed-district-latest.osm.pbf"
    os.system(f"src/osm_get_data.sh {city.region} {local_prefix}")
    return f"{local_prefix}/{local_file}"


@task
def extract_city(osm_dump_path: str, city: NamedTuple):
    local_prefix = osm_dump_path.split('/')[0]
    os.system(f"src/osm_create_geo_extract.sh {local_prefix} {osm_dump_path} {city.name} {city.osm_id}")
    return f"{local_prefix}/{city.name}/{city.name}.osm.pbf"


@task
def filter_geodata(osm_city_data: str, city: NamedTuple):
    city_prefix = osm_city_data.rsplit("/", 1)[0]
    filtered_geo_data = f"{city_prefix}/pois-in-{city.name}.osm.pbf"
    os.system(f"src/osm_tags_filter.sh {city.name} {city_prefix} {filtered_geo_data}")
    return filtered_geo_data


@task
def get_pois(fp: str) -> pd.DataFrame:
    """Находим точки интереса в обработанных данных OSM."""
    osm = pyrosm.OSM(fp)
    with open("data/osm_tags_filter.json") as f:
        custom_filter = json.load(f)
    pois = osm.get_data_by_custom_criteria(custom_filter=custom_filter)
    pois["geometry"] = pois.centroid
    return pois


@task
def calculate_placement_score(pois, h3_res):
    placement_type = pois[["shop", "geometry"]]
    placement_type = placement_type[placement_type["shop"].notna()]
    conditions = {
        100: placement_type["shop"].eq("mall"),
        90: placement_type["shop"].eq("supermarket"),
    }
    placement_type["shop"] = np.select(
        conditions.values(), conditions.keys(), default=0
    )
    placement_type = placement_type.h3.geo_to_h3_aggregate(
        h3_res, "max", return_geometry=False
    )
    placement_type.rename(columns={"shop": "placement"}, inplace=True)
    return placement_type


@task
def count_number_of_pois(pois, h3_res):
    pois_count = pois[["id", "geometry"]]
    pois_count = pois_count.h3.geo_to_h3_aggregate(
        h3_res, "count", return_geometry=False
    )
    pois_count.rename(columns={"id": "pois"}, inplace=True)
    return pois_count


@task
def evaluate_locations(*args) -> geopandas.GeoDataFrame:
    """Объединяем информацию о численности жильцов многоквартирных домов,
    типе объекта возможного размещения и количестве дополнительных источников
    клиентопотока(точек интереса).
    Оцениваем ячейки Н3 как зоны возможного размещения банкомата.
    """
    gdfh3 = pd.concat(args, axis=1).fillna(0).h3.h3_to_geo_boundary()
    criterion_weight = {
        "placement_object_type": 0.3,
        "access_mode": 0.2,
        "population": 0.3,
        "customer_traffic_sources": 0.2,
    }
    gdfh3["placement"] = gdfh3["placement"] * criterion_weight["placement_object_type"]
    gdfh3["population"] = (
        np.where(gdfh3["population"] * 0.007 > 100, 100, gdfh3["population"] * 0.007)
        * criterion_weight["population"]
    )
    gdfh3["pois"] = (
        np.where(gdfh3["pois"] * 1.25 > 100, 100, gdfh3["pois"] * 1.25)
        * criterion_weight["customer_traffic_sources"]
    )
    return gdfh3


@flow(task_runner=ConcurrentTaskRunner)
def main(bucket, city, h3_res):
    """Основной ETL поток"""
    osm_data_path = get_osm_data(city)
    city_geo_extract = extract_city.submit(osm_data_path, city)
    minio_client = create_minio_client()
    if city.apartment_buildings_data:
        download_object_url = minio_client.presigned_get_object(bucket, city.apartment_buildings_data)
        population_size = process_apartment_buildings_data(download_object_url, AVG_RESIDENTS_IN_APARTMENT, h3_res)
    else:
        population_size = estimate_population(f"data/{city.name}/{city.name}.osm.pbf", h3_res)
    minio_client.fget_object(
        bucket, "osm_tags_filter.json", "data/osm_tags_filter.json"
    )
    pois = get_pois(filter_geodata(city_geo_extract, city))
    placement_score = calculate_placement_score(pois, h3_res)
    number_of_pois = count_number_of_pois(pois, h3_res)
    gdfh3 = evaluate_locations(placement_score, population_size, number_of_pois)
    upload_to_minio(minio_client, gdfh3, bucket, f"{city.name}-h3-atm-score.geojson")


@flow
def etl_flow():
    bucket = "atm-location-assessment"
    cities = [
        City("krasnodar", "r7373058", "south", "myhouse_RU-CITY-016_points_matched.geojson"),
        City("novorossiysk", "r1477110", "south", ""),
        City("armavir", "r3476238", "south", ""),
        City("rostov", "r1285772", "south", ""),
    ]
    h3_res = 8
    for city in cities:
        main(bucket, city, h3_res)
        

if __name__ == "__main__":
    etl_flow()

import warnings
warnings.filterwarnings("ignore")

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


City = namedtuple("City", ["name", "osm_id", "region", "apartment_buildings_data"])


@task
def process_apartment_buildings_data(url: str, h3_res: int = 8) -> pd.DataFrame:
    """Считываем данные из хранилища, выполняем обработку для расчёта
    численности жителей многоквартирных домов в ячейке h3.
    """
    gdf = geopandas.read_file(url)
    attrs = ["RMC", "RMC_LIVE", "INHAB", "AREA", "AREA_LIVE", "geometry"]
    gdf = gdf[attrs]

    gdf = gdf.replace({"": np.nan})
    num_col_format = lambda x: str(x).replace(" ", "").replace(",", ".")
    for col in ["RMC", "RMC_LIVE", "AREA", "AREA_LIVE"]:
        gdf[col] = gdf[col].apply(num_col_format)
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


@task(retries=3, retry_delay_seconds=[10, 20, 40])
def get_osm_data(city: NamedTuple) -> str:
    """Скачиваем данные OSM."""
    local_prefix = "data"
    local_file = f"{city.region}-fed-district-latest.osm.pbf"
    os.system(f"src/get_osm_data.sh {city.region} {local_prefix} {local_file}")
    return (f"{local_prefix}/{local_file}", city)


@task
def extract_and_filter_geodata(osm_dump_path: str, city: NamedTuple) -> str:
    """Из дампа вырезаем нужный город и фильтруем объекты карты по тэгу."""
    osm_data_path = f"data/pois-in-{city.name}.osm.pbf"
    os.system(
        f"src/process_geo_data.sh {osm_dump_path} {city.name} {city.osm_id} {osm_data_path}"
    )
    return osm_data_path


@task
def get_pois(fp: str, h3_res: int = 8) -> pd.DataFrame:
    """Находим точки интереса в обработанных данных OSM.
    Индексируем точки ячейками h3.
    """
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


@task
def evaluate_locations(*args, group: int, mode: int) -> geopandas.GeoDataFrame:
    """Объединяем информацию о численности жильцов многоквартирных домов и
    количестве источников клиентопотока(точек интереса).
    Оцениваем ячейки Н3 как зоны возможного размещения банкомата.
    """
    gdfh3 = pd.concat(args, axis=1).fillna(0).h3.h3_to_geo_boundary()

    placement_object_type = {1: 100, 2: 90, 3: 0}
    access_mode = {24: 100, 23: 90, 19: 38}
    criterion_weight = {
        "placement_object_type": 0.3,
        "access_mode": 0.2,
        "population": 0.3,
        "customer_traffic_sources": 0.2,
    }
    gdfh3["placement_object_type"] = (
        placement_object_type[group] * criterion_weight["placement_object_type"]
    )
    gdfh3["access_mode"] = access_mode[mode] * criterion_weight["access_mode"]
    gdfh3["population"] = (
        np.where(gdfh3["population"] * 0.007 > 100, 100, gdfh3["population"] * 0.007)
        * criterion_weight["population"]
    )
    gdfh3["customer_traffic_sources"] = (
        np.where(gdfh3["pois"] * 1.25 > 100, 100, gdfh3["pois"] * 1.25)
        * criterion_weight["customer_traffic_sources"]
    )
    gdfh3["location_score"] = (
        gdfh3["placement_object_type"]
        + gdfh3["access_mode"]
        + gdfh3["population"]
        + gdfh3["customer_traffic_sources"]
    )
    return gdfh3


@flow(task_runner=ConcurrentTaskRunner)
def main(bucket, city, h3_res):
    minio_client = create_minio_client()
    download_object_url = minio_client.presigned_get_object(
        bucket, city.apartment_buildings_data
    )
    population_size = process_apartment_buildings_data.submit(
        download_object_url, h3_res
    )
    osm_data_path = extract_and_filter_geodata(*get_osm_data(city))
    minio_client.fget_object(
        bucket, "osm_tags_filter.json", "data/osm_tags_filter.json"
    )
    pois = get_pois(osm_data_path, h3_res)

    gdfh3 = evaluate_locations(population_size, pois, group=2, mode=23)
    upload_to_minio(minio_client, gdfh3, bucket, "krd-h3-atm-score.geojson")


@flow
def etl_flow():
    bucket = "atm-location-assessment"
    krd = City(
        "krasnodar", "r7373058", "south", "myhouse_RU-CITY-016_points_matched.geojson"
    )
    h3_res = 8

    main(bucket, krd, h3_res)


if __name__ == "__main__":
    etl_flow()

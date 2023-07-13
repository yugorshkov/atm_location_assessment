import geopandas
import h3pandas
import numpy as np
import os
import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from pathlib import Path


def process_apartment_buildings_data(data_url: str, h3_res: int = 8) -> pd.DataFrame:
    gdf = geopandas.read_file(data_url)
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

    gdf = gdf[["inhab_calc", "geometry"]]
    dfh3 = gdf.h3.geo_to_h3_aggregate(h3_res, return_geometry=False)
    return dfh3


def func():
    pass


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

    region, city_name, osm_city_id = 'south', 'krasnodar', 'r7373058'
    osm_data_path = f"data/interim/pois-in-{city_name}.osm.pbf"
    os.system(f"src/get_osm_data {region} {city_name} {osm_city_id} {osm_data_path}")


if __name__ == "__main__":
    main()

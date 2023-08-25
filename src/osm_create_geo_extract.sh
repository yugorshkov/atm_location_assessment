#!/bin/bash
local_prefix=$1
region_geodata_path=$2
city_name=$3
city_id=$4

city_prefix=$local_prefix/$city_name
city_boundary=$city_prefix/$city_name-boundary.osm
osm_city_data=$city_prefix/$city_name.osm.pbf

mkdir -p $city_prefix
# извлекаем данные для заданного города 
echo "Обработка данных OSM..."
osmium getid -r -t $region_geodata_path $city_id -o $city_boundary
osmium extract -p $city_boundary $region_geodata_path -o $osm_city_data

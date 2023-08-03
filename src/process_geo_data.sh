#!/bin/bash
local_path=$1
city_name=$2
city_id=$3
city_pois=$4

city_boundary=data/$city_name-boundary.osm
osm_city_data=data/$city_name.osm.pbf

# извлекаем данные для заданного города 
echo "Обработка данных OSM..."
osmium getid -r -t $local_path $city_id -o $city_boundary
osmium extract -p $city_boundary $local_path -o $osm_city_data
# отбираем необходимые объекты
osmium tags-filter $osm_city_data \
    amenity \
    building=train_station,office,industrial,university,retail \
    shop=mall,supermarket,convenience \
    highway=bus_stop railway=tram_stop aeroway=terminal \
    -o $city_pois

#!/bin/bash
city_name=$1
city_prefix=$2
city_pois=$3

osm_city_data=$city_prefix/$city_name.osm.pbf

# отбираем необходимые объекты
osmium tags-filter $osm_city_data \
    amenity \
    building=train_station,office,industrial,university,retail \
    shop=mall,supermarket,convenience \
    highway=bus_stop railway=tram_stop aeroway=terminal \
    -o $city_pois

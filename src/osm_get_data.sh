#!/bin/bash
region=$1
local_prefix=$2

url="http://download.geofabrik.de/russia/$region-fed-district-latest.osm.pbf"
# создаем рабочую папку
mkdir -p $local_prefix
# скачиваем файл карт
wget $url -P $local_prefix -N

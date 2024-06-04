#!/bin/bash

years=("2023")
for year in "${years[@]}"
do
	for month in {01..12}
	do
		python3 main.py $year $month
	done
done



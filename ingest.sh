#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate snowj


# Define the list of configuration files
configs=("01-concepts-p1.yml" "01-concepts-p2.yml" "01-concepts-p3.yml" "01-concepts-p4.yml"
          "02-hosp-00-02.yml" "03-hosp-03-07.yml"
          "07-hosp-11-12.yml")

# Iterate through each configuration file and execute the Python command
for config in "${configs[@]}"; do
    python pyingest.py "$config" 2>&1
done



#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate snowj


# Define the list of configuration files
#configs=("01-concepts-p1.yml" "01-concepts-p2.yml" "01-concepts-p3.yml" "01-concepts-p4.yml"
#          "02-hosp-00-02-p2.yml" "03-hosp-03-07.yml"
#          "10-hosp-15-15-p1.yml" "10-hosp-15-15-p2.yml")

#configs=("01-concepts-p1.yml" "01-concepts-p2.yml" "01-concepts-p3.yml" "01-concepts-p4.yml"
#          "02-hosp-00-02-p2.yml"
#          "10-hosp-15-15-p1.yml" "10-hosp-15-15-p2.yml")

configs=( #"01-concepts-p1.yml" "01-concepts-p2.yml" "01-concepts-p3.yml" "01-concepts-p4.yml" "01-concepts-p5.yml" "01-concepts-p6.yml"
          #"02-hosp-00-02-p1.yml" "02-hosp-00-02-p2.yml"
          #"03-hosp-03-07.yml"
          "05-hosp-09-09-p1.yml"
          "05-hosp-09-09-p2.yml"
          #"06-hosp-10-10-p1.yml" "06-hosp-10-10-p2.yml"
          #"07-hosp-11-12.yml"
          #"08-hosp-13-13-p1.yml" "08-hosp-13-13-p2.yml"
          #"09-hosp-14-14.yml"
          #"10-hosp-15-15-p1.yml" "10-hosp-15-15-p2.yml"
)



# Iterate through each configuration file and execute the Python command
for config in "${configs[@]}"; do
    python pyingest.py "$config" 2>&1
done



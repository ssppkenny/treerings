#!/bin/bash

for d in ./*/
do
     cd $d
     echo $d
     zip trees trees.csv
     rm trees.csv
     cd ..
done

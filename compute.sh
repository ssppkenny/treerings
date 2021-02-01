#!/bin/bash

for d in ./*/
do
     cd $d
     echo $d
     cat header.csv $(ls | grep -v 'header.csv') > trees.csv
     find . -type f ! -name 'trees.csv' -delete
     cd ..
done

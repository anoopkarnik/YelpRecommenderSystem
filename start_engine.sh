#!/bin/sh
cd app
bash /home/anoop/yelp_recommender/spark/bin/spark-submit --master spark://anoop-VirtualBox:7077 --driver-memory 8g --executor-memory 8g --driver-cores 2 server.py
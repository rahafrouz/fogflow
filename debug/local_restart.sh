#!/bin/bash

echo "this is local, nothing is pushed to the dockerhub"
#kill fogflow
docker kill $(docker ps -q)


#rebuild  the master

#echo "local rebuiling master..."
#cd ../master
#./build-local


#rebuild the worker

#echo "rebuilding worker"
#cd ../worker
#./build-local


cd sublogger
./build

cd ../local
docker-compose up -d 

cd ../edge
docker-compose up -d


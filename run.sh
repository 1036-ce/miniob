#!/bin/bash

# ./build/bin/observer -f ./etc/observer.ini -P cli
# ./build/bin/observer -f ./etc/observer.ini -P port

cd ann-benchmarks
python3 run.py --dataset fashion-mnist-784-euclidean --docker-tag ann-benchmarks-miniob --local --timeout 100 --runs 1

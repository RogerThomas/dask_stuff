#!/bin/bash

pkill -9 dask-scheduler
pkill -9 dask-worker
dask-scheduler > scheduler.log 2>&1 &
dask-worker 127.0.0.1:8786 --nprocs $1 --nthreads=2 --memory-limit=$2 > worker.log 2>&1 &

#!/bin/bash

pkill -9 dask-scheduler
pkill -9 dask-worker
dask-scheduler > scheduler.log 2>&1 &
dask-worker 127.0.0.1:8786 --nprocs $1 --nthreads=1 > worker.log 2>&1 &

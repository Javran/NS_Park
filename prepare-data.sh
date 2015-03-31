#!/bin/bash

mkdir -p data

pushd data
curl 'http://snap.stanford.edu/data/facebook.tar.gz' -o 'facebook.tar.gz' -L
tar -xf 'facebook.tar.gz'

popd

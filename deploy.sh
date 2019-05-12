#!/bin/bash

cd packages
gcloud functions deploy packages --runtime nodejs8 --trigger-topic packages --timeout 240
cd ../tarballs
gcloud functions deploy tarballs --runtime nodejs8 --trigger-topic tarballs --timeout 240
cd ../

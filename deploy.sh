#!/bin/bash

cd packages
gcloud functions deploy packages --runtime nodejs8 --trigger-topic packages
cd ../tarballs
gcloud functions deploy tarballs --runtime nodejs8 --trigger-topic tarballs
cd ../

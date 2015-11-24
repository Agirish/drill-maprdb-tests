#!/bin/sh

wget -P data https://s3.amazonaws.com/apache-drill/files/yelp.tgz

cd data
tar -xf yelp.tgz

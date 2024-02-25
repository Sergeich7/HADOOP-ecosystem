#! /usr/bin/env bash

#sed -e "s/\.\w\+\//\.com\//"
sed -e "s/\.\w\+\//\.com\//" -e "s/ .*//g"

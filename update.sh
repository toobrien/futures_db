#!/bin/bash

#python update.py $@ --sources cme_latest cboe_latest --clean 

python update.py $@ --sources cme_latest cboe_latest eia_all --clean 

#!/usr/bin/python

import sys
import pygeoip

gi = pygeoip.GeoIP("../big-data/GeoLiteCity.dat")

@outputSchema('city:chararray')
def get_city(ip):
    try:
        city = gi.record_by_name(ip)["city"]
        return city
    except:
        pass


@outputSchema('country:chararray')
def get_country(ip):
    try:
        city = gi.record_by_name(ip)["country_name"]
        return city
    except:
        pass

@outputSchema('location:chararray')
def get_geo(ip):
    try:
        geo = str(gi.record_by_name(ip)["longitude"]) + "," + str(gi.record_by_name(ip)["latitude"])
        return geo
    except:
        pass

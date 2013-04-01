"""
This file imports a ton of records from a Socrata SODA
API page. It was created due to extremely slow calls to
the .csv and .json files (I assume the server was trying
to compile a shitload of data into one HTTP response).
"""

import requests, json
import sys, time, os

HEADERS = {"X-App-Token": "iCFkWy9YYXGzULbBVVfhoUCSq"} #Dont steal me plx <3
URL = sys.argv[1]
OUT = sys.argv[2]
DATA = []

def getRequest(i):
    r = requests.get(URL, headers=HEADERS, params={"$limit": 1000, "$offset": 1000*i})
    if r.status_code != 200: return
    return r


for i in xrange(1, 10000000000): #Stupidly high number, without xrange we have a bad day
    print "Getting page #%s" % i
    r = getRequest(i)
    if not r:
        print "Waiting..."
        time.sleep(1)
        r = getRequest(i)
        if not r:
            print "Non-200 code, are we done?, %s, %s" % (i, r.json())
    with open(os.path.join(OUT, "page-%s.json" % i), "w") as f:
        json.dump(r.json(), f)

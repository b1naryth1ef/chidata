#!/usr/bin/env python
"""
Download a full CSV-dump of a City of Chicago Data set
using the SODA API. This script can be used to efficiently
download large (25,000+ row) data sets that would the
data portal otherwise refuses to export.
"""

import os, sys, time, csv, argparse
import requests

URL = "https://data.cityofchicago.org/resource/{}.csv"

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("id", metavar="ID", help="the data portal dataset ID")
parser.add_argument("--token", help="SODA2 API token")
parser.add_argument("--output", help="output file", default="data.csv")
parser.add_argument("--per", help="rows to download per page", type=int, default=25000)
args = vars(parser.parse_args())

def download_page(id, page, token, per):
    for retry in range(3):
        r = requests.get(URL.format(id),
                params={"$limit": per, "$offset": per * (page- 1)},
                headers={"X-App-Token": token})

        if len(r.content) and r.status_code == requests.codes.ok:
            return r.content.split("\n")[0:-1]

        time.sleep(1)

    raise Exception("Failed to download page {}: {}".format(page, r.content))

def download(output, id, token, per):
    print "Starting download..."

    # SODA doesn't support finding how many pages exist, so we just brute force it
    for page in xrange(1, 10**10):
        sys.stdout.write("\r  downloading page {}".format(page))
        sys.stdout.flush()

        data = download_page(id, page, token, per)

        # If we only get the header back, we have no more data
        if len(data) == 1:
            break

        # If we're at the first page, write our headers
        if page == 1:
            output.write(data[0] + '\n')

        output.write('\n'.join(data[1:]) + '\n')

    print "\n Downloaded {} pages!".format(page - 1)

def main():
    token = args['token'] or os.getenv("SODA2_TOKEN")
    if not token:
        print "ERROR: --token option not provided and SODA2_TOKEN environment variable unset!"
        return sys.exit(1)

    with open(args['output'], "w+") as output:
        download(output, args['id'], token, args['per'])

    return 0

if __name__ == "__main__":
    sys.exit(main())


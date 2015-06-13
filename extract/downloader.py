#!/usr/bin/env python
"""
Download a full-size CSV file for a City of Chicago data set using the SODA2 API.
This script can be used to efficiently and quickly download extremely large data
sets that would otherwise be impossible to export through the City's data portal
website.
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
    """
    Downloads a page. Will retry 3 times before failing. Returns the header and
    page content on success, or None if the data returned is empty.
    """
    for retry in range(3):
        r = requests.get(URL.format(id),
                params={"$limit": per, "$offset": per * (page- 1)},
                headers={"X-App-Token": token})

        if len(r.content) and r.status_code == requests.codes.ok:
            data = r.content.split("\n", 1)
            if len(data) < 1:
                return None, None

            return tuple(data)

        time.sleep(1)

    raise Exception("Failed to download page {}: {}".format(page, r.content))

def download(output, id, token, per):
    print "Starting download..."

    # SODA doesn't support finding how many pages exist, so we just brute force it
    for page in xrange(1, 10**10):
        sys.stdout.write("\r  downloading page {}".format(page))
        sys.stdout.flush()

        header, data = download_page(id, page, token, per)

        # If we don't have any data that means we hit the last page
        if not data:
            break

        # If we're at the first page, write our headers
        if page == 1:
            output.write(header + '\n')

        output.write(data)

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


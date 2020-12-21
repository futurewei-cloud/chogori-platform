'''
MIT License

Copyright (c) 2020 Futurewei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import requests, json
from urllib.parse import urlparse
import argparse

parser = argparse.ArgumentParser(description="Utility for accessing K2 API functions")
parser.add_argument("--cpo", help="API URL for CPO")
args = parser.parse_args()

class CPO:
    def __init__(self, cpo_url):
        cpo = urlparse(cpo_url)
        self.cpo = cpo.geturl()

    def getCollection(self, name):
        url = self.cpo + "/api/CollectionGet"
        data = {"name": name}
        r = requests.post(url, data=json.dumps(data))
        return r.json()

    def getSchemas(self, name):
        url = self.cpo + "/api/GetSchemas"
        data = {"collectionName": name}
        r = requests.post(url, data=json.dumps(data))
        return r.json()


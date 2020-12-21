import sys, requests, json
from urllib.parse import urlparse

class CPO:
    def __init__(self):
        i = 0
        found = False
        for arg in sys.argv:
            if arg == "--cpo":
                found = True
                break
            i += 1

        if found:
            cpo = urlparse(sys.argv[i+1])
            self.cpo = cpo.geturl()
        else:
            print("No CPO endpoint set in sysv")

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


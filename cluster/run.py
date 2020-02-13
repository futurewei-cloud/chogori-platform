import parse

runnables = parse.parseConfig("configs/tpcc_cluster.cfg")
for r in runnables:
    print(r)

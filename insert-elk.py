import os
import sys

directory = sys.argv[1]
db_name = sys.argv[2]

for filename in os.listdir(directory):
    if filename.endswith(".log"):
        sts = filename.split(".")
        index = sts[0]
        index_type = index + "Type"
        full_path =  directory + filename
        cmd_index = "curl  -H 'Content-Type: application/x-ndjson' -XPOST 'localhost:9200/{}/brotype/_bulk?pretty' --data-binary @{}  > /dev/null"
        cmd = cmd_index.format(db_name,full_path)
        print (cmd)
        os.system(cmd)

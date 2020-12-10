import requests, json, os, gzip, uuid, shutil, glob, re, tarfile
from elasticsearch import Elasticsearch, helpers
from pathlib import Path
from collections import deque

from bro2json import processFile
            


res = requests.get('http://localhost:9200')
print (res.content)

def delete_index(idx):
    response = es.indices.delete(index=idx, ignore=[400, 404])
    print ("\n Delete RESPONSE: ", response)
    
def clear_directory(path):
    files = glob.glob(path+'/*')
    for f in files:
        os.remove(f)

def unzip_logs(path):
    in_path = str(path)
    output_path = '/shared/projects/stagging/'
    clear_directory('/shared/projects/stagging/')
    print("In file = " + in_path)
    t = tarfile.open(in_path, 'r:gz')
    t.extractall(output_path)
    t.close()
    print("Done writing output files ")
    return output_path

def bulk_logs(path):
    pattern_uuid = re.compile(r'uuid\":\"(.*?)\"')
    for unzip_path in Path(path).rglob('*.json*'):
        with open(unzip_path, 'r') as f:
            print("Working on file: " + str(f))     
            for line in f:
                docket_content = line.strip()
                json_docs = json.loads(docket_content)
                uuid = ""
                if len(pattern_uuid.findall(line)) > 0:
                    uuid = pattern_uuid.findall(line)[0]
                else:
                    continue
                yield uuid, json_docs

def es_add_bulk(es, path, idx):

    k = ({
        "_index": idx,
        "_id"   : did,
        "_source": data,
    } for did, data in bulk_logs(path))

    response = helpers.parallel_bulk(es, k, chunk_size=10000, thread_count=6, queue_size=6)
    deque(response, maxlen=0)
        
    print ("\n Bulk RESPONSE: ", response)

def load_ecar(day, idx):
    delete_index(idx)
    directory = "/shared/projects/2018-darpa-tc-engagement3/data/" + day + "/"
    print(directory)
    for zip_path in Path(directory).rglob('*.json.tar.gz'):
        output_path = unzip_logs(zip_path)
        es_add_bulk(es,output_path,idx)
        clear_directory(output_path)
        print("Done indexing ................ \n\n")

def send_data_yield(contents, shortname):
    i = 1
    for content in contents:
        yield str(i)+shortname[:-4], content
        i = i + 1


if __name__ == '__main__':
    es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200}], timeout=60, max_retries=5, retry_on_timeout=True)

    days  = ["trace", "cadets-complete", "clearscope-complete", "fivedirections-complete", "theia-complete"]
    
    for day in days:
        idx =  day.lower()
        load_ecar(day, idx)

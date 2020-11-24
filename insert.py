import requests, json, os, gzip, uuid, shutil, glob
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
    output_path = '/shared/projects/stagging/' + path.name[:-3]
    clear_directory('/shared/projects/stagging/')
    print("In file = " + in_path)
    with gzip.open(in_path, 'rt', encoding='utf-8') as f_in:
        with open(output_path, 'w') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("Done writing output file = " + output_path)
    return output_path

def bulk_logs(path):
    with open(path, 'r') as f:
        for line in f:
            docket_content = line.strip()
            json_docs = json.loads(docket_content)
            object = json_docs["object"]
            if (object == "PROCESS") or  (object == "THREAD") or (object == "FLOW"):
                yield json_docs["id"], json_docs

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
    directory = "/shared/projects/Optc-dataset/optc-dataset/ecar/evaluation/" + day[0] + "/"

    for zip_path in Path(directory).rglob('*.gz'):
        unzip_path = unzip_logs(zip_path)
        es_add_bulk(es,unzip_path,idx)
        os.remove(unzip_path)
        print("Done indexing: " + unzip_path)

def load_ecar_bro(day, idx):
    directory = "/shared/projects/Optc-dataset/optc-dataset/ecar-bro/evaluation/" + day[0] + "/"

    for zip_path in Path(directory).rglob('*.gz'):
        unzip_path = unzip_logs(zip_path)
        es_add_bulk(es,unzip_path,idx)
        os.remove(unzip_path)
        print("Done indexing: " + unzip_path)

def send_data_yield(contents, shortname):
    i = 1
    for content in contents:
        yield str(i)+shortname[:-4], content
        i = i + 1

def load_bro(day, idx):
    delete_index(idx)
    
    directory = "/shared/projects/Optc-dataset/optc-dataset/bro/" + day[0] + "/"
    print("directory "  + directory)
    for zip_path in Path(directory).rglob('*.log'):
        shortname = os.path.basename(zip_path)
        file_contents = processFile(zip_path)
        
        if len(file_contents) <= 0:
            print ("file size size is zero")
            continue
        
        k = ({
        "_index": idx,
        "_id"   : did,
        "_source": data,
        } for did, data in send_data_yield(file_contents, shortname))

        response = helpers.parallel_bulk(es, k, chunk_size=10000, thread_count=6, queue_size=6)
        deque(response, maxlen=0)
        
        print ("\n Bulk RESPONSE: ", response)




if __name__ == '__main__':
    es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200}], timeout=60, max_retries=5, retry_on_timeout=True)

    day = ["25Sept"]
    bro_day = ["2019-09-25"]
    idx =  day[0].lower()

    # load_ecar(day, idx)
    # load_ecar_bro(day, idx)
    load_bro(bro_day, idx+"bro")
    
    

    


from pyquery import PyQuery as pq
import aiohttp
import asyncio
import json
import motor.motor_asyncio
import time
import logging
logging.basicConfig(filename = 'jobs_ch_log.txt', level = logging.DEBUG, filemode = 'w')
from bson.json_util import loads
import multiprocessing
import time
from helpers import get_page, get_meta_q, make_tasks_and_exc
from pymongo import MongoClient
import re
from pprint import pprint

def get_key(val, d):
    for key, value in d.items():
        if val == value:
            return key

async def get_dat(qstr):
    loc_dict = {'Zürich':'zh','Berne':'be','Lucerne':'lu','Uri':'ur','Schwyz':'sz','Obwalden':'ow','Nidwalden':'nw',
                'Glarus':'gl','Zug':'zg','Fribourg':'fr','Solothurn':'so','Basel Stadt':'bs','Basel Landschaft':'bl',
                'Schaffhausen':'sh','Appenzell Ausserrhoden':'ar' ,'Appenzell Innerrhoden':'ai','St. Gallen':'sg',
                'Grisons':'gr','Aargau':'ag','Thurgau':'tg','Ticino':'ti','Vaud':'vd','Valais':'vs','Neuchâtel':'ne',
                'Geneva':'ge','Jura':'ju'}
    #print(loc_dict['Berne'])
    client = motor.motor_asyncio.AsyncIOMotorClient()
    harvests_db = client['job_ch']
    consul1_coll = harvests_db['data']
    urls = []
    for i in list(loc_dict.keys()):
        urls.append('https://www.jobs.ch/en/salary/?canton='+loc_dict[i]+'&term='+str(('%20').join(qstr['name_display'].split(' '))))
    #print(urls)
    for i in urls:
        json_res = await get_page(i)
        #print(json_res)
        if (not json_res):
            logging.warning(f'No results from {url}, payload :{payload}')
            continue
        # json_sp = loads(json_res)
        """f = open('new4.html','w')
        f.write(json_res)
        f.close()"""
        # print(type(json_res))
        m = re.findall(r'(?<=\"result\":){\"min\".*(?=},\"aggregationCanton\")', json_res)
        #print(m)
        if(not m):
            continue
        sal_str = m[0]
        sal_dict = json.loads(sal_str)
        #print(sal_dict)
        #print(i)
        location_code = re.findall(r'(?<=canton=).*(?=&term=)', i)
        if(not location_code):
            continue
        #print(location_code)
        location = get_key(location_code[0], loc_dict)
        if(not location):
            continue
        #print(location)
        #print(qstr)
        #print(location_code[0]+qstr['name_display'])
        master_dict = {'custom_id': location_code[0] + qstr['name_display'], 'salaries': sal_dict, 'location': location,
                       'location_val': location_code[0], 'Job Role': qstr['name_display'], 'meta1': qstr}
        #pprint(master_dict)
        logging.info(f'DB Op:Inserting Job w/ ID:{qstr["id"]}')
        await consul1_coll.find_one_and_update({'custom_id': master_dict['custom_id']}, {'$set': master_dict}, upsert=True)
        # print(master_dict['custom_id'])'''
    return


def gen_query_string(query_q):
    client = MongoClient()
    db = client['job_ch']
    working_coll = db['consul1a']
    res = list(working_coll.find({}))
    print(len(res))
    for i in res:
        query_q.put(i)
    return query_q


def kw_driver(process_queue_size, query_q):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks_and_exc(query_q, process_queue_size, 5, get_dat))

def gen_query_and_exec(no_processes):
    query_q = multiprocessing.Manager().Queue()
    query_q = gen_query_string(query_q)
    logging.info(f'Query Queue Size :{(query_q.qsize())}')
    process_queue_size = (query_q.qsize() // no_processes)
    temp = query_q.qsize()
    p_queues_size = []
    for i in range(no_processes - 1):
        p_queues_size.append(process_queue_size)
        temp -= process_queue_size
    p_queues_size.append(temp)
    print(p_queues_size)
    with multiprocessing.Pool(no_processes) as p:
        logging.info('Initiating multi-pool workers')
        multi = [p.apply_async(kw_driver, (p_queues_size[i], query_q,)) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()
    #await asyncio_execute(query_q)

if __name__=='__main__':
    PROCESSES = 3
    start = time.time()
    gen_query_and_exec(PROCESSES)
    end = time.time()
    print(f'Execution Time:{str(end-start)} seconds')
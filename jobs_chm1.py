"""To get meta-data1, using string manipulations"""
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
from helpers import get_page, make_tasks_and_exc

async def get_meta1(qstr):
    #print(qstr)
    #print("here!!")
    url = 'https://www.jobs.ch/api/v1/public/meta/typeahead?limit=2000&query='+str(qstr)
    json_res = await get_page(url, headers={"Accept-Language": "en-US,en;q=0.5"})
    #print(json_res)
    if(not json_res):
        logging.warning(f'No results from {url}, payload :{payload}')
        return
    #json_sp = loads(json_res)
    json_sp = json.loads(json_res)
    client = motor.motor_asyncio.AsyncIOMotorClient()
    harvests_db = client['job_ch']
    consul1_coll = harvests_db['consul1a']
    for j in json_sp:
        logging.info(f'DB Op:Inserting Job w/ ID:{j["id"]}')
        await consul1_coll.find_one_and_update({'id':j['id']}, {'$set':j}, upsert=True)
        #print(j['id'])
    return

def gen_query_string(query_q):
    al_list = [chr(x) for x in range(ord('a'), ord('z') + 1)]
    for i in al_list:
        query_q.put(i)
    return query_q

def kw_driver(process_queue_size, query_q):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks_and_exc(query_q, process_queue_size, 30, get_meta1))

def gen_query_and_exec(no_processes):
    query_q = multiprocessing.Manager().Queue()
    query_q = gen_query_string(query_q)
    print(query_q.qsize())
    logging.info(f'Query Queue Size :{(query_q.qsize())}')
    process_queue_size = (query_q.qsize()//no_processes)
    temp = query_q.qsize()
    p_queues_size = []
    for i in range(no_processes-1):
        p_queues_size.append(process_queue_size)
        temp -= process_queue_size
    p_queues_size.append(temp)
    print(p_queues_size)
    with multiprocessing.Pool(no_processes) as p:
        logging.info('Initiating multi-pool workers')
        multi = [p.apply_async(kw_driver, (p_queues_size[i], query_q, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()
    #await asyncio_execute(query_q)

if __name__=='__main__':
    PROCESSES = 4
    start = time.time()
    gen_query_and_exec(PROCESSES)
    end = time.time()
    print(f'Execution Time:{str(end-start)} seconds')
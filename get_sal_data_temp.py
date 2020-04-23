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
from helpers import *
from pymongo import MongoClient
import re

async def get_dat(query_q):
    if (query_q.empty()):
        return
    qstr = await query_q.get()

    loc_dict = {'Zürich':'zh',
    'Berne':'be',
    'Lucerne':'lu',
    'Uri':'ur',
    'Schwyz':'sz',
    'Obwalden':'ow',
    'Nidwalden':'nw',
    'Glarus':'gl',
    'Zug':'zg',
    'Fribourg':'fr',
    'Solothurn':'so',
    'Basel Stadt':'bs',
    'Basel Landschaft':'bl',
    'Schaffhausen':'sh',
    'Appenzell Ausserrhoden':'ar' ,
    'Appenzell Innerrhoden':'ai',
    'St. Gallen':'sg',
    'Grisons':'gr',
    'Aargau':'ag',
    'Thurgau':'tg',
    'Ticino':'ti',
    'Vaud':'vd',
    'Valais':'vs',
    'Neuchâtel':'ne',
    'Geneva':'ge',
    'Jura':'ju'}
    #print(qstr)
    for i in list(loc_dict.keys()):
        url = 'https://www.jobs.ch/en/salary/?canton='+loc_dict[i]+'&term='+str(qstr['name_display'])
        #print(loc_dict[i])
        #print(qstr['name_display'])
        json_res = await get_page(url)
        #print(json_res)
        """if(not json_res):
            logging.warning(f'No results from {url}, payload :{payload}')
            return"""
        #json_sp = loads(json_res)
        """f = open('new4.html','w')
        f.write(json_res)
        f.close()"""
        m = re.search(r"(? <= \"result\":){\"min\".*(?=},\"aggregationCanton\")", json_res)
        if(m is None):
            continue
        sal_str = m.string[m.start():m.end()]
        #print(sal_str)
        sal_dict = dict(sal_str)
        master_dict = {'custom_id':loc_dict[i]+qstr['name_display'], 'salaries':sal_dict,'location':i,'location_val':loc_dict[i], 'Job Role':qstr['name_display'], 'meta1':qstr}
        client = motor.motor_asyncio.AsyncIOMotorClient()
        harvests_db = client['job_ch']
        consul1_coll = harvests_db['data']
        logging.info(f'DB Op:Inserting Job w/ ID:{j["id"]}')
        await consul1_coll.find_one_and_update({'custom_id':master_dict['custom_id']}, {'$set':j}, upsert=True)
        #print(master_dict['custom_id'])


def gen_query_string(query_q):
    client = MongoClient()
    db = client['job_ch']
    working_coll = db['consul1a']
    res = list(working_coll.find({}))
    print(len(res))
    return res[:120]

async def make_tasks_exc(meta1_queue, process_queue_size, div_factor, func):
    search_queue = asyncio.Queue()
    if(meta1_queue==[]):
        return
    for i in meta1_queue:
        await search_queue.put(i)
    print(search_queue.qsize())
    logging.info(f'Initiated async queues of {process_queue_size}')
    logging.info(f'Worker async queue size:{search_queue.qsize()}')
    #print(search_queue.qsize())
    tasks = []
    times = search_queue.qsize() // div_factor + 1
    for _ in range(times + 1):
        await asyncio.sleep(0.2)
        logging.info(f'Initiating {div_factor} batch tasks')
        for i in range(div_factor):
            #print('Making Task')
            task = asyncio.Task(func(search_queue))
            tasks.append(task)
        await asyncio.gather(*tasks)


def kw_driver34(query_q):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks_exc(query_q, 3, get_dat))

def gen_query_and_exec(no_processes):
    query_q = multiprocessing.Manager().Queue()
    query_q = gen_query_string(query_q)
    print(len(query_q))
    #logging.info(f'Query Queue Size :{(query_q.qsize())}')
    process_queue_size = (len(query_q)//no_processes) + 1
    with multiprocessing.Pool(no_processes) as p:
        logging.info('Initiating multi-pool workers')
        multi = [p.apply_async(kw_driver34, (query_q[i*process_queue_size:(i+1)*process_queue_size], )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()
    #await asyncio_execute(query_q)

if __name__=='__main__':
    PROCESSES = 14
    start = time.time()
    gen_query_and_exec(PROCESSES)
    end = time.time()
    print(f'Execution Time:{str(end-start)} seconds')
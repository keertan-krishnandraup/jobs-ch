import asyncio
import aiohttp
import logging
import motor.motor_asyncio
logging.basicConfig(filename='helper_log.txt', filemode='w', level=logging.DEBUG)
import multiprocessing

async def get_page(href='',proxy=None,redo=0,request_type='GET', headers=None):
    async with aiohttp.ClientSession() as client:
        logging.info('Hitting API Url : {0}'.format(href))
        response = await  client.request('{}'.format(request_type), href, proxy=proxy, headers=headers)
        logging.info('Status for {} : {}'.format(href,response.status))
        if response.status!= 200 and redo < 10:
            redo = redo + 1
            logging.warning("Response Code:" + str(response.status) +"received")
            return await get_page(href=href,proxy=None, redo=redo)
        else:
            return await response.text()

async def get_meta_q(db_name, coll_name):
    client = motor.motor_asyncio.AsyncIOMotorClient()
    harvests_db = client[db_name]
    meta1_coll = harvests_db[coll_name]
    meta1_queue = asyncio.Queue()
    res = await meta1_coll.find({})
    for i in list(res):
        await meta1_queue.put(i)
    return meta1_queue


async def make_tasks_and_exc(meta1_queue, process_queue_size, batch_size, func):
    search_queue = asyncio.Queue()
    for i in range(process_queue_size):
        if(not meta1_queue.empty()):
            await search_queue.put(meta1_queue.get())
    #print(search_queue.qsize())
    logging.info(f'Initiated async queues of {process_queue_size}')
    logging.info(f'Worker async queue size:{search_queue.qsize()}')
    #print(search_queue.qsize())
    counter = search_queue.qsize()
    print(f'Counter:{counter}')
    while(counter>0):
        await asyncio.sleep(0.2)
        if(counter>batch_size):
            num = batch_size
            counter -= batch_size
        else:
            num = counter
            counter = 0
        print(f'Counter:{counter}')
        logging.info(f'Initiating {num} batch tasks')
        tasks = []
        for i in range(num):
            if(not search_queue.empty()):
                item = await search_queue.get()
                #print(item)
                task = asyncio.Task(func(item))
                tasks.append(task)
        print(f'task len:{len(tasks)}')
        await asyncio.gather(*tasks)
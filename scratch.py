def test(counter, batch_size):
    while(counter>0):
            if(counter>batch_size):
                num = batch_size
                counter -= batch_size
            else:
                num = counter
                counter = 0
            print(f'Counter:{counter}')
            ##logging.info(f'Initiating {num} batch tasks')
            tasks = []
            j = 0
            for i in range(num):
                #item = await search_queue.get()
                #print(item)
                #task = asyncio.Task(func(item))
                task = j
                j += 1
                tasks.append(task)
            print(tasks)
            print(f'task len:{len(tasks)}')

test(40, 30)
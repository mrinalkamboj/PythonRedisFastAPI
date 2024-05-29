## Fast API
import random
from typing import Optional,List
from fastapi import APIRouter,Query

from redisfuncs import set_redis_key, get_redis_key, list_redis_keys, delete_redis_key, delete_all_redis_keys, acquire_redis_lock, release_redis_lock, incr_redis_key, decr_redis_key,async_redis_wrapper,set_redis_keys,generate_unique_tracking_id, get_hash_set,get_hash_set_key,get_hash_set_keys,set_hash_set,set_hash_set_keys

## Constants
from const import lock_suffix,pending,executing,success,failed,retrial1,retrial2,starting,rate_limit_suffix,redis_lock_ttl_ms,current_executing_suffix,all_banners_default_rate_limit,pending_suffix

## Asyncio
import asyncio

## Time
from time import time as et
from datetime import datetime as dt
from datetime import timedelta as tdelta


# Create an instance of the FastAPI class
router = APIRouter()

## Redis Entry function to set and get a key
@router.get("/redis/{key}/{value}")
async def entry(key:str,value:str):
    await async_redis_wrapper(set_redis_key, key, value)
    return await async_redis_wrapper(get_redis_key, key)

@router.get("/sf")
async def ada_visual_sf(banner:str,weeks:List[str]=Query(None)):
    ## Execution Begin Time
    start = dt.now()
    ## Generate a Unique Tracking Id for a Save and Finalise operation, which will be used to track all sub qeuries
    tid = await async_redis_wrapper(generate_unique_tracking_id)
    print(f"Tracking Id : {tid}")

    ## For a given tracking id, set the status of each week as "starting"
    week_tracking_dict = {w:starting for w in weeks}
    await async_redis_wrapper(set_hash_set_keys,tid,week_tracking_dict)
    
    ## Segregate and Generate Query Ids
    queryarr = [ada_visual_sf_execute(banner,tid,week) for week in weeks]

    ## Execute all queries asynchronously
    await asyncio.gather(*queryarr)
    ## Execution End Time
    end = dt.now()
    elapsed  = end - start
    elapsed_seconds = int(elapsed.total_seconds())
    print(f"*************************")
    print(f"Start Time : {start.strftime('%H:%M:%S')}")
    print(f"End Time : {end.strftime('%H:%M:%S')}")
    print(f"Elapsed Time : {elapsed_seconds}")
    print(f"*************************")

## Async Function to execute the Save and Finalise operation
async def ada_visual_sf_execute(banner:str,tid:str,qid:str,status=starting):
    
    ## Acquire the distributed lock using redis
    print("Acquiring Redis Lock !!")

    lock  = await async_redis_wrapper(acquire_redis_lock, banner+lock_suffix,redis_lock_ttl_ms)

    ## Check whether the valid lock is acquired
    if lock:
        print(f"Acquired Lock : {banner+lock_suffix} for Query : {qid}")
    else:
        print(f"Failed lock : {banner+lock_suffix} for Query : {qid}")
        await ada_visual_sf_execute(banner,tid,qid,status) ## Mandatory Retrial
        return
    
    ## Fetch the Rate Limit Value
    rate_limit = await async_redis_wrapper(get_redis_key,banner+rate_limit_suffix)

    ## set the default rate limit if not set
    if not rate_limit:
        rate_limit = all_banners_default_rate_limit

    current_executing = await async_redis_wrapper(get_redis_key,f"{tid}{current_executing_suffix}")

    query_status =  None

    if not current_executing:
        await async_redis_wrapper(set_redis_key,f"{tid}{current_executing_suffix}",0)
        ## Being the first query set the status of Executing and Proceed
        query_status = executing
    else:
        current_executing = await async_redis_wrapper(incr_redis_key,f"{tid}{current_executing_suffix}")

        if int(current_executing) < int(rate_limit):
            query_status = executing
        else:
            query_status = pending
    
    maintask = None
    if query_status == pending:
        ## Set the Query Status as Pending for Future Execution
        await async_redis_wrapper(set_hash_set,tid,qid,pending)
        ## Increment the Pending Query Count for the Banner
        await async_redis_wrapper(incr_redis_key,f"{tid}{pending_suffix}")
    else:    
        ## Create a Main Task, which is not blcoked or awaited inside the lock, it is awaited after the lock is released
        maintask = asyncio.create_task(main_task(banner,tid,qid,status))

    ## Release the distributed lock
    if lock is not None:
        print("Releasing Lock ...")
        await async_redis_wrapper(release_redis_lock, lock)

    if maintask is not None:
        ## Await the Execution of Main Query
        await maintask

## Main Task to be executed after the lock is released
async def main_task(banner:str,tid:str,qid:str,status):
    try:
        print(f"Processing Query : {qid}")
        ## Set the status of the query for a transaction id as "executing"
        if status == starting:
            status = executing

        await async_redis_wrapper(set_hash_set,tid,qid,status)        
        
        await executing_task(tid,qid)
        ## Set the status of the query for a transaction id as "success"
        await async_redis_wrapper(set_hash_set,tid,qid,success)
        await async_redis_wrapper(decr_redis_key,f"{tid}{current_executing_suffix}")
    except Exception as e:
        print(f"Error in Query : {qid} : {e}")
        ## Fetch the Query Status for a Transaction Id and Initiate Retrial based on the status
        qs = await async_redis_wrapper(get_hash_set_key,tid,qid)
        ## Convert the Bytes Data to String
        qs = str(qs.decode('utf-8'))
        if qs == executing:        
            await asyncio.create_task(main_task(banner,tid,qid,retrial1))
            return
        elif qs == retrial1:
            await async_redis_wrapper(set_hash_set,tid,qid,retrial2)
            await asyncio.create_task(main_task(banner,tid,qid,retrial2))
            return
        else:
            await async_redis_wrapper(set_hash_set,tid,qid,failed)
            ## Retrial 2 failed
            await async_redis_wrapper(decr_redis_key,f"{tid}{current_executing_suffix}")
    finally:
        await start_pending_task(banner,tid)

        

## Task to be executed asynchronously
async def executing_task(tid:str,qid:str):
    # generate a random number between 60 and 100
    random_execution_time = random.randint(10, 60)  
    # if a number is even then execute else raise an exception
    if random_execution_time <= 40:
        await asyncio.sleep(40)
    else:
        raise Exception("Random execution error, please restart !!.")

## Start a Pending Task, which is blocked due to rate limiting
async def start_pending_task(banner:str,tid:str):

    # Create a Redis Lock based on Transaction Id
    lock  = await async_redis_wrapper(acquire_redis_lock, tid+lock_suffix,redis_lock_ttl_ms)

    ## Check whether the valid lock is acquired
    if lock:
        print(f"Acquired Lock : {tid+lock_suffix}")
    else:
        print(f"Failed to acquire lock : {tid+lock_suffix}")
        await start_pending_task(banner,tid) ## Mandatory Retrial
        return
    
    ## Fetch the First Pending Query Id
    qid = await fetch_first_pending_task(tid)
    
    if qid is not None:
        ## Set the pending query status as "executing"
        await async_redis_wrapper(set_hash_set,tid,qid,executing)

        ## Create a Main Task, which is not blcoked or awaited inside the lock, it is awaited after the lock is released
        pendingtask = asyncio.create_task(main_task(banner,tid,qid,executing))

    ## Release the distributed lock
    print("Releasing Lock ...")
    await async_redis_wrapper(release_redis_lock, lock)

    if qid is not None:
        ## Decrease the pending counter for the Transaction Id
        await async_redis_wrapper(decr_redis_key,f"{tid}{pending_suffix}")
        ## Execute and Await the Pending Tasks
        await pendingtask

## Fetch the First Pending Task for a given Transaction Id
async def fetch_first_pending_task(tid:str):
    ## Fetch all Tasks for a given Transaction Id
    pending_tasks = await async_redis_wrapper(get_hash_set,tid)

    for i,v in pending_tasks.items():
        if v == bytes(pending,"utf-8"):
            return i
    return None
    




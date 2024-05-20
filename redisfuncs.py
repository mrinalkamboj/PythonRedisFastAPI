## import constants
from const import redis_host, redis_port,redis_nodes,pending,executing,success,failed,retrial1,retrial2,starting,lock_suffix,rate_limit_suffix,valid_suffix,redis_lock_ttl_ms

## import uuid
import uuid

## redis
import redis
from redlock import Redlock

## Asyncio
import asyncio
from asyncer import asyncify

## Async wrapper for all Redis functions
async def async_redis_wrapper(func, *args):
    return await asyncify(func)(*args)

## List all redis keys
def list_redis_keys():
    r = redis.Redis(host=redis_host, port=redis_port)
    keys = r.keys()
    r.close()
    return keys

## Check Whether a Redis Key Exists
def is_redis_key(key:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval =  bool(r.exists(key))
    r.close()
    return retval

## Get a redis key
def get_redis_key(key:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval =  r.get(key)
    r.close()
    return retval

## Get multiple redis keys
def get_redis_keys(keys:list):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval = []
    for key in keys:
        retval.append(r.get(key))
    r.close()
    return retval

## Set a redis key
def set_redis_key(key:str,value:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    r.set(key,value)
    r.close()

## Set multiple redis keys
def set_redis_keys(key_values:dict):
    r = redis.Redis(host=redis_host, port=redis_port)
    for key,value in key_values.items():
        r.set(key,value)
    r.close()

## Increment a redis key
def incr_redis_key(key:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval =  r.incr(key)
    r.close()
    return retval

## Decrement a redis key
def decr_redis_key(key:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval =  r.decr(key)
    r.close()
    return retval

## Acquire a redis lock
def acquire_redis_lock(key:str,ttl):

    # Create a distributed lock across Redis Nodes
    redlock = Redlock(redis_nodes)

    # Acquire the lock on a Key
    lock = redlock.lock(key,ttl)

    if lock:         
        print("Lock success !!")  
        # Return the lock object
        return lock          
    else:
        print("Lock Failed !!")
        return None
    
    

## Release a redis lock
def release_redis_lock(lock:Redlock):
    if lock:
        # Distributed lock Object across Redis Nodes
        redlock = Redlock(redis_nodes)

        # Release the lock
        redlock.unlock(lock)

## Delete a redis key
def delete_redis_key(key:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    if r.exists(key):
        r.delete(key)
        msg = f"Key {key} deleted"
    else:
        msg = f"Key {key} not found"
    r.close()
    return msg

## Delete all redis keys
def delete_all_redis_keys():
    r = redis.Redis(host=redis_host, port=redis_port)
    r.flushall()
    r.close()

## Get full hash set using the hash set name 
def get_hash_set(hash_name:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval = r.hgetall(hash_name)
    r.close()
    return retval

## For a hash set get the values of specific key
def get_hash_set_key(hash_name:str,key:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval = r.hget(hash_name,key)
    r.close()
    return retval

## For a hash set get the values of the keys
def get_hash_set_keys(hash_name:str,keys:list):
    r = redis.Redis(host=redis_host, port=redis_port)
    retval = []
    for key in keys:
        retval.append(r.hget(hash_name,key))
    r.close()
    return retval

## Set a hash set key value pair
def set_hash_set(hash_name:str,key:str,value:str):
    r = redis.Redis(host=redis_host, port=redis_port)
    r.hset(hash_name,key,value)
    r.close()

## Set multiple hash set key value pairs
def set_hash_set_keys(hash_name:str,key_values:dict):
    r = redis.Redis(host=redis_host, port=redis_port)
    r.hmset(hash_name,key_values)
    r.close()

## Generate a unique tracking id
def generate_unique_tracking_id():

    # Generate a unique id
    unique_id =  str(uuid.uuid4())

    while True:        
        if not is_redis_key(unique_id+valid_suffix):
            set_redis_key(unique_id+valid_suffix,"True")
            break
        else:
            isused = get_redis_key(unique_id+valid_suffix)
            if isused:
                unique_id =  str(uuid.uuid4())
                continue
            else:
                set_redis_key(unique_id+valid_suffix,True)
                break
    
    return unique_id
    


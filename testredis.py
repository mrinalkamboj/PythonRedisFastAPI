from redisfuncs import set_redis_key, get_redis_key, list_redis_keys, delete_redis_key, delete_all_redis_keys, acquire_redis_lock, release_redis_lock, incr_redis_key, decr_redis_key,async_redis_wrapper,set_redis_keys,generate_unique_tracking_id, get_hash_set,get_hash_set_keys,set_hash_set,set_hash_set_keys

from const import lock_suffix,pending,executing,success,failed,retrial1,retrial2,starting

incr_redis_key("test-key")

'''
set_hash_set_keys("test-hash",{"a":"1","b":"2","c":"3"})
hs = get_hash_set("test-hash")
for i,v in hs.items():
    print(i,v)

print(str(hs[b"a"]))
'''

'''
retval = get_hash_set_keys("23c5f1dd-fe30-441e-a9dc-95c6fa6e6b72",["1","2","3"])
for i in retval:
    print(str(i.decode('utf-8')))
'''

'''
if(get_redis_key("test-key") == None):
    print("Key not found")
    set_redis_key("test-key",0)
    print(get_redis_key("test-key"))
'''

'''
retdict = get_hash_set("test-hash")
for i,v in retdict.items():
    if v == bytes(pending,"utf-8"):
        print(i)

import asyncio

async def my_coroutine(seconds):
    print(f"Coroutine execution for {seconds} seconds begin")
    await asyncio.sleep(seconds)
    print(f"Coroutine executed for {seconds} seconds")

async def main():
    # Create and start tasks without awaiting
    asyncio.create_task(my_coroutine(5))
    asyncio.create_task(my_coroutine(10))

# Run the main function
asyncio.run(main())
'''

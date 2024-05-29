#redis_host
redis_host = 'localhost'

#redis_port
redis_port = 6379

#redis_nodes
redis_nodes = [
    {'host': 'localhost', 'port': 6379, 'db': 0},
    # Add more Redis server nodes if needed
]

## Redis Lock Time to Live in milliseconds
redis_lock_ttl_ms = 10000

## Suffix for appending to the few Redis Variables
valid_suffix = "_valid"
lock_suffix = "_lock"
pending_suffix = "_pending"
rate_limit_suffix = "_rate_limit"
current_executing_suffix = "_current_executing"
all_banners_default_rate_limit = 7
const_true = "True"
const_false = "False"

## Query executing status
starting = "starting"
pending = "pending"
executing = "executing"
success = "success"
failed = "failed"
retrial1 = "retrial1"
retrial2 = "retrial2"
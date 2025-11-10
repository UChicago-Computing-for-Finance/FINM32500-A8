# Latency:

Minimum: 0.3-2 ms (if Strategy polls immediately after price update)
Average: 250-252 ms (dominated by 0.5s poll interval)
Maximum: 500-502 ms (if Strategy just polled before price update)

Range due to the set poll interval of 0.5 seconds.

# Throughput:

1 tick per second. Set by design,

# Memory footprint:

4096 bytes or 4kb.

# Behavior under dropped connections or missing data:

missing symbol correctly returns a notification.

connections issues still produce latest data point.


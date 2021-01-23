# plutos

| ENV  | Description |
| ------------- | ------------- |
| DRIVER  | s3 / sqs / stdout / stub |
| ENABLE_GZIP  | true / false  |
| MAX_BUFFER_TIME_SECONDS  | must be >=1  |
| GZIP_LVL  | INT between 0-9  |
| S3_BUCKET  | S3 bucket name  |
| S3_PREFIX  | Mandatory prefix inside the bucket (no / as prefix or suffix)  |
| SQS_ENDPOINTS  | Comma separated full path urls to SQS quues  |
| SQS_BUFFER  | bytes to keep in memory before flushing to sqs max is 256 KB recommended is multiples of 64KB  |

# How to run

docker run --network=host \
       -e DRIVER=s3 \
       -e S3_REGION=<REGION> \
       -e S3_BUCKET=<BUCKET> \
       -e S3_PREFIX=data \
       -e MAX_BUFFER_TIME_SECONDS=60 \
       -e GZIP_LVL=9 \
       -e ENABLE_GZIP=true
       maxkuder/plutos:0.1.0

# settings

Benchmarks
wrk -t2 -c100 -d10s "http://127.0.0.1:8080/e?test=me"
Running 10s test @ http://127.0.0.1:8080/e?test=me
2 threads and 100 connections
    Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   533.06us    1.19ms  32.27ms   95.47%
    Req/Sec   119.28k     9.30k  130.28k    87.00%
2373350 requests in 10.04s, 86.01MB read
Requests/sec: 236426.37
Transfer/sec:      8.57MB
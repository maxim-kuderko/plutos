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
```shell
docker run --network=host \
       -e DRIVER=s3 \
       -e S3_REGION=<REGION> \
       -e S3_BUCKET=<BUCKET> \
       -e S3_PREFIX=data \
       -e MAX_BUFFER_TIME_SECONDS=60 \
       -e GZIP_LVL=9 \
       -e ENABLE_GZIP=true \
       maxkuder/plutos
```


# Benchmarks
i7 6700K @ 4.0Ghz
GOMAXPROCS=6
wrk -t2 -c100 -d10s "http://127.0.0.1:8080/e?test=me"
![img.png](img.png)

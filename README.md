## Run test

for startup redis:

    docker run --name recorder-redis -p 6379:6379 -d redis:alpine

for starting redis with password:

    docker run \
        -p 6379:6379 \
        -v $PWD/data:/data \
        --name redis \
        -d redis:alpine redis-server --appendonly yes  --requirepass "test123" 

run test:

    v -enable-globals test .

run test with logs:

    v -stats -enable-globals test .

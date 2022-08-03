## Run test

for startup redis:

    docker run --name recorder-redis -p 6379:6379 -d redis:alpine

run test:

    v -enable-globals test .

run test with logs:

    v -stats -enable-globals test .

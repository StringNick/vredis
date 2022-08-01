## Run test

for startup redis:

    docker run --name recorder-redis -p 6379:6379 -d redis:alpine

run test:

    v test .

run test with logs:

    v -stats test .
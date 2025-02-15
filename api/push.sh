#/bin/sh

docker build -t sirogami/api:latest .

docker push sirogami/api:latest

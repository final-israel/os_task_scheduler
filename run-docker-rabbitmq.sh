#!/bin/bash
# use this to run local rabbit if you want to simply test\debug and dont want to connect to central queue managment
# DONT FORGET TO ADD "127.0.0.1       tscheduler-mq" to your /etc/hosts
docker run -d --hostname tscheduler-mq --name rabbitmq -p 5672:5672 -p 15672:15672 --mount "type=bind,src=/etc/localtime,dst=/etc/localtime,ro=false" rabbitmq:3-management

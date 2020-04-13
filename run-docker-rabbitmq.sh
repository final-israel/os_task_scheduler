#!/bin/bash
docker run -d --hostname tscheduler-mq --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
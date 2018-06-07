
# Start cluster

    docker run --rm -it \
               -p 2181:2181 -p 3030:3030 -p 8081:8081 \
               -p 8082:8082 -p 8083:8083 -p 9092:9092 \
               -e ADV_HOST=127.0.0.1 \
               landoop/fast-data-dev

# New tab: follow the broker log

    docker ps
    docker exec -it <ID> tail -f /var/log/broker.log


# Kafka command lines tools
- go to bash

      docker run --rm -it --net=host landoop/fast-data-dev bash

# New tab: create our topic with appropriate configs
Create a kafka topic with high compaction ratio.

    kafka-topics --zookeeper 127.0.0.1:2181 --create \
                 --topic employee-salary-compact \
                 --partitions 1 --replication-factor 1 \
                 --config cleanup.policy=compact \
                 --config min.cleanable.dirty.ratio=0.005 \
                 --config segment.ms=10000

# in a new tab, we start a consumer

    kafka-console-consumer --bootstrap-server 127.0.0.1:9092 \
                       --topic employee-salary-compact \
                       --from-beginning \
                       --property print.key=true \
                       --property key.separator=,

# we start pushing data to the topic

    kafka-console-producer --broker-list 127.0.0.1:9092 \
                          --topic employee-salary-compact \
                          --property parse.key=true \
                          --property key.separator=,

# copy these messages:
 
    123,{"John":"80000"}
    456,{"Mark":"90000"}
    789,{"Lisa":"95000"}

# we observe that the messages have been pushed and read by the consumer

# we know push the following new messages

    789,{"Lisa":"110000"}
    123,{"John":"100000"}


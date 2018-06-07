# Docker for Mac >= 1.12, Linux, Docker for Windows 10
## run and throwaway result
    docker run --rm -it \
               -p 2181:2181 -p 3030:3030 -p 8081:8081 \
               -p 8082:8082 -p 8083:8083 -p 9092:9092 \
               -e ADV_HOST=127.0.0.1 \
               landoop/fast-data-dev

## run and restart with result reuse
   - first start of the container
   
    docker run --name=kafka-cluster-dev -it \
               -p 2181:2181 -p 3030:3030 -p 8081:8081 \
               -p 8082:8082 -p 8083:8083 -p 9092:9092 \
               -e ADV_HOST=127.0.0.1 \
               landoop/fast-data-dev
   
   - restart the container after it was stopped 
    
    docker start kafka-cluster-dev

   - stop the container  
    
    docker stop kafka-cluster-dev
    
## start with Docker toolbox
    docker run --rm -it \
              -p 2181:2181 -p 3030:3030 -p 8081:8081 \
              -p 8082:8082 -p 8083:8083 -p 9092:9092 \
              -e ADV_HOST=192.168.99.100 \
              landoop/fast-data-dev

## Kafka command lines tools

- go to bash in the docker container
    
        docker run --rm -it --net=host landoop/fast-data-dev bash

## Docker stop    
    docker stop landoop/fast-data-dev

    
# Kafka Topics
    
## create topic    
    kafka-topics --zookeeper 127.0.0.1:2181 \
        --create \
        --topic first_topic \
        --partitions 3 \
        --replication-factor 1 
     
## list topics
    kafka-topics --zookeeper 127.0.0.1:2181 \
        --list
        
## delete topics
    kafka-topics --zookeeper 127.0.0.1:2181 \
        --topic second_topic \
        --delete
        
    Note: This will have no impact if delete.topic.enable is not set to true.    
        
## describe topics
    kafka-topics --zookeeper 127.0.0.1:2181 \
        --describe \
        --topic first_topic
        
## produce data to a topic
    kafka-console-producer \
        --broker-list 127.0.0.1:9092 \
        --topic first_topic
        
    kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic

## consume data from a topic
    kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic test_topic
    
   - read all the data from beginning
        
    kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic test_topic --from-beginning
     
    --consumer-property group.id=mygroup1
    --partition <#>
    --offset <#>
    
## kafka topics UI

[kafka topics UI](http://localhost:3030/kafka-topics-ui/#)


## kafka topic configuration

- change config at creation

      kafka-topics --zookeeper 127.0.0.1:2181 \
            --create \
            --topic custom_topic \
            --partitions 3 \
            --replication-factor 1 \
            --config cleanup.policy=compact
            
- change config at runtime
    
      kafka-topics --zookeeper 127.0.0.1:2181 \
                --alter \
                --topic custom_topic \
                --config cleanup.policy=delete

- check custom configuration

      kafka-topics --zookeeper 127.0.0.1:2181 \
            --describe \
            --topic custom_topic

            
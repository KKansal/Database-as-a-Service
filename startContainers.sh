sudo docker run -d -p 15672:15672 -p 5672:5672 --name rabbitmq rabbitmq:3-management;
sudo docker run -d -p 27017:27017 mongo;
# sudo  docker run -p 2181:2181 -p 2888:2888 -p 3888:3888 -p  8080:8080 --name some-zookeeper --restart always -d zookeeper
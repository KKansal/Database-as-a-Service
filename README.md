# Database-as-a-Service (DBaaS)
A Highly-Available and scalabe Database as a Service. Project under the Cloud Computing Course at PES University 

DBaaS is a containerized service exposing Read and Write API's allowing users to manage Data.
Under the hood it MongoDB database to facilated data storage. Using Docker the whole Service has been divided into microservices
which communicate using the AMQP (Advanced Message Queue Protocol) using RabbitMQ. Zookeeper has been used to keep track of all the
components and take suitable actions on Failing hence adding High-Availibilty to the features. Python - Flask has been used to serve 
the necessary API endpoints


## Technologies Used
* Docker
* RabbitMQ.
* Zookeeper
* MongoDB
* Python - Flask
* Python - Pymongo


## Getting Started
It is Higly recommended to read through the report attached in the Directory to fully Understand the working of the project
Also the complete code has been documentated in case of any clarifications.
These instructions indicate how to run and test the code on a local machine.

### Prerequisites

1. Install Docker Server.
Instructions to install Docker for Windows,Mac OS and Linux can be found here : https://docs.docker.com/get-docker/


2. Install Docker-Compose
Instructions to install Docker-Compose can be Found here: https://docs.docker.com/compose/install/


### Deployment
1. Build the Worker Image present in /worker directory and tag the image as "worker".
For a linux system it can be automated by running :-

``` 
 sh build_worker.sh
```

2. To start the service up execute the following commands.
Make Sure Internet Connectivity is available as while running the first time Docker will download required images.

```
docker-compose up
```
Note - Make sure required permissions are available. The service by default binds on port 80 so it is important port 
is available.( Approriate  Change can be made in docker-compose.yml to use a different port).

The service should be accessible at port 80 of the local machine.

### Running the tests
The following API's are available by the service:-

1) /api/v1/write - Perform the write operation on the Database.
   Method : POST
```

Request format : {
                    table : {}, # Table/Collection name where the operation has to be performed  
                    data  : {}, # Data to be inserted/updated/deleted
                    operation : {}, # operation can be among these 3 types - insert,delete,update 
                    filter: {} # Used in update operation to find the match the data to update.
}


Response Status Codes :
  200 Write operation successfull.
  400 Bad Request - Incorrect Request Format / Invalid operation
  405 operation not supported.

```

2) /api/v1/read - Perform the read Operation on the Database.
  Method : POST
```

Request format : {
                    table : {}, # Table/Collection name where the operation has to be performed  
                    conditions : {} # Conditions / filter the data should satisfy      

}
Response Status Codes :
  200 read operation successfull.
  400 Bad Request - Incorrect Request Format / Invalid operation
  204 No Content / Condition did not match any Entries.

```

3) /api/v1/crash/master - Crash the Master Worker
   Method : POST
```
Request format : No Data required

Response format : {

            PID of the master container
    
}
```

4) /api/v1/crash/slave : Crash a Slave Worker
   Method : POST
```
Request format : No Data required

Response format : {

            PID of the slave container
    
}
```

5) /api/v1/worker/list : Get a List of all the running Workers
   Method : GET
 
 ```
  Response format : {
    [PID's of all the Worker containers]
  }
 

 ```


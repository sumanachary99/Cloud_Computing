# Cloud Computing Assignment - DBaaS
Serves the database read/write APIs for the Rideshare application.  
Before starting here, Install Docker and Docker-compose in your system

## Creating the Docker Network

1. Start the docker service.
2. Run the `build` command to start Orchestrator, RabbitMQ ,Zookeeper, Master and Slave Containers

    ```shell
    $sudo docker-compose build
    ```

3. Start the containers as daemon process

    ```shell
    $sudo docker-compose up -d
    ```
 
 ## To Experience the working of project
 
 1. Open postman and send http requests to the load balancer IP
   
    ```
    here goes loadB ip
    ```

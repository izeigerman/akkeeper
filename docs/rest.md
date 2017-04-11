# Akkeeper REST API

* [Deploy API](#deploy-api)
  * [Deploy instances](#deploy-instances)
* [Monitoring API](#monitoring-api)
  * [Retrieve list of all instances in a cluster](#retrieve-list-of-all-instances-in-a-cluster)
  * [Get all instances with specific role and/or container](#get-all-instances-with-specific-role-and-or-container)
  * [Get a detailed info about the instance](#get-a-detailed-info-about-the-instance)
  * [Instance termination](#instance-termination)
* [Container API](#container-api)
  * [Get list of available containers](#get-list-of-available-containers)
  * [Get a detailed info about container](#get-a-detailed-info-about-container)
  * [Create a new container](#create-a-new-container)
  * [Update the existing container](#update-the-existing-container)
  * [Delete container](#delete-container)

## Deploy API
#### Deploy instances

**Command**
````bash
curl -X POST -H "Content-Type: application/json" -d "@body.json" http://<master_host>:5050/api/v1/deploy
````
**Request body**
```json
{
  "name": "pingContainer",
  "quantity": 1
}
```
**Response code**

202 - Accepted

**Response body**
```json
{
  "requestId": "477b324f-f620-45df-a0e6-bf7211d01fd2",
  "containerName": "pingContainer",
  "instanceIds": ["pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6"]
}
```

## Monitoring API
#### Retrieve list of all instances in a cluster

**Command**
```bash
curl -X GET -H "Content-Type: application/json" http://<master_host>:5050/api/v1/instances
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "1fec516e-aeda-4859-b25c-bd641988c91c",
  "instanceIds": ["pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6", "pingContainer-2e76ea01-a623-4aea-abfa-cf5e37c6c898"]
}
```

#### Get all instances with specific role and/or container

**Command**
```bash
curl -X GET -H "Content-Type: application/json" http://<master_host>:5050/api/v1/instances?role=ping&containerName=pingContainer
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "1fec516e-aeda-4859-b25c-bd641988c91c",
  "instanceIds": ["pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6", "pingContainer-2e76ea01-a623-4aea-abfa-cf5e37c6c898"]
}
```

#### Get a detailed info about the instance

**Command**
```bash
curl -X GET -H "Content-Type: application/json" http://<master_host>:5050/api/v1/instances/pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "0f292e02-d12c-4926-82f4-085e4cff8a69",
  "info": {
    "instanceId": "pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6",
    "containerName": "pingContainer",
    "roles": ["ping"],
    "status": "UP",
    "actors": ["/user/akkeeperInstance/pingService"],
    "address": {
      "protocol": "akka.tcp",
      "system": "AkkeeperSystem",
      "host": "172.17.0.7",
      "port": 44874
    }
  }
}
```

#### Instance termination

**Command**
```bash
curl -X DELETE -H "Content-Type: application/json" http://<master_host>:5050/api/v1/instances/pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "382f7496-59b8-44fa-b877-35088faa1917",
  "instanceId": "pingContainer-2aa1a500-4c6e-423c-bde1-d22295c7e1b6"
}
```

## Container API
#### Get list of available containers

**Command**
```bash
curl -X GET -H "Content-Type: application/json" http://<master_host>:5050/api/v1/containers
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "5096a7db-3cf9-4696-89e1-e0a143cc4d25",
  "containers": ["pingContainer"]
}
```

#### Get a detailed info about container

**Command**
```bash
curl -X GET -H "Content-Type: application/json" http://<master_host>:5050/api/v1/containers/pingContainer
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "f423e203-936b-45e1-8a21-6b9da7c4f1e3",
  "container": {
    "name": "pingContainer",
    "jvmArgs": ["-Xmx1G"],
    "jvmProperties": {
      "akka.cluster.roles.\"0\"": "ping",
      "ping-app.response-value": "Akkeeper"
    },
    "environment": {

    },
    "actors": [{
      "name": "pingService",
      "fqn": "akkeeper.examples.PingActor"
    }],
    "cpus": 1,
    "memory": 1024
  }
}
```

#### Create a new container

**Command**
```bash
curl -X POST -H "Content-Type: application/json" -d "@body.json" http://<master_host>:5050/api/v1/containers
```
**Request body**
```json
{
  "name": "myContainer",
  "cpus": 2,
  "memory": 2048,
  "actors": [
    {
      "name": "myActor",
      "fqn": "akkeeper.examples.PingActor"
    }
  ],
  "jvmArgs": ["-Xmx2048"],
  "jvmProperties": {},
  "environment": {}
}
```
**Response code**

201 - Created

**Response body**
```json
{
  "requestId": "859d37fb-1d56-43e8-9cea-8e807baeca1c",
  "name": "myContainer"
}
```

#### Update the existing container

**Command**
```bash
curl -X PATCH -H "Content-Type: application/json" -d "@body.json" http://<master_host>:5050/api/v1/containers/myContainer
```
**Request body**

See the container creation request body.

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "859d37fb-1d56-43e8-9cea-8e807baeca1c",
  "name": "myContainer"
}
```

#### Delete container

**Command**
```bash
curl -X DELETE -H "Content-Type: application/json" http://<master_host>:5050/api/v1/containers/myContainer
```
**Request body**

**Response code**

200 - OK

**Response body**
```json
{
  "requestId": "859d37fb-1d56-43e8-9cea-8e807baeca1c",
  "name": "myContainer"
}
```
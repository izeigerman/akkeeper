# Akkeeper REST API

* [Deploy API](#deploy-api)
  * [Deploy instances](#deploy-instances)
* [Monitoring API](#monitoring-api)
  * [Retrieve a list of all instances in a cluster](#retrieve-a-list-of-all-instances-in-a-cluster)
  * [Get all instances by specific role, container name or status](#get-all-instances-by-specific-role-container-name-or-status)
  * [Get a detailed info about the instance](#get-a-detailed-info-about-the-instance)
  * [Instance termination](#instance-termination)
* [Container API](#container-api)
  * [Get a list of available containers](#get-a-list-of-available-containers)
  * [Get a detailed info about container](#get-a-detailed-info-about-container)
  * [Create a new container](#create-a-new-container)
  * [Update the existing container](#update-the-existing-container)
  * [Delete container](#delete-container)
* [Master API](#master-api)
  * [Terminate master](#terminate-master)

## Deploy API
### Deploy instances

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
### Retrieve a list of all instances in a cluster

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

### Get all instances by specific role, container name or status

**Command**
```bash
curl -X GET -H "Content-Type: application/json" http://<master_host>:5050/api/v1/instances?role=ping&containerName=pingContainer&status=up
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

### Get a detailed info about the instance

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

### Instance termination

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
### Get a list of available containers

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

### Get a detailed info about container

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

## Master API
### Terminate master

**Command**
```bash
curl -X POST http://<master_host>:5050/api/v1/master/terminate
```
**Request body**

**Response code**

202 - Accepted

**Response body**

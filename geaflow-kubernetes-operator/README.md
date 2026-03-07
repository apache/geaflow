## Introduction

Geaflow-kubernetes-operator is a tool that can quickly deploy Geaflow applications to kubernetes clusters.
This tool is implemented in Java language and can quickly deploy Geaflow applications, monitor
application status, perform hot updates and other functions by kubectl commands or other tools.
JDK11 or higher version is required。

## Quick Start

1. (Optional) Execute the shell script to build the image of this project.
```shell
cd geaflow-kubernetes-operator
./build-operator.sh
```
2. The image name and tag can be modified in helm/geaflow-kubernetes-operator/values.
3. Execute this command in the project root directory to quickly deploy to the kubernetes cluster.
```shell
cd geaflow-kubernetes-operator
helm install geaflow-kubernetes-operator helm/geaflow-kubernetes-operator
```
### Frontend Dashboard
Geaflow-kubernetes-operator comes with a frontend dashboard page, which you can access through port 8089.
The dashboard will display job statistics within the cluster, as well as their current status and basic information.


### Example

Please refer to example/example.yml and modify the necessary parameters. After helm deployment
is completed, execute this command to submit the example job into your kubernetes cluster.
```shell
cd geaflow-kubernetes-operator
kubectl apply -f example/example_hla.yml
```

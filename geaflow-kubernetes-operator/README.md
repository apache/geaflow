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
3. Create the required authentication Secret following [the security model](SECURITY_MODEL.md).
   Set a unique tag for an image built from this source. Then deploy:
```shell
cd geaflow-kubernetes-operator
helm install geaflow-kubernetes-operator helm/geaflow-kubernetes-operator \
  --namespace geaflow-system \
  --set webSecurity.existingSecret=geaflow-operator-web-auth \
  --set image.tag=YOUR_UNIQUE_PATCHED_IMAGE_TAG \
  --set-json 'watchNamespaces=["geaflow-system"]'
```
### Frontend Dashboard
The dashboard requires an administrator or read-only bearer token. Prefer the read-only token.
The web service listens on port 8089 in Helm (8080 for standalone execution). The default
NetworkPolicy denies pod ingress; use a local tunnel and open `http://127.0.0.1:8089/`:

```sh
kubectl -n geaflow-system port-forward --address 127.0.0.1 service/geaflow-kubernetes-operator 8089:80
```

The dashboard displays job statistics and state. Tokens stay in page memory and must be
re-entered after reload. For TLS, credential rotation, RBAC and admission requirements,
read [the security model](SECURITY_MODEL.md). Job submission is reserved for trusted operators.


### Example

Please refer to example/example.yml and modify the necessary parameters. After helm deployment
is completed, execute this command to submit the example job into your kubernetes cluster.
```shell
cd geaflow-kubernetes-operator
kubectl -n geaflow-system apply -f example/example_hla.yml
```

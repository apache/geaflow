## 介绍

Geaflow-kubernetes-operator是一个可以快速将Geaflow应用部署到kubernetes集群中的部署工具。
本工具使用Java语言实现，可以通过kubectl等工具快速部署Geaflow应用、监听应用状态、进行热更新等功能。
最低要求JDK11。

## 快速入门

1. （可选）执行build.sh构建本项目的镜像
```shell
cd geaflow-kubernetes-operator
./build-operator.sh
```
2. 在helm/geaflow-kubernetes-operator/values中可以修改镜像名和tag。
3. 在项目根目录下执行以下命令即可快速部署到kubernetes集群中。
```shell
cd geaflow-kubernetes-operator
helm install geaflow-kubernetes-operator helm/geaflow-kubernetes-operator
```

### 前端Dashboard
Geaflow-kubernetes-operator自带一个前端dashboard页面，你可以通过8089端口访问它。
dashboard会展示集群内的作业统计，并展示它们的当前状态和基础信息。

### 示例

请参考example/example.yml并修改必要参数。在helm部署完成后，执行以下命令提交示例的作业。
```shell
cd geaflow-kubernetes-operator
kubectl apply -f example/example_hla.yml
```

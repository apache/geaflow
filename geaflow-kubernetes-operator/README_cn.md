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
3. 先按[安全模型与部署要求](SECURITY_MODEL.md)在部署命名空间创建认证 Secret，
   并使用包含本次认证功能的独立镜像 tag，再执行部署：
```shell
cd geaflow-kubernetes-operator
helm install geaflow-kubernetes-operator helm/geaflow-kubernetes-operator \
  --namespace geaflow-system \
  --set webSecurity.existingSecret=geaflow-operator-web-auth \
  --set image.tag=YOUR_UNIQUE_PATCHED_IMAGE_TAG \
  --set-json 'watchNamespaces=["geaflow-system"]'
```

### 前端Dashboard
Dashboard 需要管理员或只读 Bearer 令牌，日常查看优先使用只读令牌。
Helm 中 Web 监听端口为 8089（独立启动默认 8080）。默认 NetworkPolicy 拒绝 Pod 入站访问，
可以建立本机隧道并访问 `http://127.0.0.1:8089/`：

```sh
kubectl -n geaflow-system port-forward --address 127.0.0.1 service/geaflow-kubernetes-operator 8089:80
```

Dashboard 展示作业统计、状态和基础信息。令牌只保存在页面内存，刷新后需重新输入。
管理员提交作业等同于授权执行代码，只能交给可信运维人员。
TLS、Secret 轮换、命名空间 RBAC 和准入策略要求见[安全模型](SECURITY_MODEL.md)。

### 示例

请参考example/example.yml并修改必要参数。在helm部署完成后，执行以下命令提交示例的作业。
```shell
cd geaflow-kubernetes-operator
kubectl -n geaflow-system apply -f example/example_hla.yml
```

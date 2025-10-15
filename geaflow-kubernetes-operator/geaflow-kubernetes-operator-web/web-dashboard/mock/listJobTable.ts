import {Request, Response} from 'express';


const genJobList = (current: number, pageSize: number) => {
  const tableListDataSource: API.GeaflowJob[] = [];

  const jobStateList = ['SUBMITTED', 'RUNNING', 'FAILED', 'SUSPENDED', 'REDEPLOYING', 'FINISHED'];
  const componentStateList = ['NOT_DEPLOYED', 'DEPLOYED_NOT_READY', 'RUNNING', 'EXITED', 'ERROR'];

  for (let i = 0; i < pageSize; i += 1) {
    const index = (current - 1) * 10 + i;
    tableListDataSource.push({
      "apiVersion": "geaflow.antgroup.com/v1",
      "kind": "GeaflowJob",
      "metadata": {
        "creationTimestamp": new Date().getTime() - index * 1000,
        "wadwad": "wdauidhwagyudhishaiudbhiuashdiusahskjhisuahdiwauhkdwiafgiasukhfiahdwaiudhwiauhfiwahdiushaisuhiahfeiabfwahfiwuahfawiuhfishdisauhdiuahifdhsiuahfiuhiusahdiufhihfiaufhwiadhuisdhskfhkahfwuiadhihu",
        "generation": 3,
        "name": "geaflow-example-" + index,
        "namespace": "default",
        "resourceVersion": 677080,
        "uid": "e05ec272-a255-4c03-b9c3-1fa08d090929"
      },
      "spec": {
        "clientSpec": {
          "resource": {
            "cpuCores": 1,
            "jvmOptions": "-Xmx800m,-Xms800m,-Xmn300m",
            "memoryMb": 300
          }
        },
        "containerSpec": {
          "containerNum": index,
          "resource": {
            "cpuCores": 1,
            "jvmOptions": "-Xmx800m,-Xms800m,-Xmn300m",
            "memoryMb": 1000
          },
          "workerNumPerContainer": 4
        },
        "driverSpec": {
          "driverNum": index,
          "resource": {
            "cpuCores": 1,
            "jvmOptions": "-Xmx800m,-Xms800m,-Xmn300m",
            "memoryMb": 1000
          }
        },
        "engineJars": [
          {
            "md5": "2d9d04e942522e66ba19e6bda7a18989",
            "name": "rayag.jar",
            "url": "http://xxxx/rayag-1.0-comile-test.jar"
          }
        ],
        "entryClass": "dynamic.graph.example.org.apache.geaflow.IncrGraphCompute",
        "image": "geaflow:0.1-heartbeat-register-fix",
        "imagePullPolicy": "Never",
        "masterSpec": {
          "resource": {
            "cpuCores": 1,
            "jvmOptions": "-Xmx800m,-Xms800m,-Xmn300m",
            "memoryMb": 1000
          }
        },
        "serviceAccount": "geaflow",
        "udfJars": [
          {
            "md5": "f32a8a2524620dbecc9f6bf6a20c293f",
            "name": "guava.jar",
            "url": "http://xxxx/guava-20.0.jar"
          }
        ],
        "userSpec": {
          "additionalArgs": {
            "geaflow.worker.num": "20"
          },
          "metricConfig": {
            "geaflow.metric.reporters": "slf4j",
            "geaflow.metric.stats.type": "memory"
          },
          "stateConfig": {
            "geaflow.file.persistent.root": "/tmp",
            "geaflow.file.persistent.type": "LOCAL",
            "geaflow.store.redis.host": "localhost",
            "geaflow.store.redis.port": "5123"
          }
        }
      },
      "status": {
        "clientState": componentStateList[index % componentStateList.length],
        "jobUid": 230913050902833920,
        "lastReconciledSpec": "{}",
        "masterState": componentStateList[index % componentStateList.length],
        "state": jobStateList[index % jobStateList.length],
        "errorMessage": "Here is an error message."
      }
    });
  }
  tableListDataSource.reverse();
  return tableListDataSource;
};

let tableListDataSource = genJobList(1, 100);

function getJobs(req: Request, res: Response, u: string) {
  const result = {
    data: tableListDataSource,
    success: true
  };

  return res.json(result);
}

export default {
  'GET /api/jobs': getJobs,
};

#!/usr/bin/env python

import time
import random
import json

from kubernetes import client, config, watch

#config.load_kube_config()
config.load_incluster_config()
v1=client.CoreV1Api()

scheduler_name = "staticScheduler"

def getFileSizes():
    pod_list = v1.list_namespaced_pod("default")
    custom_master_pods = filter(lambda x : x.metadata.labels["appType"] == "custom_master" and x.status.phase == "Running", pod_list)
    spark_master_pods = filter(lambda x : x.metadata.labels["appType"] == "spark_master" and x.status.phase == "Running", pod_list)
    if len(custom_master_pods) == 1 and len(spark_master_pods) == 1:
        return [int(custom_master_pods[0].metadata.labels["inputSize"]), int(spark_master_pods[0].metadata.labels["inputSize"])]
    else:
        return [0,0]

def workersAllowed(app, filesizes): #app = 'spark' or 'custom'
    #calculating number of nodes to allocate
    #ratio = ourfunct(filesize)
    #spark = round( available * ( ratio / ratio+1 ))
    #custom = available - spark
    if app == "spark":
        return 5
    else:
        return 2

def workersAlreadyRunning(app): #app = 'spark' or 'custom'
    pod_list = v1.list_namespaced_pod("default")
    #phase can be Pending / Running / Succeeded / Failed / Unknown
    working_pod_list = filter(lambda x : x.metadata.labels["appType"] == app and x.status.phase == "Running", pod_list)
    return(len(working_pod_list))

def nodes_available():
    ready_nodes = []
    for n in v1.list_node().items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    ready_nodes.append(n.metadata.name)
    return ready_nodes

def scheduler(name, node, namespace="default"):
    body=client.V1Binding()

    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node

    meta=client.V1ObjectMeta()
    meta.name=name

    body.target=target
    body.metadata=meta

    return v1.create_namespaced_binding_binding(name, namespace, body)

def main():
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "default"):
        appType = event['object'].metadata.labels['appType']
        if pod.status.phase == "Pending" and pod['object'].spec.scheduler_name == scheduler_name:
            #wait if we don't see both drivers for custom & spark
            fileSizes = []
            while True:
                fileSizes = getFilesizes()
                if fileSizes[0] == 0:
                    time.sleep(5)
                else:
                    break
            #check if there's already enough workers or not
            if workersAlreadyRunning(appType) < workersAllowed(appType):
                try:
                    res = scheduler(event['object'].metadata.name, random.choice(nodes_available()))
                except client.rest.ApiException as e:
                    print(json.loads(e.body)['message'])
            else:
                v1.delete_namespaced_pod(event['object'].metadata.name, 'default')

if __name__ == '__main__':
    main()

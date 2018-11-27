#!/usr/bin/env python

import time
import random
import json

from kubernetes import client, config, watch

config.load_kube_config()
v1=client.CoreV1Api()

scheduler_name = "foobar"

def workersAllowed(app): #app = 'spark' or 'custom'
    #calculating number of nodes to allocate
    #ratio = ourfunct(filesize)
    #spark = round( available * ( ratio / ratio+1 ))
    #custom = available - spark
    if app == "spark":
        return spark
    else:
        return custom

def workersAlreadyRunning(app): #app = 'spark' or 'custom'
    pod_list = v1.list_namespaced_pod("default")
    #phase can be Pending / Running / Succeeded / Failed / Unknown
    working_pod_list = filter(lambda x : x.metadata.labels["appType"] == app and x.status.phase == "Running" : x, pod_list)
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
        pod = event['object']
        if pod.status.phase == "Pending" and pod['object'].spec.scheduler_name == scheduler_name:
            appType = pod.metadata.labels['appType']
            if workersAlreadyRunning(appType) < workersAllowed(appType):
                try:
                    res = scheduler(event['object'].metadata.name, random.choice(nodes_available()))
                except client.rest.ApiException as e:
                    print json.loads(e.body)['message']
            else:
                v1.delete_namespaced_pod(event['object'].metadata.name, 'default')

if __name__ == '__main__':
    main()

import time
import random
import json

from kubernetes import client, config, watch

log = open('log.txt', 'w+')

config.load_incluster_config()
v1 = client.CoreV1Api()

scheduler_name = "staticscheduler"

scheduled_custom = {}
scheduled_spark = {}


def getFileSizes():
    log.write("trying to get filesizes\n")
    log.flush()
    custom_master_pods = v1.list_namespaced_pod("default", label_selector="appType==custom_master", limit=1).items
    spark_master_pods = v1.list_namespaced_pod("default", label_selector="appType==spark_master", limit=1).items
    log.write("length of cmp: {}\n".format(len(list(custom_master_pods))))
    log.write("length of smp: {}\n".format(len(list(spark_master_pods))))
    for v in custom_master_pods:
        log.write("cmp: appType: {}, phase: {}, inputSize: {}\n".format(v.metadata.labels["appType"], v.status.phase, v.metadata.labels["inputSize"]))
    log.flush()
    #TODO: change or to and for running both custom and spark
    if len(list(custom_master_pods)) == 1 or len(list(spark_master_pods)) == 1:
        return [int(custom_master_pods[0].metadata.labels["inputSize"]), 0] #int(spark_master_pods[0].metadata.labels["inputSize"])]
    else:
        return [0,0]

def workersAllowed(app, filesizes): #app = 'spark' or 'custom'
    log.write("getting workersAllowed\n")
    log.flush()
    #calculating number of nodes to allocate
    #ratio = ourfunct(filesize)
    #spark = round( available * ( ratio / ratio+1 ))
    #custom = available - spark
    print(app)
    if app == "spark":
        return 5
    else:
        return 2


def workersAlreadyRunning(app):  # app = 'spark' or 'custom'
    log.write("getting workersAlreadyRunning\n")
    log.flush()
    pod_list = v1.list_namespaced_pod("default", label_selector="appType=={}".format(app)).items
    #phase can be Pending / Running / Succeeded / Failed / Unknown
    running_pods = []
    for p in pod_list:
        print("detected pod: {}, phase: {}".format(p.metadata.name, p.status.phase))
        if p.status.phase == "Running":
            running_pods.append(p.metadata.name)
    print("running_pods: {}\n".format(running_pods))
    return(len(running_pods))


def nodes_available():
    log.write("getting nodes available \n")
    log.flush()
    ready_nodes = []
    for n in v1.list_node().items:
        for status in n.status.conditions:
            if status.status == "True" and status.type == "Ready":
                ready_nodes.append(n.metadata.name)
    return ready_nodes


def scheduler(name, node, namespace="default"):
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node)
    meta = client.V1ObjectMeta(name=name)
    body = client.V1Binding(metadata=meta, target=target)

    log.write("meta and target name: {} , {}\n".format(body.metadata.name, body.target.name))
    print("meta and target name: {} , {}\n".format(body.metadata.name, body.target.name))
    print("node name: {}".format(node))
    print("pod name: {}".format(name))

    res = None
    try:
        res = v1.create_namespaced_binding(namespace, body)
    except ValueError:
        print('ValueError as Expected')
    finally:
        return res

def main():
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "default"):
        pod = event['object']
        print("I've got this pod: {}\n".format(pod.metadata.labels['run']))
        print("phase: {} \t scheduler_name: {} \t appType: {} \n".format(pod.status.phase, pod.spec.scheduler_name, pod.metadata.labels['appType']))
        appType = event['object'].metadata.labels['appType']
        print("should I try scheduling this? --appType: {}, phase: {}, name: {}".format(appType, pod.status.phase, pod.spec.scheduler_name))
        if pod.status.phase == "Pending" and pod.spec.scheduler_name == scheduler_name:
            log.write("okay on this pod, let's start \n")
            log.flush()
            #wait if we don't see both drivers for custom & spark
            fileSizes = []
            while True:
                fileSizes = getFileSizes()
                if fileSizes[0] == 0:
                    print("filesizes: {}".format(fileSizes))
                    print("falling asleep\n")
                    time.sleep(5)
                else:
                    break
            #check if there's already enough workers or not
            war = workersAlreadyRunning(appType)
            print("{} workers already running".format(war))
            if war < workersAllowed(appType, [200, 500]):
                print("okay I can assign a node\n")
                nodesAvailable = nodes_available()
                try:
                    node = random.choice(nodesAvailable)
                    res = scheduler(pod.metadata.name, node)
                except client.rest.ApiException as e:
                    log.write(json.loads(e.body)['message'])
                    log.flush()
            else:
                print("I shouldn't assign a node")
                body = client.V1DeleteOptions()
                v1.delete_namespaced_pod(event['object'].metadata.name, 'default', body)

if __name__ == '__main__':
    main()
    log.close()

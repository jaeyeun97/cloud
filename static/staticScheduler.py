import time
import random
import json

from kubernetes import client, config, watch

log = open('log.txt', 'w+')

config.load_incluster_config()
v1 = client.CoreV1Api()
delete_option = client.V1DeleteOptions()

scheduler_name = "staticscheduler"

podSeen = dict()

def getFileSizes():
    custom_master_pods = v1.list_namespaced_pod("default", label_selector="run==group2-custom-master", limit=1).items
    spark_master_pods = v1.list_namespaced_pod("default", label_selector="run==group2-spark-master", limit=1).items

    if len(list(custom_master_pods)) == 1 and len(list(spark_master_pods)) == 0:
        print("got inputsizes/ custom: {} , spark: {}".format(int(custom_master_pods[0].metadata.labels["inputSize"]), "N/A"))
    elif len(list(custom_master_pods)) == 0 and len(list(spark_master_pods)) == 1:
        print("got inputsizes/ custom: {} , spark: {}".format("N/A", int(spark_master_pods[0].metadata.labels["inputSize"])))
    else:
        print("got inputsizes/ custom: {} , spark: {}".format(int(custom_master_pods[0].metadata.labels["inputSize"]), int(spark_master_pods[0].metadata.labels["inputSize"])))
    #TODO: change or to and for running both custom and spark
    if len(list(custom_master_pods)) == 1 and len(list(spark_master_pods)) == 1:
        return [int(custom_master_pods[0].metadata.labels["inputSize"]), int(spark_master_pods[0].metadata.labels["inputSize"])]
    else:
        return [0,0]

def workersAllowed(app, filesizes): #app = 'spark' or 'custom'
    #calculating number of nodes to allocate
    #ratio = ourfunct(filesize)
    #spark = round( available * ( ratio / ratio+1 ))
    #custom = available - spark
    print(app)
    if app == "group2-spark-worker":
        return 3
    else:
        return 4

def workersAlreadyRunning(app):  # app = 'spark' or 'custom'
    if app == 'group2-spark-worker' or app == 'group2-custom-worker':
        return len([k for k, v in podSeen[app].items() if v == 'running'])
    else:
        return 9999


def nodes_available():
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

    print("assign {} to {}".format(body.target.name, body.metadata.name))
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
        name = pod.metadata.name
        label = pod.metadata.labels['run']
        # labels = {group2-custom-worker, group2-spark-worker}
        if label not in podSeen:
            podSeen[label] = dict()
        print("\nGot a pod - name: {}\tphase: {}\tscheduler_name: {}\tlabel: {}".format(name, pod.status.phase, pod.spec.scheduler_name, label))

        #phase = Pending / Running / Succeeded / Failed / Unknown
        if pod.status.phase == 'Succeeded':
            podSeen[label][name] = 'Succeeded'
        elif pod.status.phase == 'Failed':
            print("Failed Pod received, Deleting this pod")
            podSeen[label][name] = 'Deleted'
            try:
                v1.delete_namespaced_pod(pod.metadata.name, 'default', delete_option)
            except client.rest.ApiException:
                print('ApiException as expected for double deleting')
        elif pod.status.phase == "Pending" and pod.spec.scheduler_name == scheduler_name:
            log.write("okay on this pod, let's start \n")
            log.flush()
            #wait if we don't see both drivers for custom & spark
            fileSizes = []
            while True:
                fileSizes = getFileSizes()
                if fileSizes[0] == 0:
                    print("filesizes: {}, falling asleep".format(fileSizes))
                    time.sleep(5)
                else:
                    break
            #check if there's already enough workers or not
            war = workersAlreadyRunning(label)
            print("{} workers already running".format(war))
            if war < workersAllowed(label, [200, 500]):
                if name not in podSeen[label]:
                    print("okay I can assign a node")
                    nodesAvailable = nodes_available()
                    try:
                        node = random.choice(nodesAvailable)
                        podSeen[label][name] = 'running'
                        res = scheduler(name, node)
                    except client.rest.ApiException as e:
                        log.write(json.loads(e.body)['message'])
                        log.flush()
                elif podSeen[label][name] == 'running':
                    print("I already assigned this pod a node before but this came again, but I shouldn't delete it")
                else:
                    print("I've seen this pod before, I marked it either deleted or succeeded but it came again pending. This shouldn't really happen.")
            else:
                print("I shouldn't assign a node")
                if name not in podSeen[label]:
                    print("Deleting this pod")
                    podSeen[label][name] = 'Deleted'
                    try:
                        v1.delete_namespaced_pod(pod.metadata.name, 'default', delete_option)
                    except client.rest.ApiException:
                        print('ApiException as expected for double deleting')

if __name__ == '__main__':
    main()
    log.close()

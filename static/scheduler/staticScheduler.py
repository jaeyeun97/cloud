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
    log.write("trying to get filesizes\n")
    log.flush()
    custom_master_pods = v1.list_namespaced_pod("default", label_selector="run==group2-custom-master", limit=1).items
    spark_master_pods = v1.list_namespaced_pod("default", label_selector="run==group2-spark-master", limit=1).items
    log.write("length of cmp: {}\n".format(len(list(custom_master_pods))))
    log.write("length of smp: {}\n".format(len(list(spark_master_pods))))
    for v in custom_master_pods:
        log.write("cmp: appType: {}, phase: {}, inputSize: {}\n".format(v.metadata.labels["appType"], v.status.phase, v.metadata.labels["inputSize"]))
    log.flush()
    #TODO: change or to and for running both custom and spark
    if len(list(custom_master_pods)) == 1 or len(list(spark_master_pods)) == 1:
        return [0, int(spark_master_pods[0].metadata.labels["inputSize"])]
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
        return 3
    else:
        return 4

def workersAlreadyRunning(app):  # app = 'spark' or 'custom'
<<<<<<< HEAD
    if app == 'group2_spark_worker' or app == 'group2_custom_worker':
=======
    if app == 'group2-spark-worker' or app == 'group2-custom-worker':
>>>>>>> cada79a1e59344cc7e6a446048f02d1cab903672
        return len([k for k, v in podSeen[app].items() if v == 'running'])
    else:
        return 9999


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
        print("Got a pod - name: {}\tphase: {}\tscheduler_name: {}\tlabel: {} \n".format(name, pod.status.phase, pod.spec.scheduler_name, label))

        #phase = Pending / Running / Succeeded / Failed / Unknown
        if pod.status.phase == 'Succeeded':
            podSeen[label][name] = 'Succeeded'
        elif pod.status.phase == "Pending" and pod.spec.scheduler_name == scheduler_name:
            log.write("okay on this pod, let's start \n")
            log.flush()
            #wait if we don't see both drivers for custom & spark
            fileSizes = []
            while True:
                fileSizes = getFileSizes()
                if fileSizes[1] == 0:
                    print("filesizes: {}".format(fileSizes))
                    print("falling asleep\n")
                    time.sleep(5)
                else:
                    break
            #check if there's already enough workers or not
            war = workersAlreadyRunning(label)
            print("{} workers already running".format(war))
            if war < workersAllowed(label, [200, 500]):
                print("okay I can assign a node\n")
                nodesAvailable = nodes_available()
                try:
                    node = random.choice(nodesAvailable)
                    podSeen[label][name] = 'running'
                    res = scheduler(name, node)
                except client.rest.ApiException as e:
                    log.write(json.loads(e.body)['message'])
                    log.flush()
            else:
                print("I shouldn't assign a node")
                if name not in podSeen[label]:
                    podSeen[label][name] = 'Deleted'
                    v1.delete_namespaced_pod(pod.metadata.name, 'default', delete_option)


if __name__ == '__main__':
    main()
    log.close()

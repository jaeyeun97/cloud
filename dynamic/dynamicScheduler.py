import time
import random
import json

from kubernetes import client, config, watch

log = open('log.txt', 'w+')

config.load_incluster_config()
v1 = client.CoreV1Api()
delete_option = client.V1DeleteOptions()

scheduler_name = "staticscheduler"

def delete(name):
    try:
        v1.delete_namespaced_pod(name, 'default', delete_option)
    except ApiException:
        print('ApiException as expected for double deleting')

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
    except client.rest.ApiException:
        print('ValueError as Expected')
    finally:
        return res

def main():
    customChunks = 0
    customCount = 0
    assigned = list()
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "default"):
        pod = event['object']
        name = pod.metadata.name
        label = pod.metadata.labels['run']
        # labels = {group2-custom-worker, group2-spark-worker}
        print("\nGot a pod - name: {}\tphase: {}\tscheduler_name: {}\tlabel: {}".format(name, pod.status.phase, pod.spec.scheduler_name, label))

        #phase = Pending / Running / Succeeded / Failed / Unknown
        if pod.status.phase == "Pending" and pod.spec.scheduler_name == scheduler_name:
            #TODO: accept custom first
            if customCount <= customCount:
                if label == 'group2-custom-worker':
                    customChunks = int(pod.metadata.labels['chunkSize'])
                    customCount += 1
                else:
                    print("We need to assign custom workers first")
                    delete(pod.metadata.name)
                    continue

            #Do assignment as usual
            nodesAvailable = nodes_available()
            if len(nodesAvailable) != 0 and name not in assigned:
                print("Okay there are spare node(s)")
                try:
                    node = random.choice(nodesAvailable)
                    assigned.append(name)
                    res = scheduler(name, node)
                except client.rest.ApiException as e:
                    log.write(json.loads(e.body)['message'])
                    log.flush()
            else:
                print("There are no spare nodes or I already assigned a node to this pod")
                delete(pod.metadata.name)

        elif pod.status.phase == 'Failed' or pod.status.phase = 'Succeeded':
            print("Pod {}, Deleting this pod".format(pod.status.phase))
            delete(pod.metadata.name)

if __name__ == '__main__':
    main()
    log.close()

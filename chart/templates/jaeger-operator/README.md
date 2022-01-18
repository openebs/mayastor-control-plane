# Using Jaegertracing

To deploy a control-plane enabled jaeger on k8s, please set the following in the helm chart:
```yaml
base:
  jaeger:
    enabled: true
```
One way of doing it is by using the `-s` option with the generate-deploy-yamls script, eg:
```shell
$REPO_ROOT/scripts/generate-deploy-yamls.sh -s base.jaeger.enabled=true develop
```

You can now install mayastor+control plane as you'd usually do, except the control plane yaml files now have an init
container (which can be disabled through `base.jaeger.initContainer=false`) which waits for the jaeger agent port to
be ready.

For this to happen, we need to install jaeger!
We do so by applying the yaml files from the $REPO_ROOT/deploy/jaeger-operator directory, which include the jaeger-operator
itself and the file `jaeger.yaml` which tells the operator what kind of jaeger deployment we want, example:
```yaml
---
# Source: mayastor-control-plane/templates/jaeger-operator/jaeger.yaml
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger
  namespace: mayastor
spec:
  strategy: allInOne
  ingress:
    enabled: false
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: node-role.kubernetes.io/master
            operator: In
            values:
            - ""
  tolerations:
    - key: node-role.kubernetes.io/master
  query:
    serviceType: NodePort
    nodePort: 30012
  storage:
    type: memory
    options:
      memory:
        max-traces: 100000
```

In this case, we're configuring it to use an allInOne strategy (means all jaeger components live in a single pod) with
an in-memory database of up to 100000 traces.
For more information, please refer to: `https://github.com/jaegertracing/jaeger-operator`.
For the helm chart configuration: `https://github.com/jaegertracing/helm-charts/tree/main/charts/jaeger-operator`.

And so, let's install Jaeger (this time for real):
```shell
kubectl create -f $REPO_ROOT/deploy/jaeger-operator
```

This will install the jaeger-operator which will parse our jaeger.yaml definition and will then install an AllInOne
deployment. You can get the state of the AllInOne deployment this way:
```shell
> kubectl -n mayastor get Jaeger
NAME     STATUS    VERSION   STRATEGY   STORAGE   AGE
jaeger   Running   1.24.0    allinone   memory    24s
```
Once it's running, the control plane components should now also start to run (if you're using initContainers).

All that's left to do is to install pools and pvc's, as you'd normally do, to generate some traffic.
And then you should be able to access the traces using the NodePort service, eg:
```shell
echo $(kubectl get pods -l app.kubernetes.io/instance=mayastor -lapp.kubernetes.io/name=jaeger-operator -n mayastor -ojsonpath='{.items[0].status.hostIP}')
curl $(kubectl get pods -l app.kubernetes.io/instance=mayastor -lapp.kubernetes.io/name=jaeger-operator -n mayastor -ojsonpath='{.items[0].status.hostIP}'):30012
```
Now, you can use a real browser and access $NODE_IP:30012 and starting digging in the traces...
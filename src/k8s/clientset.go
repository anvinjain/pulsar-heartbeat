//
//  Copyright (c) 2020-2021 Datastax, Inc.
//
//
//
//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
//

package k8s

import (
	"context"
	"fmt"
	"github.com/datastax/pulsar-heartbeat/src/util"
	"os"
	"path/filepath"

	log "github.com/apex/log"
	apps_v1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"
	core_v1 "k8s.io/api/core/v1"
	ext_v1beta1 "k8s.io/api/extensions/v1beta1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

const (
	// DefaultPulsarNamespace is the default pulsar namespace in the cluster
	DefaultPulsarNamespace = "pulsar"

	// ZookeeperSts is zookeeper sts name
	ZookeeperSts = "zookeeper"

	// BookkeeperSts is bookkeeper sts name
	BookkeeperSts = "bookie"

	// BrokerDeployment is the broker deployment name
	BrokerDeployment = "broker"

	// BrokerSts is the broker deployment name
	BrokerSts = "brokersts"

	// ProxyDeployment is the proxy deployment name
	ProxyDeployment = "proxy"

	// FunctionWorkerDeployment is the function worker deployment name
	FunctionWorkerDeployment = "functionWorker"
)

// ClusterStatusCode is the high level health of cluster status
type ClusterStatusCode int

const (
	// TotalDown is the initial status
	TotalDown ClusterStatusCode = iota

	// OK is the healthy status
	OK

	// PartialReady is some parts of system are ok
	PartialReady
)

type Config struct {
	PulsarNamespace 	string		`json:"pulsarNamespace"`
	BrokerStsLabel		string		`json:"brokerStsLabel"`
	BrokerDepLabel		string		`json:"brokerDepLabel"`
	ProxyDepLabel		string		`json:"proxyDepLabel"`
	ZookeeperLabel		string		`json:"zookeeperLabel"`
	BookkeeperLabel		string		`json:"bookkeeperLabel"`
	BkWriteQuorum		int32		`json:"bkWriteQuorum"`
}

func (c *Config) sanitize () {
	c.PulsarNamespace = util.AssignString(c.PulsarNamespace, DefaultPulsarNamespace)
	c.BrokerStsLabel = util.AssignString(c.BrokerStsLabel, BrokerSts)
	c.BrokerDepLabel = util.AssignString(c.BrokerDepLabel, BrokerDeployment)
	c.ProxyDepLabel = util.AssignString(c.ProxyDepLabel, ProxyDeployment)
	c.ZookeeperLabel = util.AssignString(c.ZookeeperLabel, ZookeeperSts)
	c.BookkeeperLabel = util.AssignString(c.BookkeeperLabel, BookkeeperSts)

	if c.BkWriteQuorum <= 0 {
		c.BkWriteQuorum = 2
	}
}

// Client is the k8s client object
type Client struct {
	Clientset        *kubernetes.Clientset
	Metrics          *metrics.Clientset
	ClusterName      string
	Status           ClusterStatusCode
	Zookeeper        StatefulSet
	Bookkeeper       StatefulSet
	BrokerSts        StatefulSet
	Broker           Deployment
	Proxy            Deployment
	FunctionWorker   StatefulSet
	*Config
}

// ClusterStatus is the health status of the cluster and its components
type ClusterStatus struct {
	ZookeeperOfflineInstances  int
	BookkeeperOfflineInstances int
	BrokerOfflineInstances     int
	BrokerStsOfflineInstances  int
	ProxyOfflineInstances      int
	Status                     ClusterStatusCode
}

// Deployment is the k8s deployment
type Deployment struct {
	Name      string
	Replicas  int32
	Instances int32
}

// StatefulSet is the k8s sts
type StatefulSet struct {
	Name      string
	Replicas  int32
	Instances int32
}

// GetK8sClient gets k8s clientset
func GetK8sClient(k8sCfg Config) (*Client, error) {
	var config *rest.Config
	k8sCfg.sanitize()

	if home := homedir.HomeDir(); home != "" {
		// TODO: add configuration to allow customized config file
		kubeconfig := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(kubeconfig); os.IsNotExist(err) {
			log.Infof("this is an in-cluster k8s monitor, pulsar namespace %s", k8sCfg.PulsarNamespace)
			if config, err = rest.InClusterConfig(); err != nil {
				return nil, err
			}

		} else {
			log.Infof("this is outside of k8s cluster monitor, kubeconfig dir %s, pulsar namespace %s", kubeconfig, k8sCfg.PulsarNamespace)
			if config, err = clientcmd.BuildConfigFromFlags("", kubeconfig); err != nil {
				return nil, err
			}
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	metrics, err := metrics.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	client := Client{
		Clientset: clientset,
		Metrics:   metrics,
		Config: &k8sCfg,
	}

	err = client.UpdateReplicas()
	if err != nil {
		return nil, err
	}
	return &client, nil
}

func buildInClusterConfig() kubernetes.Interface {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Can not get kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Can not create kubernetes client: %v", err)
	}

	return clientset
}

// UpdateReplicas updates the replicas for deployments and sts
func (c *Client) UpdateReplicas() error {
	brokersts, err := c.getStatefulSets(c.PulsarNamespace, c.BrokerStsLabel)
	if err != nil {
		return err
	}
	if len(brokersts.Items) == 0 {
		c.BrokerSts.Replicas = 0
	} else {
		c.BrokerSts.Replicas = *(brokersts.Items[0]).Spec.Replicas
	}

	broker, err := c.getDeployments(c.PulsarNamespace, c.BrokerDepLabel)
	if err != nil {
		return err
	}
	if len(broker.Items) == 0 {
		c.Broker.Replicas = 0
	} else {
		c.Broker.Replicas = *(broker.Items[0]).Spec.Replicas
	}

	proxy, err := c.getDeployments(c.PulsarNamespace, c.ProxyDepLabel)
	if err != nil {
		return err
	}
	if len(proxy.Items) == 0 {
		c.Proxy.Replicas = 0
	} else {
		c.Proxy.Replicas = *(proxy.Items[0]).Spec.Replicas
	}

	zk, err := c.getStatefulSets(c.PulsarNamespace, c.ZookeeperLabel)
	if err != nil {
		return err
	}
	c.Zookeeper.Replicas = *(zk.Items[0]).Spec.Replicas

	bk, err := c.getStatefulSets(c.PulsarNamespace, c.BookkeeperLabel)
	if err != nil {
		return err
	}
	c.Bookkeeper.Replicas = *(bk.Items[0]).Spec.Replicas

	return nil
}

// WatchPods watches the running pods vs intended replicas
func (c *Client) WatchPods() error {

	if counts, err := c.runningPodCounts(c.PulsarNamespace, c.ZookeeperLabel); err == nil {
		c.Zookeeper.Instances = int32(counts)
	} else {
		return err
	}

	if counts, err := c.runningPodCounts(c.PulsarNamespace, c.BookkeeperLabel); err == nil {
		c.Bookkeeper.Instances = int32(counts)
	} else {
		return err
	}

	if c.Broker.Replicas > 0 {
		if counts, err := c.runningPodCounts(c.PulsarNamespace, c.BrokerDepLabel); err == nil {
			c.Broker.Instances = int32(counts)
		} else {
			return err
		}
	}

	if c.BrokerSts.Replicas > 0 {
		if counts, err := c.runningPodCounts(c.PulsarNamespace, c.BrokerStsLabel); err == nil {
			c.BrokerSts.Instances = int32(counts)
		} else {
			return err
		}
	}

	if c.Proxy.Replicas > 0 {
		if counts, err := c.runningPodCounts(c.PulsarNamespace, c.ProxyDepLabel); err == nil {
			c.Proxy.Instances = int32(counts)
		} else {
			return err
		}
	}
	return nil
}

// EvalHealth evaluate the health of cluster status
func (c *Client) EvalHealth() (string, ClusterStatus) {
	health := ""
	status := ClusterStatus{
		ZookeeperOfflineInstances:  int(c.Zookeeper.Replicas - c.Zookeeper.Instances),
		BookkeeperOfflineInstances: int(c.Bookkeeper.Replicas - c.Bookkeeper.Instances),
		BrokerOfflineInstances:     int(c.Broker.Replicas - c.Broker.Instances),
		BrokerStsOfflineInstances:  int(c.BrokerSts.Replicas - c.BrokerSts.Instances),
		ProxyOfflineInstances:      int(c.Proxy.Replicas - c.Proxy.Instances),
		Status:                     OK,
	}
	if c.Zookeeper.Instances <= (c.Zookeeper.Replicas / 2) {
		health = fmt.Sprintf("\nCluster error - zookeeper is running %d instances out of %d replicas", c.Zookeeper.Instances, c.Zookeeper.Replicas)
		status.Status = TotalDown
	} else if c.Zookeeper.Instances < c.Zookeeper.Replicas {
		health = fmt.Sprintf("\nCluster warning - zookeeper is running %d instances out of %d", c.Zookeeper.Instances, c.Zookeeper.Replicas)
		status.Status = PartialReady
	}

	if c.Bookkeeper.Instances < c.BkWriteQuorum {
		health = health + fmt.Sprintf("\nCluster error - bookkeeper is running %d instances out of %d replicas", c.Bookkeeper.Instances, c.Bookkeeper.Replicas)
		status.Status = TotalDown
	} else if c.Bookkeeper.Instances < c.Bookkeeper.Replicas {
		health = health + fmt.Sprintf("\nCluster warning - bookkeeper is running %d instances out of %d", c.Bookkeeper.Instances, c.Bookkeeper.Replicas)
		status.Status = updateStatus(status.Status, PartialReady)
	}

	if (c.Broker.Instances + c.BrokerSts.Instances) == 0 {
		health = health + fmt.Sprintf("\nCluster error - no broker instances is running")
		status.Status = TotalDown
	} else if c.Broker.Instances < c.Broker.Replicas {
		health = fmt.Sprintf("\nCluster warning - broker is running %d instances out of %d", c.Broker.Instances, c.Broker.Replicas)
		status.Status = updateStatus(status.Status, PartialReady)
	}

	if c.BrokerSts.Replicas > 0 && c.BrokerSts.Instances == 0 {
		health = health + fmt.Sprintf("\nCluster error - broker stateful has no running instances out of %d replicas", c.Broker.Replicas)
		status.Status = TotalDown
	} else if status.BrokerStsOfflineInstances > 0 {
		health = health + fmt.Sprintf("\nCluster error - broker statefulset only has %d running instances out of %d replicas", c.BrokerSts.Instances, c.BrokerSts.Replicas)
		status.Status = TotalDown
	}

	if c.Proxy.Replicas > 0 && c.Proxy.Instances == 0 {
		health = health + fmt.Sprintf("\nCluster error - proxy has no running instances out of %d replicas", c.Proxy.Replicas)
		status.Status = TotalDown
	} else if c.Proxy.Replicas > 0 && c.Proxy.Instances < c.Proxy.Replicas {
		health = health + fmt.Sprintf("\nCluster warning - proxy is running %d instances out of %d", c.Proxy.Instances, c.Proxy.Replicas)
		status.Status = updateStatus(status.Status, PartialReady)
	}
	c.Status = status.Status
	return health, status
}

func updateStatus(original, current ClusterStatusCode) ClusterStatusCode {
	if current == TotalDown || original == TotalDown {
		return TotalDown
	} else if current == PartialReady || original == PartialReady {
		return PartialReady
	}
	return current
}

// WatchPodResource watches pod's resource
func (c *Client) WatchPodResource(namespace, component string) error {
	podMetrics, err := c.Metrics.MetricsV1beta1().PodMetricses(namespace).List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
	if err != nil {
		return err
	}
	for _, podMetric := range podMetrics.Items {
		podContainers := podMetric.Containers
		for _, container := range podContainers {
			cpuQuantity := container.Usage.Cpu().AsDec()
			memQuantity, _ := container.Usage.Memory().AsInt64()

			msg := fmt.Sprintf("Container Name: %s \n CPU usage: %v \n Memory usage: %d", container.Name, cpuQuantity, memQuantity)
			fmt.Println(msg)
		}

	}
	return nil
}

func (c *Client) runningPodCounts(namespace, component string) (int, error) {
	pods, err := c.Clientset.CoreV1().Pods(namespace).List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
	if err != nil {
		return -1, err
	}

	counts := 0
	for _, item := range pods.Items {
		containers := 0
		readyContainers := 0
		for _, status := range item.Status.ContainerStatuses {
			// status.Name is container name
			if status.Ready {
				readyContainers++
			}
			containers++
		}
		if containers == readyContainers {
			counts++
		}
	}

	return counts, nil
}

// GetNodeResource gets the node total available memory
func (c *Client) GetNodeResource() {
	nodeList, err := c.Clientset.CoreV1().Nodes().List(context.TODO(), meta_v1.ListOptions{})

	if err == nil {
		if len(nodeList.Items) > 0 {
			node := &nodeList.Items[0]
			memQuantity := node.Status.Allocatable[core_v1.ResourceMemory] // "memory"
			totalMemAvail := int(memQuantity.Value() >> 20)
			fmt.Printf("total memory %d", totalMemAvail)
		} else {
			log.Fatal("Unable to read node list")
			return
		}
	} else {
		log.Fatalf("Error while reading node list data: %v", err)
	}
}

func (c *Client) getDeployments(namespace, component string) (*v1.DeploymentList, error) {
	deploymentsClient := c.Clientset.AppsV1().Deployments(namespace)

	return deploymentsClient.List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
}

func (c *Client) getStatefulSets(namespace, component string) (*v1.StatefulSetList, error) {
	stsClient := c.Clientset.AppsV1().StatefulSets(namespace)

	return stsClient.List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
}

// ClusterStatusCodeString is the cluster status code in Kubernetes
func ClusterStatusCodeString(status ClusterStatusCode) string {
	switch status {
	case TotalDown:
		return "Down"
	case OK:
		return "OK"
	case PartialReady:
		return "PartialReady"
	default:
		return "invalid"
	}
}

// GetObjectMetaData returns metadata of a given k8s object
func GetObjectMetaData(obj interface{}) meta_v1.ObjectMeta {

	var objectMeta meta_v1.ObjectMeta

	switch object := obj.(type) {
	case *apps_v1.Deployment:
		objectMeta = object.ObjectMeta
	case *core_v1.ReplicationController:
		objectMeta = object.ObjectMeta
	case *apps_v1.ReplicaSet:
		objectMeta = object.ObjectMeta
	case *apps_v1.DaemonSet:
		objectMeta = object.ObjectMeta
	case *core_v1.Service:
		objectMeta = object.ObjectMeta
	case *core_v1.Pod:
		objectMeta = object.ObjectMeta
	case *batch_v1.Job:
		objectMeta = object.ObjectMeta
	case *core_v1.PersistentVolume:
		objectMeta = object.ObjectMeta
	case *core_v1.Namespace:
		objectMeta = object.ObjectMeta
	case *core_v1.Secret:
		objectMeta = object.ObjectMeta
	case *ext_v1beta1.Ingress:
		objectMeta = object.ObjectMeta
	}
	return objectMeta
}

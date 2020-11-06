package k8pool

import (
	"context"
	"fmt"
	"reflect"

	api_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type PeerInfo struct {
	// (Optional) The name of the data center this peer is in. Leave blank if not using multi data center support.
	DataCenter string

	// (Required) The ip address of the peer
	IPAddress string

	// (Optional) The http address:port of the peer
	HTTPAddress string

	// (Optional) The grpc address:port of the peer
	GRPCAddress string

	// (Optional) Is true if PeerInfo is for this instance of app
	IsOwner bool
}

type UpdateFunc func([]PeerInfo)

type Pool struct {
	informer cache.SharedIndexInformer
	client   *kubernetes.Clientset
	log      logger
	conf     Config
	done     chan struct{}
}

type Config struct {
	Logger    logger
	OnUpdate  UpdateFunc
	Namespace string
	Selector  string
	PodIP     string
	PodPort   string
}

func New(conf Config) (*K8sPool, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	// creates the client
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	pool := &Pool{
		done:   make(chan struct{}),
		log:    conf.Logger,
		client: client,
		conf:   conf,
	}

	return pool, pool.start()
}

func (e *K8sPool) start() error {

	e.informer = cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = e.conf.Selector
				return e.client.CoreV1().Endpoints(e.conf.Namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = e.conf.Selector
				return e.client.CoreV1().Endpoints(e.conf.Namespace).Watch(context.TODO(), options)
			},
		},
		&api_v1.Endpoints{},
		0, //Skip resync
		cache.Indexers{},
	)

	e.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			e.log.Debugf("Queue (Add) '%s' - %s", key, err)
			if err != nil {
				e.log.Errorf("while calling MetaNamespaceKeyFunc(): %s", err)
				return
			}
		},
		UpdateFunc: func(obj, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			e.log.Debugf("Queue (Update) '%s' - %s", key, err)
			if err != nil {
				e.log.Errorf("while calling MetaNamespaceKeyFunc(): %s", err)
				return
			}
			e.updatePeers()
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			e.log.Debugf("Queue (Delete) '%s' - %s", key, err)
			if err != nil {
				e.log.Errorf("while calling MetaNamespaceKeyFunc(): %s", err)
				return
			}
			e.updatePeers()
		},
	})

	go e.informer.Run(e.done)

	if !cache.WaitForCacheSync(e.done, e.informer.HasSynced) {
		close(e.done)
		return fmt.Errorf("timed out waiting for caches to sync")
	}

	return nil
}

func (e *K8sPool) updatePeers() {
	e.log.Debug("Fetching peer list from endpoints API")
	var peers []PeerInfo
	for _, obj := range e.informer.GetStore().List() {
		endpoint, ok := obj.(*api_v1.Endpoints)
		if !ok {
			e.log.Errorf("expected type v1.Endpoints got '%s' instead", reflect.TypeOf(obj).String())
		}

		for _, s := range endpoint.Subsets {
			for _, addr := range s.Addresses {
				// TODO(thrawn01): Might consider using the `namespace` as the `DataCenter`. We should
				//  do what ever k8s convention is for identifying a k8s cluster within a federated multi-data
				//  center setup.
				peer := PeerInfo{
					IPAddress:   fmt.Sprintf("%s", addr.IP),
					HTTPAddress: fmt.Sprintf("http://%s:%s", addr.IP, e.conf.PodPort),
					GRPCAddress: fmt.Sprintf("%s:%s", addr.IP, e.conf.PodPort),
				}

				if addr.IP == e.conf.PodIP {
					peer.IsOwner = true
				}
				peers = append(peers, peer)
				e.log.Debugf("Peer: %+v\n", peer)
			}
		}
	}
	e.conf.OnUpdate(peers)
}

func (e *K8sPool) Close() {
	close(e.done)
}

func isPodReady(pod *api_v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == api_v1.PodReady && condition.Status == api_v1.ConditionTrue {
			return true
		}
	}
	return false
}

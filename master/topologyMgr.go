package main

import (
	"encoding/json"
	"sync"

	. "github.com/smartfog/fogflow/common/config"
	. "github.com/smartfog/fogflow/common/datamodel"
	. "github.com/smartfog/fogflow/common/ngsi"
)

type ProcessingPlane struct {
	Intent *ServiceIntent // orchestration intent issued by external applications

	ExecutionPlan  []*TaskInstance          // represent the derived execution plan
	DeploymentPlan []*ScheduledTaskInstance // represent the derived deployment plan
}

type TopologyMgr struct {
	master *Master

	//list of all submitted topologies
	topologyList      map[string]*Topology
	topologyList_lock sync.RWMutex

	// list of all service intents and also the mapping between service intent and task intents
	serviceIntentMap map[string]*ServiceIntent
	service2TaskMap  map[string][]string
}

func NewTopologyMgr(myMaster *Master) *TopologyMgr {
	return &TopologyMgr{master: myMaster}
}

func (tMgr *TopologyMgr) Init() {
	tMgr.topologyList = make(map[string]*Topology)

}

//
// update the execution plan and deployment plan according to the system changes
//
func (tMgr *TopologyMgr) handleTopologyUpdate(topologyCtxObj *ContextObject) {
	topology := Topology{}
	jsonText, _ := json.Marshal(topologyCtxObj.Attributes["template"].Value.(map[string]interface{}))
	err := json.Unmarshal(jsonText, &topology)
	if err == nil {
		INFO.Println(topology)
		tMgr.topologyList_lock.Lock()
		tMgr.topologyList[topology.Name] = &topology
		tMgr.topologyList_lock.Unlock()

		INFO.Println(topology)
	}
}

func (tMgr *TopologyMgr) handleServiceIntentUpdate(intentCtxObj *ContextObject) {
	INFO.Println("handle intent update")
	INFO.Println(intentCtxObj)

	status := intentCtxObj.Attributes["status"].Value

	sIntent := ServiceIntent{}
	jsonText, _ := json.Marshal(intentCtxObj.Attributes["intent"].Value.(map[string]interface{}))
	err := json.Unmarshal(jsonText, &sIntent)
	if err == nil {
		INFO.Println(sIntent)
	} else {
		INFO.Println(err)
	}

	// attached the entityID as the ID of this service intent
	sIntent.ID = intentCtxObj.Entity.ID

	if status == "remove" {
		tMgr.removeServiceIntent()
	} else {
		tMgr.handleServiceIntent(&sIntent)
	}
}

//
// to break down the service intent from the service level into the task level
//
func (tMgr *TopologyMgr) handleServiceIntent(serviceIntent *ServiceIntent) {
	INFO.Println("receive a service intent")
	INFO.Println(serviceIntent)

	// find the required topology object
	tMgr.topologyList_lock.RLock()
	serviceIntent.TopologyObject = tMgr.topologyList[serviceIntent.TopologyName]
	tMgr.topologyList_lock.RUnlock()

	for _, task := range serviceIntent.TopologyObject.Tasks {

		// to handle the task intent directly
		taskIntent := TaskIntent{}

		taskIntent.GeoScope = serviceIntent.GeoScope
		taskIntent.Priority = serviceIntent.Priority
		taskIntent.QoS = serviceIntent.QoS
		taskIntent.TaskObject = task

		if taskIntent.TaskObject.CanBeDivided() == true {
			tMgr.intentPartition(&taskIntent)
		} else {
			tMgr.master.taskMgr.handleTaskIntent(&taskIntent)
		}
	}
}

//
// to divide the task intent for all sites in this geoscope
//
func (tMgr *TopologyMgr) intentPartition(taskIntent *TaskIntent) {
	var geoscope = taskIntent.GeoScope

	client := NGSI9Client{IoTDiscoveryURL: tMgr.master.discoveryURL}
	siteList, err := client.QuerySiteList(geoscope)

	if err != nil {
		ERROR.Println("error happens when querying the site list from IoT Discovery")
		ERROR.Println(err)
	} else {
		for _, site := range siteList {
			if site.IsLocalSite == true {
				intent := TaskIntent{}

				scope := OperationScope{}
				scope.Type = "local"

				intent.GeoScope = scope
				intent.Priority = taskIntent.Priority
				intent.QoS = taskIntent.QoS
				intent.ServiceName = taskIntent.ServiceName
				intent.TaskObject = taskIntent.TaskObject

				// handle a sub-intent locally
				tMgr.master.taskMgr.handleTaskIntent(&intent)
			} else {
				// forward a sub-intent to the remote site
				intent := TaskIntent{}

				scope := OperationScope{}
				scope.Type = "local"

				intent.GeoScope = scope
				intent.Priority = taskIntent.Priority
				intent.QoS = taskIntent.QoS
				intent.ServiceName = taskIntent.ServiceName
				intent.TaskObject = taskIntent.TaskObject

				tMgr.ForwardIntent2RemoteSite(&intent, site)
			}
		}
	}
}

func (tMgr *TopologyMgr) ForwardIntent2RemoteSite(taskIntent *TaskIntent, site SiteInfo) {
	brokerURL := "http://" + site.ExternalAddress + "/proxy"

	ctxElem := ContextElement{}
	ctxElem.Entity.ID = ""
	ctxElem.Entity.Type = "TaskIntent"

	ctxElem.Attributes = make([]ContextAttribute, 0)

	attribute := ContextAttribute{}
	attribute.Type = "object"
	attribute.Name = "intent"
	attribute.Value = taskIntent

	ctxElem.Attributes = append(ctxElem.Attributes, attribute)

	client := NGSI10Client{IoTBrokerURL: brokerURL}
	client.UpdateContext(&ctxElem)
}

func (tMgr *TopologyMgr) removeServiceIntent() {

}

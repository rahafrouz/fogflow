package main

import (
	"encoding/json"
	"sync"

	. "github.com/smartfog/fogflow/common/config"
	. "github.com/smartfog/fogflow/common/datamodel"
	. "github.com/smartfog/fogflow/common/ngsi"
)

type ServiceMgr struct {
	master *Master

	// list of all service intents and also the mapping between service intent and task intents
	serviceIntentMap map[string]*ServiceIntent
	service2TaskMap  map[string][]string
	intentList_lock  sync.RWMutex
}

func NewServiceMgr(myMaster *Master) *ServiceMgr {
	return &ServiceMgr{master: myMaster}
}

func (sMgr *ServiceMgr) Init() {
	sMgr.serviceIntentMap = make(map[string]*ServiceIntent)
	sMgr.service2TaskMap = make(map[string][]string)
}

func (sMgr *ServiceMgr) handleServiceIntentUpdate(intentCtxObj *ContextObject) {
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
		sMgr.removeServiceIntent(sIntent.ID)
	} else {
		sMgr.handleServiceIntent(&sIntent)
	}
}

//
// to break down the service intent from the service level into the task level
//
func (sMgr *ServiceMgr) handleServiceIntent(serviceIntent *ServiceIntent) {
	INFO.Println("receive a service intent")

	serviceIntent.TopologyObject = sMgr.master.getTopologyByName(serviceIntent.TopologyName)

	INFO.Println(serviceIntent)

	for _, task := range serviceIntent.TopologyObject.Tasks {

		// to handle the task intent directly
		taskIntent := TaskIntent{}

		taskIntent.GeoScope = serviceIntent.GeoScope
		taskIntent.Priority = serviceIntent.Priority
		taskIntent.QoS = serviceIntent.QoS
		taskIntent.ServiceName = serviceIntent.TopologyName
		taskIntent.TaskObject = task

		INFO.Println(taskIntent)

		if taskIntent.TaskObject.CanBeDivided() == true {
			sMgr.intentPartition(&taskIntent)
		} else {
			sMgr.master.taskMgr.handleTaskIntent(&taskIntent)
		}

		// to record the task intents for this high level service intent

	}
}

//
// to divide the task intent for all sites in this geoscope
//
func (sMgr *ServiceMgr) intentPartition(taskIntent *TaskIntent) {
	var geoscope = taskIntent.GeoScope

	client := NGSI9Client{IoTDiscoveryURL: sMgr.master.discoveryURL}
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
				sMgr.master.taskMgr.handleTaskIntent(&intent)
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

				sMgr.ForwardIntent2RemoteSite(&intent, site)
			}
		}
	}
}

func (sMgr *ServiceMgr) ForwardIntent2RemoteSite(taskIntent *TaskIntent, site SiteInfo) {
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

func (sMgr *ServiceMgr) removeServiceIntent(id string) {
	INFO.Printf("the master is going to remove the requested service intent %s\n", id)
}

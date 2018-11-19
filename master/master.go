package main

import (
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"time"

	. "github.com/smartfog/fogflow/common/communicator"
	. "github.com/smartfog/fogflow/common/datamodel"
	. "github.com/smartfog/fogflow/common/ngsi"

	. "github.com/smartfog/fogflow/common/config"
)

type Master struct {
	cfg *Config

	BrokerURL string

	id           string
	myURL        string
	messageBus   string
	discoveryURL string

	communicator *Communicator
	ticker       *time.Ticker
	agent        *NGSIAgent

	//list of all workers
	workers         map[string]*WorkerProfile
	workerList_lock sync.RWMutex

	//list of all operators
	operatorList      map[string]Operator
	operatorList_lock sync.RWMutex

	//list of all docker images
	dockerImageList      map[string][]DockerImage
	dockerImageList_lock sync.RWMutex

	//to manage the orchestration of service topology
	topologyMgr *TopologyMgr

	//to manage the orchestration of tasks
	taskMgr *TaskMgr

	//type of subscribed entities
	subID2Type map[string]string
}

func (master *Master) Start(configuration *Config) {
	master.cfg = configuration

	master.messageBus = configuration.GetMessageBus()
	master.discoveryURL = configuration.GetDiscoveryURL()

	master.workers = make(map[string]*WorkerProfile)

	master.operatorList = make(map[string]Operator)
	master.dockerImageList = make(map[string][]DockerImage)

	master.subID2Type = make(map[string]string)

	// find a nearby IoT Broker
	for {
		nearby := NearBy{}
		nearby.Latitude = master.cfg.PLocation.Latitude
		nearby.Longitude = master.cfg.PLocation.Longitude
		nearby.Limit = 1

		client := NGSI9Client{IoTDiscoveryURL: master.cfg.GetDiscoveryURL()}
		selectedBroker, err := client.DiscoveryNearbyIoTBroker(nearby)

		if err == nil && selectedBroker != "" {
			master.BrokerURL = selectedBroker
			break
		} else {
			if err != nil {
				ERROR.Println(err)
			}

			INFO.Println("continue to look up a nearby IoT broker")
			time.Sleep(5 * time.Second)
		}
	}

	// initialize the manager for both fog function and service topology
	master.taskMgr = NewTaskMgr(master)
	master.taskMgr.Init()

	master.topologyMgr = NewTopologyMgr(master)
	master.topologyMgr.Init()

	// announce myself to the nearby IoT Broker
	master.registerMyself()

	// start the NGSI agent
	master.agent = &NGSIAgent{Port: configuration.Master.AgentPort}
	master.myURL = "http://" + configuration.InternalIP + ":" + strconv.Itoa(configuration.Master.AgentPort)
	master.agent.Start()
	master.agent.SetContextNotifyHandler(master.onReceiveContextNotify)
	master.agent.SetContextAvailabilityNotifyHandler(master.onReceiveContextAvailability)

	// start the message consumer
	go func() {
		cfg := MessageBusConfig{}
		cfg.Broker = configuration.GetMessageBus()
		cfg.Exchange = "fogflow"
		cfg.ExchangeType = "topic"
		cfg.DefaultQueue = master.id
		cfg.BindingKeys = []string{master.id + ".", "heartbeat.*"}

		// create the communicator with the broker info and topics
		master.communicator = NewCommunicator(&cfg)
		for {
			retry, err := master.communicator.StartConsuming(master.id, master)
			if retry {
				INFO.Printf("Going to retry launching the rabbitmq. Error: %v", err)
			} else {
				INFO.Printf("stop retrying")
				break
			}
		}
	}()

	// start a timer to do something periodically
	master.ticker = time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-master.ticker.C
			master.onTimer()
		}
	}()

	// subscribe to the update of required context information
	master.triggerInitialSubscriptions()
}

func (master *Master) onTimer() {

}

func (master *Master) Quit() {
	INFO.Println("to stop the master")
	master.unregisterMyself()
	master.communicator.StopConsuming()
	master.ticker.Stop()
	INFO.Println("stop consuming the messages")
}

func (master *Master) registerMyself() {
	ctxObj := ContextObject{}

	ctxObj.Entity.ID = master.id
	ctxObj.Entity.Type = "Master"
	ctxObj.Entity.IsPattern = false

	ctxObj.Metadata = make(map[string]ValueObject)

	mylocation := Point{}
	mylocation.Latitude = master.cfg.PLocation.Latitude
	mylocation.Longitude = master.cfg.PLocation.Longitude
	ctxObj.Metadata["location"] = ValueObject{Type: "point", Value: mylocation}

	client := NGSI10Client{IoTBrokerURL: master.BrokerURL}
	err := client.UpdateContextObject(&ctxObj)
	if err != nil {
		ERROR.Println(err)
	}
}

func (master *Master) unregisterMyself() {
	entity := EntityId{}
	entity.ID = master.id
	entity.Type = "Master"
	entity.IsPattern = false

	client := NGSI10Client{IoTBrokerURL: master.BrokerURL}
	err := client.DeleteContext(&entity)
	if err != nil {
		ERROR.Println(err)
	}
}

func (master *Master) triggerInitialSubscriptions() {
	master.subscribeContextEntity("Operator")
	master.subscribeContextEntity("DockerImage")
	master.subscribeContextEntity("Topology")
	master.subscribeContextEntity("ServiceIntent")
	master.subscribeContextEntity("TaskIntent")
}

func (master *Master) subscribeContextEntity(entityType string) {
	subscription := SubscribeContextRequest{}

	newEntity := EntityId{}
	newEntity.Type = entityType
	newEntity.IsPattern = true
	subscription.Entities = make([]EntityId, 0)
	subscription.Entities = append(subscription.Entities, newEntity)
	subscription.Reference = master.myURL

	client := NGSI10Client{IoTBrokerURL: master.BrokerURL}
	sid, err := client.SubscribeContext(&subscription, true)
	if err != nil {
		ERROR.Println(err)
	}
	INFO.Println(sid)

	master.subID2Type[sid] = entityType
}

func (master *Master) onReceiveContextNotify(notifyCtxReq *NotifyContextRequest) {
	sid := notifyCtxReq.SubscriptionId
	stype := master.subID2Type[sid]

	DEBUG.Println("NGSI10 NOTIFY ", sid, " , ", stype)
	contextObj := CtxElement2Object(&(notifyCtxReq.ContextResponses[0].ContextElement))
	DEBUG.Println(contextObj)

	switch stype {
	// registry of an operator
	case "Operator":
		master.handleOperatorRegistration(contextObj)

	// registry of a docker image
	case "DockerImage":
		master.handleDockerImageRegistration(contextObj)

	// topology to define service template
	case "Topology":
		master.topologyMgr.handleTopologyUpdate(contextObj)

	// service orchestration
	case "ServiceIntent":
		master.topologyMgr.handleServiceIntentUpdate(contextObj)

	// task orchestration
	case "TaskIntent":
		master.taskMgr.handleTaskIntentUpdate(contextObj)
	}
}

//
// to handle the registry of operator
//
func (master *Master) handleOperatorRegistration(operatorCtxObj *ContextObject) {
	INFO.Println(operatorCtxObj)

	var operator = Operator{}
	jsonText, _ := json.Marshal(operatorCtxObj.Attributes["operator"].Value.(map[string]interface{}))
	err := json.Unmarshal(jsonText, &operator)
	if err != nil {
		ERROR.Println("failed to read the given operator")
	} else {
		master.operatorList_lock.Lock()
		master.operatorList[operator.Name] = operator
		master.operatorList_lock.Unlock()
	}
}

//
// to handle the management of docker images
//
func (master *Master) handleDockerImageRegistration(dockerImageCtxObj *ContextObject) {
	INFO.Printf("%+v\r\n", dockerImageCtxObj)

	dockerImage := DockerImage{}
	dockerImage.OperatorName = dockerImageCtxObj.Attributes["operator"].Value.(string)
	dockerImage.ImageName = dockerImageCtxObj.Attributes["image"].Value.(string)
	dockerImage.ImageTag = dockerImageCtxObj.Attributes["tag"].Value.(string)
	dockerImage.TargetedHWType = dockerImageCtxObj.Attributes["hwType"].Value.(string)
	dockerImage.TargetedOSType = dockerImageCtxObj.Attributes["osType"].Value.(string)
	dockerImage.Prefetched = dockerImageCtxObj.Attributes["prefetched"].Value.(bool)

	master.dockerImageList_lock.Lock()
	master.dockerImageList[dockerImage.OperatorName] = append(master.dockerImageList[dockerImage.OperatorName], dockerImage)
	master.dockerImageList_lock.Unlock()

	if dockerImage.Prefetched == true {
		// inform all workers to prefetch this docker image in advance
		master.prefetchDockerImages(dockerImage)
	}
}

func (master *Master) prefetchDockerImages(image DockerImage) {
	workers := master.queryWorkers()

	for _, worker := range workers {
		workerID := worker.Entity.ID
		taskMsg := SendMessage{Type: "PREFETCH_IMAGE", RoutingKey: workerID + ".", From: master.id, PayLoad: image}
		master.communicator.Publish(&taskMsg)
	}
}

func (master *Master) queryWorkers() []*ContextObject {
	query := QueryContextRequest{}

	query.Entities = make([]EntityId, 0)

	entity := EntityId{}
	entity.Type = "Worker"
	entity.IsPattern = true
	query.Entities = append(query.Entities, entity)

	client := NGSI10Client{IoTBrokerURL: master.BrokerURL}
	ctxObjects, err := client.QueryContext(&query)
	if err != nil {
		ERROR.Println(err)
	}

	return ctxObjects
}

func (master *Master) onReceiveContextAvailability(notifyCtxAvailReq *NotifyContextAvailabilityRequest) {
	INFO.Println("===========RECEIVE CONTEXT AVAILABILITY=========")

	DEBUG.Print("received raw availability notify: %+v\r\n", notifyCtxAvailReq)

	subID := notifyCtxAvailReq.SubscriptionId

	var action string
	switch notifyCtxAvailReq.ErrorCode.Code {
	case 201:
		action = "CREATE"
	case 301:
		action = "UPDATE"
	case 410:
		action = "DELETE"
	}

	for _, registrationResp := range notifyCtxAvailReq.ContextRegistrationResponseList {
		registration := registrationResp.ContextRegistration
		for _, entity := range registration.EntityIdList {
			// convert context registration to entity registration
			entityRegistration := master.contextRegistration2EntityRegistration(&entity, &registration)
			master.taskMgr.HandleContextAvailabilityUpdate(subID, action, entityRegistration)
		}
	}
}

func (master *Master) contextRegistration2EntityRegistration(entityId *EntityId, ctxRegistration *ContextRegistration) *EntityRegistration {
	entityRegistration := EntityRegistration{}

	ctxObj := master.RetrieveContextEntity(entityId.ID)

	if ctxObj == nil {
		entityRegistration.ID = entityId.ID
		entityRegistration.Type = entityId.Type
	} else {
		entityRegistration.ID = ctxObj.Entity.ID
		entityRegistration.Type = ctxObj.Entity.Type

		entityRegistration.AttributesList = make(map[string]ContextRegistrationAttribute)
		for attrName, attrValue := range ctxObj.Attributes {
			attributeRegistration := ContextRegistrationAttribute{}
			attributeRegistration.Name = attrName
			attributeRegistration.Type = attrValue.Type

			entityRegistration.AttributesList[attrName] = attributeRegistration
		}

		entityRegistration.MetadataList = make(map[string]ContextMetadata)
		for metaname, ctxmeta := range ctxObj.Metadata {
			cm := ContextMetadata{}
			cm.Name = metaname
			cm.Type = ctxmeta.Type
			cm.Value = ctxmeta.Value

			entityRegistration.MetadataList[metaname] = cm
		}
	}

	entityRegistration.ProvidingApplication = ctxRegistration.ProvidingApplication

	DEBUG.Print("REGISTERATION OF ENTITY CONTEXT AVAILABILITY: %+v\r\n", entityRegistration)

	return &entityRegistration
}

func (master *Master) subscribeContextAvailability(availabilitySubscription *SubscribeContextAvailabilityRequest) string {

	availabilitySubscription.Reference = master.myURL + "/notifyContextAvailability"

	client := NGSI9Client{IoTDiscoveryURL: master.cfg.GetDiscoveryURL()}
	subscriptionId, err := client.SubscribeContextAvailability(availabilitySubscription)
	if err != nil {
		ERROR.Println(err)
		return ""
	}

	return subscriptionId
}

//
// to deal with the communication between master and workers via rabbitmq
//
func (master *Master) Process(msg *RecvMessage) error {
	//INFO.Println("type ", msg.Type)

	switch msg.Type {
	case "heart_beat":
		profile := WorkerProfile{}
		err := json.Unmarshal(msg.PayLoad, &profile)
		if err == nil {
			master.onHeartbeat(msg.From, &profile)
		}

	case "task_update":
		update := TaskUpdate{}
		err := json.Unmarshal(msg.PayLoad, &update)
		if err == nil {
			master.onTaskUpdate(msg.From, &update)
		}
	}

	return nil
}

func (master *Master) onHeartbeat(from string, profile *WorkerProfile) {
	master.workerList_lock.Lock()
	master.workers[profile.WID] = profile
	master.workerList_lock.Unlock()
}

func (master *Master) onTaskUpdate(from string, update *TaskUpdate) {
	INFO.Println("==task update=========")
	INFO.Println(update)
}

//
// to carry out deployment actions given by the orchestrators of fog functions and service topologies
//
func (master *Master) DeployTasks(taskInstances []*ScheduledTaskInstance) {
	for _, pScheduledTaskInstance := range taskInstances {
		// convert the operator name into the name of a proper docker image for the assigned worker
		operatorName := (*pScheduledTaskInstance).DockerImage
		assignedWorkerID := (*pScheduledTaskInstance).WorkerID
		(*pScheduledTaskInstance).DockerImage = master.DetermineDockerImage(operatorName, assignedWorkerID)

		taskMsg := SendMessage{Type: "ADD_TASK", RoutingKey: pScheduledTaskInstance.WorkerID + ".", From: master.id, PayLoad: *pScheduledTaskInstance}
		INFO.Println(taskMsg)
		master.communicator.Publish(&taskMsg)
	}
}

func (master *Master) TerminateTasks(instances []*ScheduledTaskInstance) {
	INFO.Println("to terminate all scheduled tasks, ", len(instances))
	for _, instance := range instances {
		taskMsg := SendMessage{Type: "REMOVE_TASK", RoutingKey: instance.WorkerID + ".", From: master.id, PayLoad: *instance}
		INFO.Println(taskMsg)
		master.communicator.Publish(&taskMsg)
	}
}

func (master *Master) DeployTask(taskInstance *ScheduledTaskInstance) {
	// convert the operator name into the name of a proper docker image for the assigned worker
	operatorName := (*taskInstance).DockerImage
	assignedWorkerID := (*taskInstance).WorkerID
	(*taskInstance).DockerImage = master.DetermineDockerImage(operatorName, assignedWorkerID)

	taskMsg := SendMessage{Type: "ADD_TASK", RoutingKey: taskInstance.WorkerID + ".", From: master.id, PayLoad: *taskInstance}
	INFO.Println(taskMsg)
	master.communicator.Publish(&taskMsg)
}

func (master *Master) TerminateTask(taskInstance *ScheduledTaskInstance) {
	taskMsg := SendMessage{Type: "REMOVE_TASK", RoutingKey: taskInstance.WorkerID + ".", From: master.id, PayLoad: *taskInstance}
	INFO.Println(taskMsg)
	master.communicator.Publish(&taskMsg)
}

func (master *Master) AddInputEntity(flowInfo FlowInfo) {
	taskMsg := SendMessage{Type: "ADD_INPUT", RoutingKey: flowInfo.WorkerID + ".", From: master.id, PayLoad: flowInfo}
	INFO.Println(taskMsg)
	master.communicator.Publish(&taskMsg)
}

func (master *Master) RemoveInputEntity(flowInfo FlowInfo) {
	taskMsg := SendMessage{Type: "REMOVE_INPUT", RoutingKey: flowInfo.WorkerID + ".", From: master.id, PayLoad: flowInfo}
	INFO.Println(taskMsg)
	master.communicator.Publish(&taskMsg)
}

//
// the shared functions for function manager and topology manager to call
//
func (master *Master) RetrieveContextEntity(eid string) *ContextObject {
	query := QueryContextRequest{}

	query.Entities = make([]EntityId, 0)

	entity := EntityId{}
	entity.ID = eid
	entity.IsPattern = false
	query.Entities = append(query.Entities, entity)

	client := NGSI10Client{IoTBrokerURL: master.BrokerURL}
	ctxObjects, err := client.QueryContext(&query)
	if err == nil && ctxObjects != nil && len(ctxObjects) > 0 {
		return ctxObjects[0]
	} else {
		if err != nil {
			ERROR.Println("error occured when retrieving a context entity :", err)
		}

		return nil
	}
}

//
// to select the right docker image of an operator for the selected worker
//
func (master *Master) DetermineDockerImage(operatorName string, wID string) string {
	selectedDockerImageName := ""

	wProfile := master.workers[wID]
	master.dockerImageList_lock.RLock()
	for _, image := range master.dockerImageList[operatorName] {
		DEBUG.Println(image.TargetedOSType, image.TargetedHWType)
		DEBUG.Println(wProfile.OSType, wProfile.HWType)

		hwType := "X86"
		osType := "Linux"

		if wProfile.HWType == "arm" {
			hwType = "ARM"
		}

		if wProfile.OSType == "linux" {
			osType = "Linux"
		}

		if image.TargetedOSType == osType && image.TargetedHWType == hwType {
			selectedDockerImageName = image.ImageName + ":" + image.ImageTag
		}
	}

	master.dockerImageList_lock.RUnlock()

	return selectedDockerImageName
}

//
// to select the worker that is closest to the given points
//
func (master *Master) SelectWorker(locations []Point) string {
	if len(locations) == 0 {
		for _, worker := range master.workers {
			return worker.WID
		}

		return ""
	}

	closestWorkerID := ""
	closestTotalDistance := uint64(18446744073709551615)
	for _, worker := range master.workers {
		INFO.Printf("check worker %+v\r\n", worker)

		wp := Point{}
		wp.Latitude = worker.PLocation.Latitude
		wp.Longitude = worker.PLocation.Longitude

		totalDistance := uint64(0)

		for _, location := range locations {
			distance := Distance(wp, location)
			totalDistance += distance
			INFO.Printf("distance = %d between %+v, %+v\r\n", distance, wp, location)
		}

		if totalDistance < closestTotalDistance {
			closestWorkerID = worker.WID
			closestTotalDistance = totalDistance
		}

		INFO.Println("closest worker ", closestWorkerID, " with the closest distance ", closestTotalDistance)
	}

	return closestWorkerID
}

func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

func Distance(p1 Point, p2 Point) uint64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = p1.Latitude * math.Pi / 180
	lo1 = p1.Longitude * math.Pi / 180
	la2 = p2.Latitude * math.Pi / 180
	lo2 = p2.Longitude * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return uint64(2 * r * math.Asin(math.Sqrt(h)))
}

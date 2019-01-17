package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/ant0ine/go-json-rest/rest"
	_ "github.com/lib/pq"
	"github.com/satori/go.uuid"

	. "github.com/smartfog/fogflow/common/config"
	. "github.com/smartfog/fogflow/common/ngsi"
)

type InterSiteSubscription struct {
	DiscoveryURL   string
	SubscriptionID string
}

type FastDiscovery struct {
	//backend entity repository
	repository EntityRepository

	//routing table
	routingTable *Routing

	//list of active brokers within the same site
	BrokerList map[string]*BrokerProfile

	//mapping from subscriptionID to subscription
	subscriptions                map[string]*SubscribeContextAvailabilityRequest
	linkedInterSiteSubscriptions map[string][]InterSiteSubscription
	subscriptions_lock           sync.RWMutex
}

func (fd *FastDiscovery) Init(cfg *DatabaseCfg) {
	fd.subscriptions = make(map[string]*SubscribeContextAvailabilityRequest)
	fd.linkedInterSiteSubscriptions = make(map[string][]InterSiteSubscription)
	fd.BrokerList = make(map[string]*BrokerProfile)

	fd.repository.Init(cfg)
}

func (fd *FastDiscovery) Stop() {
	fd.repository.Close()
}

func (fd *FastDiscovery) RegisterContext(w rest.ResponseWriter, r *rest.Request) {
	registerCtxReq := RegisterContextRequest{}
	err := r.DecodeJsonPayload(&registerCtxReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if registerCtxReq.RegistrationId == "" {
		u1, err := uuid.NewV4()
		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		registrationID := u1.String()
		registerCtxReq.RegistrationId = registrationID
	}

	// update context registration
	go fd.updateRegistration(&registerCtxReq)

	// send out the response
	registerCtxResp := RegisterContextResponse{}
	registerCtxResp.RegistrationId = registerCtxReq.RegistrationId
	registerCtxResp.Duration = registerCtxReq.Duration
	registerCtxResp.ErrorCode.Code = 200
	registerCtxResp.ErrorCode.ReasonPhrase = "OK"
	w.WriteJson(&registerCtxResp)
}

func (fd *FastDiscovery) forwardRegistrationCtxAvailability(discoveryURL string, registrationReq *RegisterContextRequest) {
	requestURL := "http://" + discoveryURL + "/ngsi9/registerContext"
	client := NGSI9Client{IoTDiscoveryURL: requestURL}
	_, err := client.RegisterContext(registrationReq)
	if err != nil {
		ERROR.Println(err)
	}
}

func (fd *FastDiscovery) notifySubscribers(registration *ContextRegistration, updateAction string) {
	fd.subscriptions_lock.RLock()
	defer fd.subscriptions_lock.RUnlock()

	providerURL := registration.ProvidingApplication
	for _, subscription := range fd.subscriptions {
		// find out the updated entities matched with this subscription
		entities := fd.matchingWithSubscription(registration, subscription)
		if len(entities) == 0 {
			continue
		}

		subscriberURL := subscription.Reference
		subID := subscription.SubscriptionId

		entityMap := make(map[string][]EntityId)
		entityMap[providerURL] = entities

		// send out AvailabilityNotify to subscribers
		go fd.sendNotify(subID, subscriberURL, entityMap, updateAction)
	}
}

func (fd *FastDiscovery) matchingWithSubscription(registration *ContextRegistration, subscription *SubscribeContextAvailabilityRequest) []EntityId {
	matchedEntities := make([]EntityId, 0)

	for _, entity := range registration.EntityIdList {
		// check entityId part
		atLeastOneMatched := false
		for _, tmp := range subscription.Entities {
			matched := matchEntityId(entity, tmp)
			if matched == true {
				atLeastOneMatched = true
				break
			}
		}
		if atLeastOneMatched == false {
			continue
		}

		// check attribute set
		matched := matchAttributes(registration.ContextRegistrationAttributes, subscription.Attributes)
		if matched == false {
			continue
		}

		// check metadata set
		matched = matchMetadatas(registration.Metadata, subscription.Restriction)
		if matched == false {
			continue
		}

		// if matched, add it into the list
		if matched == true {
			matchedEntities = append(matchedEntities, entity)
		}
	}

	return matchedEntities
}

func (fd *FastDiscovery) updateRegistration(registReq *RegisterContextRequest) {
	for _, registration := range registReq.ContextRegistrations {
		for _, entity := range registration.EntityIdList {
			// update the registration, both in the memory cache and in the database
			updatedRegistration := fd.repository.updateEntity(entity, &registration)

			// inform the associated subscribers after updating the repository
			go fd.notifySubscribers(updatedRegistration, "UPDATE")
		}
	}
}

func (fd *FastDiscovery) deleteRegistration(eid string) {
	registration := fd.repository.retrieveRegistration(eid)
	if registration != nil {
		fd.notifySubscribers(registration, "DELETE")
	}

	fd.repository.deleteEntity(eid)
}

func (fd *FastDiscovery) SiteDiscoverContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	discoverCtxReq := DiscoverContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&discoverCtxReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// query database to get the result
	result := fd.handleQueryCtxAvailability(&discoverCtxReq)

	// send out the response
	discoverCtxResp := DiscoverContextAvailabilityResponse{}
	if result == nil {
		discoverCtxResp.ErrorCode.Code = 500
		discoverCtxResp.ErrorCode.ReasonPhrase = "database is too overloaded"
	} else {
		discoverCtxResp.ContextRegistrationResponses = result
		discoverCtxResp.ErrorCode.Code = 200
		discoverCtxResp.ErrorCode.ReasonPhrase = "OK"
	}
	w.WriteJson(&discoverCtxResp)
}

func (fd *FastDiscovery) InterSiteDiscoverContextAvailability(siteURL string, discoverCtxAvailabilityReq *DiscoverContextAvailabilityRequest) ([]ContextRegistrationResponse, error) {
	requestURL := "http://" + siteURL + "/ngsi9/interSiteContextAvailabilityQuery"

	INFO.Println(requestURL)

	body, err := json.Marshal(discoverCtxAvailabilityReq)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	text, _ := ioutil.ReadAll(resp.Body)

	discoverCtxAvailResp := DiscoverContextAvailabilityResponse{}
	err = json.Unmarshal(text, &discoverCtxAvailResp)
	if err != nil {
		return nil, err
	}

	return discoverCtxAvailResp.ContextRegistrationResponses, nil
}

func (fd *FastDiscovery) DiscoverContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	discoverCtxReq := DiscoverContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&discoverCtxReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// query all relevant discovery instances to get the matched result
	result := make([]ContextRegistrationResponse, 0)

	// look up the routing table to see which sites should be contacted for this query
	geoscope := discoverCtxReq.Restriction.GetScope()
	siteList := fd.routingTable.QuerySitesByScope(geoscope)

	INFO.Println("===========MULTI-DISCOVERY===================")
	INFO.Println(siteList)

	for _, site := range siteList {
		if site.ExternalAddress == fd.routingTable.MySiteInfo().ExternalAddress {
			registrationList := fd.handleQueryCtxAvailability(&discoverCtxReq)
			for _, registration := range registrationList {
				result = append(result, registration)
			}
		} else {
			if ctxRegisterationResponseList, ok := fd.InterSiteDiscoverContextAvailability(site.ExternalAddress, &discoverCtxReq); ok == nil {
				for _, registrationResponse := range ctxRegisterationResponseList {
					result = append(result, registrationResponse)
				}
			}
		}
	}

	// send out the response
	discoverCtxResp := DiscoverContextAvailabilityResponse{}
	if result == nil {
		discoverCtxResp.ErrorCode.Code = 500
		discoverCtxResp.ErrorCode.ReasonPhrase = "database is too overloaded"
	} else {
		discoverCtxResp.ContextRegistrationResponses = result
		discoverCtxResp.ErrorCode.Code = 200
		discoverCtxResp.ErrorCode.ReasonPhrase = "OK"
	}
	w.WriteJson(&discoverCtxResp)
}

func (fd *FastDiscovery) handleQueryCtxAvailability(req *DiscoverContextAvailabilityRequest) []ContextRegistrationResponse {
	entityMap := fd.repository.queryEntities(req.Entities, req.Attributes, req.Restriction)

	// prepare the response
	registrationList := make([]ContextRegistrationResponse, 0)

	for url, entity := range entityMap {
		resp := ContextRegistrationResponse{}
		resp.ContextRegistration.ProvidingApplication = url
		resp.ContextRegistration.EntityIdList = entity

		resp.ErrorCode.Code = 200
		resp.ErrorCode.ReasonPhrase = "OK"

		registrationList = append(registrationList, resp)
	}

	return registrationList
}

func (fd *FastDiscovery) SiteSubscribeContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	subscribeCtxAvailabilityReq := SubscribeContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&subscribeCtxAvailabilityReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// generate a new subscription id
	u1, err := uuid.NewV4()
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	subID := u1.String()

	subscribeCtxAvailabilityReq.SubscriptionId = subID

	// add the new subscription
	fd.subscriptions_lock.Lock()
	fd.subscriptions[subID] = &subscribeCtxAvailabilityReq
	fd.subscriptions_lock.Unlock()

	// send out the response
	subscribeCtxAvailabilityResp := SubscribeContextAvailabilityResponse{}
	subscribeCtxAvailabilityResp.SubscriptionId = subID
	subscribeCtxAvailabilityResp.Duration = subscribeCtxAvailabilityReq.Duration
	subscribeCtxAvailabilityResp.ErrorCode.Code = 200
	subscribeCtxAvailabilityResp.ErrorCode.ReasonPhrase = "OK"

	w.WriteJson(&subscribeCtxAvailabilityResp)

	// trigger the process to send out the matched context availability infomation to the subscriber
	go fd.handleSubscribeCtxAvailability(&subscribeCtxAvailabilityReq)
}

/*
func (fd *FastDiscovery) SubscribeContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	subscribeCtxAvailabilityReq := SubscribeContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&subscribeCtxAvailabilityReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// generate a new subscription id
	u1, err := uuid.NewV4()
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	subID := u1.String()

	subscribeCtxAvailabilityReq.SubscriptionId = subID

	// add the new subscription
	fd.subscriptions_lock.Lock()
	fd.subscriptions[subID] = &subscribeCtxAvailabilityReq
	fd.subscriptions_lock.Unlock()

	// send out the response
	subscribeCtxAvailabilityResp := SubscribeContextAvailabilityResponse{}
	subscribeCtxAvailabilityResp.SubscriptionId = subID
	subscribeCtxAvailabilityResp.Duration = subscribeCtxAvailabilityReq.Duration
	subscribeCtxAvailabilityResp.ErrorCode.Code = 200
	subscribeCtxAvailabilityResp.ErrorCode.ReasonPhrase = "OK"

	w.WriteJson(&subscribeCtxAvailabilityResp)

	// trigger the process to send out the matched context availability infomation to the subscriber
	go fd.handleSubscribeCtxAvailability(&subscribeCtxAvailabilityReq)
}
*/

func (fd *FastDiscovery) SubscribeContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	subscribeCtxAvailabilityReq := SubscribeContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&subscribeCtxAvailabilityReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// generate a new subscription id
	u1, err := uuid.NewV4()
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	subID := u1.String()

	subscribeCtxAvailabilityReq.SubscriptionId = subID

	// add the new subscription
	fd.subscriptions_lock.Lock()
	fd.subscriptions[subID] = &subscribeCtxAvailabilityReq
	fd.subscriptions_lock.Unlock()

	// send out the response
	subscribeCtxAvailabilityResp := SubscribeContextAvailabilityResponse{}
	subscribeCtxAvailabilityResp.SubscriptionId = subID
	subscribeCtxAvailabilityResp.Duration = subscribeCtxAvailabilityReq.Duration
	subscribeCtxAvailabilityResp.ErrorCode.Code = 200
	subscribeCtxAvailabilityResp.ErrorCode.ReasonPhrase = "OK"

	w.WriteJson(&subscribeCtxAvailabilityResp)

	// look up the routing table to see which sites should be contacted for this subscription
	geoscope := subscribeCtxAvailabilityReq.Restriction.GetScope()
	siteList := fd.routingTable.QuerySitesByScope(geoscope)

	INFO.Println("===========MULTI-DISCOVERY=======SUBSCRIBE============")
	INFO.Println(siteList)

	for _, site := range siteList {
		if site.ExternalAddress == fd.routingTable.MySiteInfo().ExternalAddress {
			// trigger the process to send out the matched context availability infomation to the subscriber
			go fd.handleSubscribeCtxAvailability(&subscribeCtxAvailabilityReq)
		} else {
			// forward this subscription to the other discovery servers
			go fd.forwardSubscribeCtxAvailability(site.ExternalAddress, subID, &subscribeCtxAvailabilityReq)
		}
	}

}

// forward the received NGSI9 subscription to another discovery server
func (fd *FastDiscovery) forwardSubscribeCtxAvailability(discoveryURL string, originalSID string, subReq *SubscribeContextAvailabilityRequest) {
	requestURL := "http://" + discoveryURL + "/ngsi9/interSiteContextAvailabilityUnsubscribe"
	client := NGSI9Client{IoTDiscoveryURL: requestURL}
	sid, err := client.SubscribeContextAvailability(subReq)
	if sid != "" && err == nil {
		fd.subscriptions_lock.Lock()

		if _, exist := fd.linkedInterSiteSubscriptions[originalSID]; exist == false {
			fd.linkedInterSiteSubscriptions[originalSID] = make([]InterSiteSubscription, 1)
		}

		interSiteSubscription := InterSiteSubscription{}
		interSiteSubscription.DiscoveryURL = discoveryURL
		interSiteSubscription.SubscriptionID = sid

		fd.linkedInterSiteSubscriptions[originalSID] = append(fd.linkedInterSiteSubscriptions[originalSID], interSiteSubscription)

		fd.subscriptions_lock.Unlock()
	}
}

// handle NGSI9 subscription based on the local database
func (fd *FastDiscovery) handleSubscribeCtxAvailability(subReq *SubscribeContextAvailabilityRequest) {
	entityMap := fd.repository.queryEntities(subReq.Entities, subReq.Attributes, subReq.Restriction)

	if len(entityMap) > 0 {
		fd.sendNotify(subReq.SubscriptionId, subReq.Reference, entityMap, "CREATE")
	}
}

func (fd *FastDiscovery) sendNotify(subID string, subscriberURL string, entityMap map[string][]EntityId, action string) {
	notifyReq := NotifyContextAvailabilityRequest{}
	notifyReq.SubscriptionId = subID

	// carry the actions via the code number
	switch action {
	case "CREATE":
		notifyReq.ErrorCode.Code = 201
	case "UPDATE":
		notifyReq.ErrorCode.Code = 301
	case "DELETE":
		notifyReq.ErrorCode.Code = 410
	}

	notifyReq.ErrorCode.ReasonPhrase = "OK"

	// prepare the response
	registrationList := make([]ContextRegistrationResponse, 0)

	for url, entity := range entityMap {
		resp := ContextRegistrationResponse{}
		resp.ContextRegistration.ProvidingApplication = url
		resp.ContextRegistration.EntityIdList = entity

		resp.ErrorCode.Code = 200

		resp.ErrorCode.ReasonPhrase = "OK"

		registrationList = append(registrationList, resp)
	}

	notifyReq.ContextRegistrationResponseList = registrationList

	body, err := json.Marshal(notifyReq)
	if err != nil {
		ERROR.Println(err)
		return
	}

	req, err := http.NewRequest("POST", subscriberURL, bytes.NewBuffer(body))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err2 := client.Do(req)
	if err2 != nil {
		ERROR.Println(err2)
		return
	}

	defer resp.Body.Close()

	text, _ := ioutil.ReadAll(resp.Body)

	notifyCtxAvailResp := NotifyContextAvailabilityResponse{}
	err = json.Unmarshal(text, &notifyCtxAvailResp)
	if err != nil {
		ERROR.Println(err)
		return
	}

	if notifyCtxAvailResp.ResponseCode.Code != 200 {
		ERROR.Println(notifyCtxAvailResp.ResponseCode.ReasonPhrase)
	}
}

func (fd *FastDiscovery) SiteUnsubscribeContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	unsubscribeCtxAvailabilityReq := UnsubscribeContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&unsubscribeCtxAvailabilityReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	subID := unsubscribeCtxAvailabilityReq.SubscriptionId

	// remove the subscription
	fd.subscriptions_lock.Lock()
	delete(fd.subscriptions, subID)
	fd.subscriptions_lock.Unlock()

	// send out the response
	unsubscribeCtxAvailabilityResp := UnsubscribeContextAvailabilityResponse{}
	unsubscribeCtxAvailabilityResp.SubscriptionId = unsubscribeCtxAvailabilityReq.SubscriptionId
	unsubscribeCtxAvailabilityResp.StatusCode.Code = 200
	unsubscribeCtxAvailabilityResp.StatusCode.Details = "OK"

	w.WriteJson(&unsubscribeCtxAvailabilityResp)
}

func (fd *FastDiscovery) UnsubscribeContextAvailability(w rest.ResponseWriter, r *rest.Request) {
	unsubscribeCtxAvailabilityReq := UnsubscribeContextAvailabilityRequest{}
	err := r.DecodeJsonPayload(&unsubscribeCtxAvailabilityReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	subID := unsubscribeCtxAvailabilityReq.SubscriptionId

	var interSiteSubList []InterSiteSubscription

	// remove the subscription
	fd.subscriptions_lock.Lock()
	delete(fd.subscriptions, subID)
	interSiteSubList = fd.linkedInterSiteSubscriptions[subID]
	delete(fd.linkedInterSiteSubscriptions, subID)
	fd.subscriptions_lock.Unlock()

	// send out the response
	unsubscribeCtxAvailabilityResp := UnsubscribeContextAvailabilityResponse{}
	unsubscribeCtxAvailabilityResp.SubscriptionId = unsubscribeCtxAvailabilityReq.SubscriptionId
	unsubscribeCtxAvailabilityResp.StatusCode.Code = 200
	unsubscribeCtxAvailabilityResp.StatusCode.Details = "OK"

	w.WriteJson(&unsubscribeCtxAvailabilityResp)

	// issue unsubscribe to the other discovery server if there are existing inter-site subscription that have been issued before
	for _, linkedSub := range interSiteSubList {
		go fd.sendInterSiteUnsubscribeContextAvailability(linkedSub.DiscoveryURL, linkedSub.SubscriptionID)
	}
}

func (fd *FastDiscovery) sendInterSiteUnsubscribeContextAvailability(discoveryURL string, sid string) error {
	requestURL := "http://" + discoveryURL + "/ngsi9/interSiteContextAvailabilityUnsubscribe"
	client := NGSI9Client{IoTDiscoveryURL: requestURL}
	err := client.UnsubscribeContextAvailability(sid)
	return err
}

func (fd *FastDiscovery) getRegisteredEntity(w rest.ResponseWriter, r *rest.Request) {
	var eid = r.PathParam("eid")

	registration := fd.repository.retrieveRegistration(eid)
	w.WriteJson(registration)
}

func (fd *FastDiscovery) deleteRegisteredEntity(w rest.ResponseWriter, r *rest.Request) {
	var eid = r.PathParam("eid")
	w.WriteHeader(200)

	go fd.deleteRegistration(eid)
}

func (fd *FastDiscovery) getSubscription(w rest.ResponseWriter, r *rest.Request) {
	var sid = r.PathParam("sid")

	fd.subscriptions_lock.RLocker()
	defer fd.subscriptions_lock.RUnlock()

	subscription := fd.subscriptions[sid]
	w.WriteJson(subscription)
}

func (fd *FastDiscovery) getSubscriptions(w rest.ResponseWriter, r *rest.Request) {
	fd.subscriptions_lock.RLock()
	defer fd.subscriptions_lock.RUnlock()

	w.WriteJson(fd.subscriptions)
}

func (fd *FastDiscovery) onBroadcast(w rest.ResponseWriter, r *rest.Request) {
	msg := RecvBroadcastMsg{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fd.routingTable.ReceiveBroadcast(&msg)

	w.WriteHeader(200)
}

func (fd *FastDiscovery) getAllSites(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(fd.routingTable.Serialization())
	w.WriteHeader(200)
}

func (fd *FastDiscovery) onQuerySiteByScope(w rest.ResponseWriter, r *rest.Request) {
	geoscope := OperationScope{}
	err := r.DecodeJsonPayload(&geoscope)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	siteList := fd.routingTable.QuerySitesByScope(geoscope)

	w.WriteJson(siteList)
}

func (fd *FastDiscovery) getStatus(w rest.ResponseWriter, r *rest.Request) {
	w.WriteHeader(200)
}

func (fd *FastDiscovery) onForwardContextUpdate(w rest.ResponseWriter, r *rest.Request) {
	updateCtxReq := UpdateContextRequest{}

	err := r.DecodeJsonPayload(&updateCtxReq)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// send out the response
	updateCtxResp := UpdateContextResponse{}
	updateCtxResp.ErrorCode.Code = 200
	updateCtxResp.ErrorCode.ReasonPhrase = "OK"
	w.WriteJson(&updateCtxResp)

	INFO.Println("============FORWARD============")

	// perform the update action accordingly
	switch updateCtxReq.UpdateAction {
	case "UPDATE":
		for _, ctxElem := range updateCtxReq.ContextElements {
			//if ctxElem.Entity.ID == "" {
			selectedBroker := fd.selectBroker()
			if selectedBroker != nil {
				providerURL := selectedBroker.MyURL
				client := NGSI10Client{IoTBrokerURL: providerURL}
				client.InternalUpdateContext(&ctxElem)
			}
			/*} else {
				eid := ctxElem.Entity.ID
				registration := fd.repository.retrieveRegistration(eid)
				if registration != nil {
					providerURL := registration.ProvidingApplication
					client := NGSI10Client{IoTBrokerURL: providerURL}
					client.InternalUpdateContext(&ctxElem)
				}
			} */
		}

	case "DELETE":
		for _, ctxElem := range updateCtxReq.ContextElements {
			eid := ctxElem.Entity.ID
			registration := fd.repository.retrieveRegistration(eid)
			if registration != nil {
				providerURL := registration.ProvidingApplication
				client := NGSI10Client{IoTBrokerURL: providerURL}
				client.InternalDeleteContext(&ctxElem.Entity)
			}
		}
	}
}

func (fd *FastDiscovery) onBrokerHeartbeat(w rest.ResponseWriter, r *rest.Request) {
	brokerProfile := BrokerProfile{}

	err := r.DecodeJsonPayload(&brokerProfile)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// send out the response
	updateCtxResp := UpdateContextResponse{}
	updateCtxResp.ErrorCode.Code = 200
	updateCtxResp.ErrorCode.ReasonPhrase = "OK"
	w.WriteJson(&updateCtxResp)

	if broker, exist := fd.BrokerList[brokerProfile.BID]; exist {
		broker.MyURL = brokerProfile.MyURL
	} else {
		fd.BrokerList[brokerProfile.BID] = &brokerProfile
	}
}

func (fd *FastDiscovery) selectBroker() *BrokerProfile {
	for _, broker := range fd.BrokerList {
		return broker
	}

	return nil
}

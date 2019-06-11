package main

import (
	"fmt"
	"github.com/smartfog/fogflow/common/ngsi"
	"os"
	"strconv"
	"time"
)

type SubscribtionLogger struct {
	agent *ngsi.NGSIAgent
	main *Main
}

func (sublog * SubscribtionLogger) onReceiveContextNotify(notifyCtxReq *ngsi.NotifyContextRequest){
	//fmt.Println(time.Now().Unix())
	for  _,b := range notifyCtxReq.ContextResponses[0].ContextElement.Attributes {
		if b.Name == "imageId" {
			sublog.main.receiveFileHanlder <- []string{fmt.Sprintf("%v",b.Value),strconv.FormatInt(time.Now().UnixNano(),10)}
			//fmt.Fprintf(os.Stdout,"\nNotified! \n%v , %v", b.Value , time.Now().Unix())
			//fmt.Fprintf(os.Stdout,"\nE: %v, %v" , b.Value, time.Now().Unix())

		}

	}

}

func (sublog * SubscribtionLogger) onReceiveContextAvailability(notifyCtxAvailReq *ngsi.NotifyContextAvailabilityRequest){
	fmt.Fprintf(os.Stdout,"\nnotifycontextavailablity has to be implemented")
}


func (sublog * SubscribtionLogger)InitAgent(m *Main) {
	fmt.Fprint(os.Stdout,"\nstarting ngsi agent")
	sublog.main=m
	sublog.agent = &ngsi.NGSIAgent{Port: 6666}
	sublog.agent.SetContextNotifyHandler(sublog.onReceiveContextNotify)
	sublog.agent.SetContextAvailabilityNotifyHandler(sublog.onReceiveContextAvailability)
	sublog.agent.Start()
	fmt.Fprint(os.Stdout,"\nsublogger started")


}

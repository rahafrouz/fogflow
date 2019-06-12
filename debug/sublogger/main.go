package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)


type Main struct {
	receiveFileHanlder chan []string
	sendFileHanlder chan []string
	brokerurl string
	edges  [10]string
	myaddress string
	DEVICE_COUNT int
	MAX_DEVICE_COUNT int

	MAX_DATA_COUNT int
	DATA_COUNT int
	writers *csv.Writer
	writerr *csv.Writer
}
func main(){
	program:=Main{}
	program.main()
}

func (m * Main)main() {
	m.brokerurl = "http://master:8080"
	m.edges = [10]string{"http://edge0:8080","http://edge1:8080","http://edge2:8080","http://edge3:8080","http://edge4:8080","http://edge5:8080","http://edge6:8080","http://edge7:8080","http://edge8:8080","http://edge9:8080"}
	m.myaddress = "http://13.48.6.180:6666"
	m.DEVICE_COUNT=1
	m.MAX_DEVICE_COUNT=12
	m.MAX_DATA_COUNT=100
	m.DATA_COUNT =2



	fmt.Fprint(os.Stdout,"\nBANG! sublogger started")
	m.sendFileHanlder = make(chan []string)
	m.receiveFileHanlder = make(chan []string)
	m.initWriteProcedure()
	fmt.Fprint(os.Stdout,"\nIs this working?")
	sublogger := SubscribtionLogger{}
	sublogger.InitAgent(m)


	m.startExperiment()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c
	fmt.Fprintf(os.Stdout,"\nsublogger finished")


}


func (m * Main)registerFaceCounter(){
	url := m.brokerurl+"/ngsi10/updateContext"

	payload := strings.NewReader("{\n    \"contextElements\": [\n        {\n            \"entityId\": {\n                \"id\": \"rahafrouz/facecounter:latest\",\n                \"type\": \"DockerImage\",\n                \"isPattern\": false\n            },\n            \"attributes\": [\n                {\n                    \"name\": \"image\",\n                    \"type\": \"string\",\n                    \"contextValue\": \"rahafrouz/facecounter\"\n                },\n                {\n                    \"name\": \"tag\",\n                    \"type\": \"string\",\n                    \"contextValue\": \"latest\"\n                },\n                {\n                    \"name\": \"hwType\",\n                    \"type\": \"string\",\n                    \"contextValue\": \"X86\"\n                },\n                {\n                    \"name\": \"osType\",\n                    \"type\": \"string\",\n                    \"contextValue\": \"Linux\"\n                },\n                {\n                    \"name\": \"operator\",\n                    \"type\": \"string\",\n                    \"contextValue\": \"experimentfacecounter\"\n                },\n                {\n                    \"name\": \"prefetched\",\n                    \"type\": \"boolean\",\n                    \"contextValue\": true\n                }\n            ],\n            \"domainMetadata\": [\n                {\n                    \"name\": \"operator\",\n                    \"type\": \"string\",\n                    \"value\": \"experimentfacecounter\"\n                }\n            ]\n        }\n    ],\n    \"updateAction\": \"UPDATE\"\n}")

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
	}
	defer res.Body.Close()
	//body, _ := ioutil.ReadAll(res.Body)


	//fmt.Println(res)
	//fmt.Println(string(body))
}




func (m * Main)createFogFunction() {
	url := m.brokerurl + "/ngsi10/updateContext"

	payload := strings.NewReader("{\n    \"contextElements\": [\n        {\n            \"entityId\": {\n                \"id\": \"FogFunction.experiment.facecounter\",\n                \"type\": \"FogFunction\",\n                \"isPattern\": false\n            },\n            \"attributes\": [\n                {\n                    \"name\": \"status\",\n                    \"type\": \"string\",\n                    \"contextValue\": \"enabled\"\n                },\n                {\n                    \"name\": \"designboard\",\n                    \"type\": \"object\",\n                    \"contextValue\": {\n                        \"edges\": [\n                            {\n                                \"id\": 1,\n                                \"block1\": 3,\n                                \"connector1\": [\n                                    \"condition\",\n                                    \"output\"\n                                ],\n                                \"block2\": 1,\n                                \"connector2\": [\n                                    \"conditions\",\n                                    \"input\"\n                                ]\n                            },\n                            {\n                                \"id\": 2,\n                                \"block1\": 1,\n                                \"connector1\": [\n                                    \"selector\",\n                                    \"output\"\n                                ],\n                                \"block2\": 2,\n                                \"connector2\": [\n                                    \"selectors\",\n                                    \"input\"\n                                ]\n                            },\n                            {\n                                \"id\": 3,\n                                \"block1\": 2,\n                                \"connector1\": [\n                                    \"annotators\",\n                                    \"output\"\n                                ],\n                                \"block2\": 4,\n                                \"connector2\": [\n                                    \"annotator\",\n                                    \"input\"\n                                ]\n                            }\n                        ],\n                        \"blocks\": [\n                            {\n                                \"id\": 1,\n                                \"x\": -76.5,\n                                \"y\": -163,\n                                \"type\": \"InputTrigger\",\n                                \"module\": null,\n                                \"values\": {\n                                    \"selectedattributes\": [\n                                        \"carid\"\n                                    ],\n                                    \"groupby\": [\n                                        \"car.plate.number\"\n                                    ]\n                                }\n                            },\n                            {\n                                \"id\": 2,\n                                \"x\": -74.5,\n                                \"y\": -54,\n                                \"type\": \"FogFunction\",\n                                \"module\": null,\n                                \"values\": {\n                                    \"name\": \"experiment.facecounter\",\n                                    \"user\": \"fogflow\"\n                                }\n                            },\n                            {\n                                \"id\": 3,\n                                \"x\": -336.5,\n                                \"y\": -76,\n                                \"type\": \"SelectCondition\",\n                                \"module\": null,\n                                \"values\": {\n                                    \"type\": \"EntityType\",\n                                    \"value\": \"Car.Data\"\n                                }\n                            },\n                            {\n                                \"id\": 4,\n                                \"x\": 156.5,\n                                \"y\": 35,\n                                \"type\": \"OutputAnnotator\",\n                                \"module\": null,\n                                \"values\": {\n                                    \"entitytype\": \"experiment.car.counted.faces\",\n                                    \"herited\": false\n                                }\n                            }\n                        ]\n                    }\n                },\n                {\n                    \"name\": \"fogfunction\",\n                    \"type\": \"object\",\n                    \"contextValue\": {\n                        \"type\": \"docker\",\n                        \"code\": \"\",\n                        \"dockerImage\": \"experimentfacecounter\",\n                        \"name\": \"experiment.facecounter\",\n                        \"user\": \"fogflow\",\n                        \"inputTriggers\": [\n                            {\n                                \"name\": \"selector1\",\n                                \"selectedAttributeList\": [\n                                    \"carid\"\n                                ],\n                                \"groupedAttributeList\": [\n                                    \"car.plate.number\"\n                                ],\n                                \"conditionList\": [\n                                    {\n                                        \"type\": \"EntityType\",\n                                        \"value\": \"Car.Data\"\n                                    }\n                                ]\n                            }\n                        ],\n                        \"outputAnnotators\": [\n                            {\n                                \"entityType\": \"experiment.car.counted.faces\",\n                                \"groupInherited\": false\n                            }\n                        ]\n                    }\n                }\n            ],\n            \"domainMetadata\": []\n        }\n    ],\n    \"updateAction\": \"UPDATE\"\n}")

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	//body, _ := ioutil.ReadAll(res.Body)

	//fmt.Println(res)
	//fmt.Println(string(body))
}


func (m * Main)registerDevice(i int) {
	url := m.brokerurl+"/ngsi10/updateContext"

	query:= fmt.Sprintf("{\n\t\"contextElements\":[\n           {\n                \"entityId\": {\n                    \"id\": \"car.%d\",\n                    \"type\": \"Car.Data\",\n                    \"isPattern\": false\n                },\"attributes\": [\n\t\t          {\n\t\t            \"name\": \"carid\",\n\t\t            \"type\": \"string\",\n\t\t            \"contextValue\": \"car.%d\"\n\t\t          }\n\t\t        ], \"domainMetadata\": [\n                    {\n                        \"name\": \"car.plate.number\",\n                        \"type\": \"string\",\n                        \"value\": \"%d\"\n                    }\n                ]\n\n            }\n\t\t\n\t\t],\n\t\t\n\t\"updateAction\": \"UPDATE\"\n}",i,i,i)
	payload := strings.NewReader(query)

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	//body, _ := ioutil.ReadAll(res.Body)
	//
	//fmt.Println(res)
	//fmt.Println(string(body))
}

func (m * Main)sendDataToEdgeBroker(edge string, carid int, imageid int ){

	fmt.Println("Sending data (Image) to car node %v, image: %v", carid,imageid)
	//fmt.Println("%v",time.Now().Unix())

	//fmt.Fprintf(os.Stdout,"\nS: image.%v , %v " , imageid, time.Now().UnixNano())
	data := []string{strconv.Itoa(imageid),strconv.FormatInt(time.Now().UnixNano(),10)}
	fmt.Fprintf(os.Stdout,"\nS: %v" , data)

	//writePointToFile(data,"S")
	m.sendFileHanlder <-data
	//m.writeSendToFile(data)
	url := edge+"/ngsi10/updateContext"

	query:= fmt.Sprintf("\n    {\n      \"contextElements\": [\n      \t{\n        \"entityId\": {\n          \"id\": \"car.%d\",\n          \"type\": \"Car.Data\",\n          \"isPattern\": false\n        },\n        \"attributes\": [\n          {\n            \"name\": \"url\",\n            \"type\": \"string\",\n            \"contextValue\": \"https://static1.squarespace.com/static/56886aafc21b8690d5b6e52d/598dfb3f579fb309340e5dd7/598dfc917131a54f72669879/1502477460050/Open+House+Group.jpg\"\n          },\n          {\n            \"name\": \"imageId\",\n            \"type\": \"string\",\n            \"contextValue\": \"image.%d\"\n          }\n        ]\n      }\n      ],\n      \t\"updateAction\": \"UPDATE\"\n      \n    }\n",carid,imageid)
	payload := strings.NewReader(query)

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err !=nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
}
func (m * Main)subscribeToResults(edgeAddress string){

	url := edgeAddress + "/ngsi10/subscribeContext"
	query:= fmt.Sprintf("{\n    \"entities\": [\n        {\n            \"id\": \".*\",\n            \"type\": \"experiment.car.counted.faces\",\n            \"isPattern\": true\n        }\n    ],\n    \"attributes\": [\n        \n    ],\n    \"reference\": \"%v\",\n    \"restriction\": {\n        \"attributeExpression\": \"\"\n    }\n}\n", m.myaddress)
	payload := strings.NewReader(query)

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err!=nil{
		fmt.Println("Error subscribing to ",edgeAddress,err)
	}
	defer res.Body.Close()
	//body, _ := ioutil.ReadAll(res.Body)
	//
	//fmt.Println(res)
	//fmt.Println(string(body))
}
func (m * Main)startExperiment() {



	fmt.Println("Starting the experiment. Going to wait for 10 seconds to boot up the system")
	time.Sleep(20* time.Second)

	fmt.Println("Registering Docker image to the system (Facecounter)... ")
	m.registerFaceCounter()
	time.Sleep(6* time.Second)


	fmt.Println("Subscribing to output to all edge nodes")
	for _, edge := range m.edges {
		m.subscribeToResults(edge)
	}



	fmt.Println("Creating the fog function for experiment...")
	m.createFogFunction()
	time.Sleep(6* time.Second)


	go m.registerDevices()
	go m.sendDataToEdgeNodes()

	////imageid:=0
	//
	//for i:=1; i<DEVICE_COUNT; i++ {
	//	fmt.Println("Registring Device %d", i)
	//	registerDevice(i)
	//	time.Sleep(3*time.Second)
	//	//get the address of proper broker
	//	//send the data to the selected, or all edge nodes
	//	rand.Seed(time.Now().Unix())
	//	DATA_COUNT =rand.Intn(MAX_DATA_COUNT)
	//	for j:=1;j<DATA_COUNT;j++ {
	//		time.Sleep(1*time.Second)
	//		for _, edge := range edges {
	//			sendDataToEdgeBroker(edge,i, i*1000 + j)
	//			//imageid+=1
	//		}
	//	}
	//
	//
	//	//find out the assigned broker address
	//
	//}



}



func (m * Main)registerDevices() {
	for i:=1; i<m.MAX_DEVICE_COUNT; i++ {
		fmt.Println("Registring Device %d", i)
		m.registerDevice(i)
		time.Sleep(1*time.Second)
		m.DEVICE_COUNT += 1
	}
}


func (m * Main)sendDataToEdgeNodes() {

	for i:=1; i<m.MAX_DATA_COUNT; i++ {

		rand.Seed(time.Now().Unix())
		time.Sleep(100*time.Millisecond)

		deviceid :=rand.Intn(m.DEVICE_COUNT)
		fmt.Printf("DEBUG: DeviceID: %v",i)
		for _, edge := range m.edges {
			m.sendDataToEdgeBroker(edge,deviceid, deviceid*1000 + i)
		}
	}
}

func (m * Main)initWriteProcedure(){
	files, err := os.Create("/tmp/sublogger/send.csv")
	m.checkError("Cannot create file", err)
	//defer file.Close()
	m.writers = csv.NewWriter(files)
	go m.writeSendToFile()

	filer, err := os.Create("/tmp/sublogger/receive.csv")
	m.checkError("Cannot create file", err)
	//defer file.Close()
	m.writerr = csv.NewWriter(filer)
	go m.writeReceiveToFile()

}
func (m * Main)writeSendToFile(){
	for {
		fmt.Println("SENDER started")
		data := <-m.sendFileHanlder
		err := m.writers.Write(data)
		fmt.Println("SENDER: Going to write %v",data)
		m.checkError("Cannot write to file", err)
		m.writers.Flush()
	}
}
func (m * Main)writeReceiveToFile(){
	for {
		fmt.Println("WRITER started")
		data := <- m.receiveFileHanlder
		err := m.writerr.Write(data)
		fmt.Println("WRITER: Going to write %v",data)
		m.checkError("Cannot write to file", err)
		m.writerr.Flush()
	}
}

func (m * Main)checkError(message string, err error) {
	if err != nil {
		fmt.Println(message)
		//log.Fatal(message, err)
	}
}
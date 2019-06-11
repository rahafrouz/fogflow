package main

import (
	"encoding/json"
	"fmt"
	"gonum.org/v1/gonum/mat"
	"io/ioutil"
	"log"
	"net/http"
	url2 "net/url"
	"strconv"
	"math"
)

func main(){
	//var DIMS int
	// ROWS=3
	// COLUMNS=3
	//DIMS=3

	preferenceMatrix := mat.NewDense(4, 4, []float64{
	    1,0.5, 10, 5,
	    2, 1 , 2 , 7,
	    1,0.5, 1 ,0.3,
	    2,  4, 5 , 3,
	})
	fmt.Printf("Matrix of Preference of Metrics is =\n %v\n\n", mat.Formatted(preferenceMatrix, mat.Prefix("    ")))

	var eig mat.Eigen

	ok := eig.Factorize(preferenceMatrix, mat.EigenRight)
	if !ok {
	    log.Fatal("Eigendecomposition failed")
	}

	eigenValues := eig.Values(nil)
	var maxIndex,maxValue float64
	maxIndex = 0
	maxValue = -1

	//Find out the biggest Real EigenValue 
	for i := 0; i < len(eigenValues); i++ {
		//fmt.Printf("\ntestttttt: %v \n",real(eigenValues[i]))
		if(real(eigenValues[i])>maxValue && imag(eigenValues[i])==0){
			maxIndex=float64(i)
			maxValue=real(eigenValues[i])
		}	
	}

	eigenVectors := eig.VectorsTo(nil)
	


	//fmt.Printf("\n Max value is: %v, and max index is: %v",maxValue,maxIndex)
	
	//fmt.Printf("\noriginal matrix is : %v\n", a)
	//fmt.Printf("Eigen Values are: ", eig.Values(nil))
	
	//fmt.Printf("Eigen Vectors are : %v\n", eigenVectors)

	r,_:=eigenVectors.Dims()
	weights := make([]float64, r)
	//var weights[r] float64

	for i := 0; i < r; i++ {
		weights[i]=real(eigenVectors.At(i,int(maxIndex)))
	}

	fmt.Printf("weights are : %v\n", weights)

	METRIC_COUNT:=4

	//Add it globally in the master
	utilization := make( map[string] []float64)
	utilization["52.32.9.56:8090"] = make([]float64,METRIC_COUNT,METRIC_COUNT)
	utilization["13.53.90.176:8090"] = make([]float64,METRIC_COUNT,METRIC_COUNT)

	UpdateUtilization(&utilization)
	fmt.Printf("The metrics:\n%v\n%v\n",utilization["52.32.9.56:8090"],utilization["13.53.90.176:8090"])
	ChooseEdgeNode(&utilization,&weights)

}
func ChooseEdgeNode(util *map[string][]float64, weights *[]float64) {
	fmt.Printf("We are going to choose according to such weights:\n%v \n and such utilization: \n%v\n",*weights,*util)
}
func UpdateUtilization (util *map[string][]float64) {
	METRIC_COUNT := 4
	//var MetricQueries [METRIC_COUNT]string
	MetricQueries := make([]string,METRIC_COUNT)
	MetricQueries[0] = `sum(rate(container_cpu_usage_seconds_total{job="fogflow",id="/"}[2m] )) by (instance)`
	MetricQueries[1] = `sum(container_memory_working_set_bytes{job="fogflow",id=~"/docker.*"})by(instance)`
	MetricQueries[2] = `sum(container_memory_working_set_bytes{job="fogflow",id=~"/docker.*"}) by(instance) / sum(machine_memory_bytes) by (instance)`
	MetricQueries[3] = `sum(rate(container_memory_failures_total{failure_type="pgmajfault"}[20m])) by (instance)`

	//for address, metric := range *util {
	//	//AAA fmt.Printf("address %v is: %v \n", address, metric)
	//	//fmt.Printf("the value of metrics are: %v",util[address][0])
	//	//return worker.WID
	//}

	//AAA fmt.Printf("the test vector : %v\n", (*util)["13.53.90.176:8090"])
	for metricNumber, metricQuery := range MetricQueries {
		//AAA fmt.Printf("metricnumber is: %v",metricNumber)
		prom_address := "localhost:9090"
		//query := `sum(container_memory_working_set_bytes{job="fogflow",id=~"/docker.*"}) by(instance) / sum(machine_memory_bytes) by (instance)`
		url := "http://" + prom_address + "/api/v1/query?query=" + url2.QueryEscape(metricQuery)
		//AAA fmt.Printf("url:%v", url)
		req, err := http.NewRequest("GET", url, nil)
		//req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Accept", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		text, _ := ioutil.ReadAll(resp.Body)
		var data PromReply
		json.Unmarshal(text, &data)
		//AAA fmt.Printf("\nresponse from prometheus: %v", string(text))

		for _, d := range data.Data.Result {
			//AAA fmt.Printf("\nthis shit is metric:%v", d.Value)
			f, _ := strconv.ParseFloat(d.Value[1], 64)
			(*util)[d.Metric.Instance][metricNumber] = f
		}
	}

	//Normalize the Utlization data
	for metricNumber, _ := range MetricQueries {
		//Normalize data of Each Metric (metricNumber) 
		//for all hosts

		// fmt.Printf("%v: This is host: %v, and this is data: %v\n",metricNumber,host,data[metricNumber])

		//Get the sum
		var sumMetric float64 = 0
		for _,data :=range (*util){
			sumMetric += data[metricNumber]
		}
		//Divide each value to sum --> Normalize
		for _,data :=range (*util){
			data[metricNumber] = divide(data[metricNumber],sumMetric)
		}

	}
}

func divide(a,b float64) float64{
	if math.IsNaN(a/b){
		return 0
	} else {
		return (a/b)

	}
}

type PromReply struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
				Instance string `json:"instance"`
			} `json:"metric"`
			Value []string `json:"value"`
		} `json:"result"`
	} `json:"data"`
}
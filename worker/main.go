package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mmcloughlin/geohash"

	. "github.com/smartfog/fogflow/common/config"
)

func generateID(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func main() {
	configurationFile := flag.String("f", "config.json", "A configuration file")
	id := flag.String("i", "0", "its ID in the current site")

	flag.Parse()
	config, err := LoadConfig(*configurationFile)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%s\n", err.Error()))
		fmt.Println("please specify the configuration file, for example, \r\n\t./worker -f config.json")
		os.Exit(-1)
	}

	geohashID := geohash.EncodeWithPrecision(config.PLocation.Latitude, config.PLocation.Longitude, config.Precision)
	myID := "Worker." + geohashID + "." + (*id)

	// start the worker to deal with tasks
	var worker = &Worker{id: myID}
	ok := worker.Start(&config)
	if ok == false {
		ERROR.Println("failed to start the worker instance")
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c

	worker.Quit()
}

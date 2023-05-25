package main

import (
	dpfm_api_caller "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Caller"
	dpfm_api_input_reader "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-product-master-doc-reads-rmq-kube/config"
	"encoding/json"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"time"
)

func main() {
	l := logger.NewLogger()
	conf := config.NewConf()
	db, err := database.NewMySQL(conf.DB)
	rmq, err := rabbitmq.NewRabbitmqClient(conf.RMQ.URL(), conf.RMQ.QueueFrom(), "", conf.RMQ.QueueToSQL(), 0)
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Close()
	iter, err := rmq.Iterator()
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Stop()

	caller := dpfm_api_caller.NewDPFMAPICaller(conf, rmq, db)

	for msg := range iter {
		l.Debug("received queue message")
		start := time.Now()
		err = callProcess(rmq, caller, conf, msg)
		if err != nil {
			msg.Fail()
			continue
		}
		msg.Success()
		l.Info("process time %v\n", time.Since(start).Milliseconds())
	}
}

func recovery(l *logger.Logger, err *error) {
	if e := recover(); e != nil {
		*err = fmt.Errorf("error occurred: %w", e)
		l.Error(err)
		return
	}
}

func getSessionID(data map[string]interface{}) string {
	id := fmt.Sprintf("%v", data["runtime_session_id"])
	return id
}

func callProcess(rmq *rabbitmq.RabbitmqClient, caller *dpfm_api_caller.DPFMAPICaller, conf *config.Conf, msg rabbitmq.RabbitmqMessage) (err error) {
	l := logger.NewLogger()
	defer recovery(l, &err)

	l.AddHeaderInfo(map[string]interface{}{"runtime_session_id": getSessionID(msg.Data())})
	var input dpfm_api_input_reader.SDC
	var output dpfm_api_output_formatter.SDC

	err = json.Unmarshal(msg.Raw(), &input)
	if err != nil {
		l.Error(err)
		return
	}
	err = json.Unmarshal(msg.Raw(), &output)
	if err != nil {
		l.Error(err)
		return
	}

	res, errs := caller.AsyncReads(&input)

	if len(errs) != 0 {
		for _, err := range errs {
			l.Error(err)
		}

		var errStr interface{}
		errStr = fmt.Sprintf("%v", errs)

		output.Message = errStr

		rmq.Send(conf.RMQ.QueueToResponse(), &output)
		return errs[0]
	}

	output.Message = res

	l.JsonParseOut(output)
	rmq.Send(conf.RMQ.QueueToResponse(), output)

	return nil
}

package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	"data-platform-api-product-master-doc-reads-rmq-kube/config"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncReads(
	input *dpfm_api_input_reader.SDC,
) (interface{}, []error) {
	errs := make([]error, 0, 5)

	var response interface{}
	response = c.readSqlProcess(input, &errs)

	return response, errs
}

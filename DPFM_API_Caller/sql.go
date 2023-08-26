package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var generalDoc *[]dpfm_api_output_formatter.GeneralDoc

	for _, fn := range accepter {
		switch fn {
		case "GeneralDoc":
			func() {
				generalDoc = c.GeneralDoc(input, output, errs, log)
			}()
		}
	}

	data := &dpfm_api_output_formatter.Message{
		GeneralDoc: generalDoc,
	}

	return data
}

func (c *DPFMAPICaller) GeneralDoc(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GeneralDoc {
	where := "WHERE 1 = 1"

	if input.GeneralDoc.Product != nil && len(*input.GeneralDoc.Product) != 0 {
		where = fmt.Sprintf("%s\nAND Product = '%v'", where, *input.GeneralDoc.Product)
	}
	if input.GeneralDoc.DocType != nil && len(*input.GeneralDoc.DocType) != 0 {
		where = fmt.Sprintf("%s\nAND DocType = '%v'", where, *input.GeneralDoc.DocType)
	}
	if input.GeneralDoc.DocIssuerBusinessPartner != nil && *input.GeneralDoc.DocIssuerBusinessPartner != 0 {
		where = fmt.Sprintf("%s\nAND DocIssuerBusinessPartner = %v", where, *input.GeneralDoc.DocIssuerBusinessPartner)
	}
	groupBy := "\nGROUP BY Product, DocType, DocIssuerBusinessPartner "

	rows, err := c.db.Query(
		`SELECT
    Product, DocType, MAX(DocVersionID), DocID, FileExtension, FileName, FilePath, DocIssuerBusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_master_general_doc_data
		` + where + groupBy + `;`)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGeneralDoc(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-master-doc-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
)

func (c *DPFMAPICaller) readSqlProcess(
	input *dpfm_api_input_reader.SDC,
	errs *[]error,
) interface{} {
	headerDoc := c.HeaderDoc(input, errs)

	data := &dpfm_api_output_formatter.Message{
		HeaderDoc: headerDoc,
	}

	return data
}

func (c *DPFMAPICaller) HeaderDoc(
	input *dpfm_api_input_reader.SDC,
	errs *[]error,
) *[]dpfm_api_output_formatter.HeaderDoc {
	where := "WHERE 1 = 1"

	if input.Header.Product != nil && len(*input.Header.Product) != 0 {
		where = fmt.Sprintf("%s\nAND Product = '%v'", where, *input.Header.Product)
	}
	if input.Header.HeaderDoc.DocType != nil && len(*input.Header.HeaderDoc.DocType) != 0 {
		where = fmt.Sprintf("%s\nAND DocType = '%v'", where, *input.Header.HeaderDoc.DocType)
	}
	if input.Header.HeaderDoc.DocIssuerBusinessPartner != nil && *input.Header.HeaderDoc.DocIssuerBusinessPartner != 0 {
		where = fmt.Sprintf("%s\nAND DocIssuerBusinessPartner = %v", where, *input.Header.HeaderDoc.DocIssuerBusinessPartner)
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

	data, err := dpfm_api_output_formatter.ConvertToHeaderDoc(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

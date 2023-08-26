package dpfm_api_input_reader

type SDC struct {
	ConnectionKey     string     `json:"connection_key"`
	Result            bool       `json:"result"`
	RedisKey          string     `json:"redis_key"`
	Filepath          string     `json:"filepath"`
	APIStatusCode     int        `json:"api_status_code"`
	RuntimeSessionID  string     `json:"runtime_session_id"`
	BusinessPartnerID *int       `json:"business_partner"`
	ServiceLabel      string     `json:"service_label"`
	APIType           string     `json:"api_type"`
	GeneralDoc        GeneralDoc `json:"ProductMaster"`
	APISchema         string     `json:"api_schema"`
	Accepter          []string   `json:"accepter"`
	Deleted           bool       `json:"deleted"`
}

type GeneralDoc struct {
	Product                  *string `json:"Product"`
	DocType                  *string `json:"DocType"`
	FileExtension            *string `json:"FileExtension"`
	DocVersionID             *int    `json:"DocVersionID"`
	DocID                    *string `json:"DocID"`
	DocIssuerBusinessPartner *int    `json:"DocIssuerBusinessPartner"`
	FilePath                 *string `json:"FilePath"`
	FileName                 *string `json:"FileName"`
}

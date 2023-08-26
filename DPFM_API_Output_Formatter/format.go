package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToGeneralDoc(rows *sql.Rows) (*[]GeneralDoc, error) {
	defer rows.Close()
	generalDoc := make([]GeneralDoc, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &GeneralDoc{}

		err := rows.Scan(
			&pm.Product,
			&pm.DocType,
			&pm.DocVersionID,
			&pm.DocID,
			&pm.FileExtension,
			&pm.FileName,
			&pm.FilePath,
			&pm.DocIssuerBusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &generalDoc, err
		}

		data := pm
		generalDoc = append(generalDoc, GeneralDoc{
			Product:                  data.Product,
			DocType:                  data.DocType,
			DocVersionID:             data.DocVersionID,
			DocID:                    data.DocID,
			FileExtension:            data.FileExtension,
			FileName:                 data.FileName,
			FilePath:                 data.FilePath,
			DocIssuerBusinessPartner: data.DocIssuerBusinessPartner,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &generalDoc, nil
	}

	return &generalDoc, nil
}

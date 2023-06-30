package dataset

import (
    "github.com/bingLAN/data_driver/common"
    "gorm.io/gorm"
)

type DatasetField struct {
    datasetId   string
    fields      []common.DatasetTableField
}

func (d *DatasetField) getFieldsFromDB(db *gorm.DB) ([]common.DatasetTableField, error) {
    var res []common.DatasetTableField

    err := db.Model(&common.DatasetTableField{}).Where("dataset_id = ?", d.datasetId).Scan(&res).Error
    if err != nil {
        return nil, err
    }

    return res, nil
}

func createDatasetFieldId() string {
    return common.GetUUID()
}

func createDatasetFields(datasetId string, fields []common.DatasetTableField) *DatasetField {
    for index, _ := range fields {
        fields[index].FieldId = createDatasetFieldId()
    }
    
    return &DatasetField{datasetId: datasetId, fields: fields}
}

// 从数据库中找到对应dataset的field

func getDatasetFields(datasetId string, db *gorm.DB) (*DatasetField, error) {
    var fields []common.DatasetTableField
    
    err := db.Model(&common.DatasetTableField{}).Where("dataset_id = ?", datasetId).Scan(&fields).Error
    if err != nil {
        return nil, err
    }
    
    return &DatasetField{datasetId: datasetId, fields: fields}, nil
}


type DatasetFieldService struct {

}

// 从数据库中查找对应数据集的所有field

func (s *DatasetFieldService) GetAllFieldFromDB(datasetId string, db *gorm.DB) ([]common.DatasetTableField, error) {
    var fields []common.DatasetTableField
    
    err := db.Model(&common.DatasetTableField{}).Where("dataset_id = ?", datasetId).Scan(&fields).Error
    if err != nil {
        return nil, err
    }
    
    return fields, nil
}
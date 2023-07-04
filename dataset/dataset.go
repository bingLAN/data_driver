package dataset

import (
    "errors"
    "fmt"
    "github.com/bingLAN/data_driver/common"
    "github.com/bingLAN/data_driver/datasource"
    "github.com/bingLAN/data_driver/db_driver"
    cmap "github.com/orcaman/concurrent-map"
    "gorm.io/gorm"
)

type Dataset struct {
    DatasetInfo *common.DatasetTable
    Fields      *DatasetField
    Datasource  *datasource.Datasource
}

func (ds *Dataset) GetData(datasetId string, db *gorm.DB, offset, limit int, sortNames []string, sortOpt string) (*common.DsResult, error) {
    // 查看数据源是否可用
    status := ds.Datasource.DBDriver.GetDBConnStatus()
    if status != db_driver.ConnSuccess {
        // 尝试恢复连接
        err := ds.Datasource.DBDriver.DBRecovery()
        if err == nil {
            // 连接恢复成功，更新数据库状态
            errDB := db.Model(&common.DatasourceTable{}).Where("datasource_id = ?", ds.DatasetInfo.DatasourceId).Update("status", db_driver.ConnSuccess).Error
            if errDB != nil {
                return nil, err
            }
        } else {
            return nil, errors.New(fmt.Sprintf("datasource not available!"))
        }
    }
    
    // 调用db_driver的接口
    return ds.Datasource.DBDriver.GetData(datasetId, ds.DatasetInfo, ds.Fields.fields, offset, limit, sortNames, sortOpt)
}

func (ds *Dataset) GetFields() []common.DatasetTableField {
    return ds.Fields.fields
}


func createDatasetId() string {
    return common.GetUUID()
}


type Datasets struct {
    sources     *datasource.Datasources
    datasetMap  cmap.ConcurrentMap      // id---*Dataset表
}

func (d *Datasets) GetDatasetById(datasetId string) (*Dataset, error) {
    s, ok := d.datasetMap.Get(datasetId)
    if !ok {
        return nil, errors.New(fmt.Sprintf("datasetMap doesn't have [%s] dataset", datasetId))
    }
    
    return s.(*Dataset), nil
}

func (d *Datasets) ModifyDatasetFields(fields []common.DatasetTableField, db *gorm.DB) error {
    if len(fields) == 0 {
        return nil
    }
    datasetId := fields[0].DatasetId
    s, ok := d.datasetMap.Get(datasetId)
    if !ok {
        return errors.New(fmt.Sprintf("datasetMap doesn't have [%s] dataset", datasetId))
    }
    dataset := s.(*Dataset)

    // 建立map同时同步db

   tx := db.Begin()

    fieldsMap := make(map[string]int)
    for index, _ := range fields {
        fieldId := fields[index].FieldId
        fieldsMap[fieldId] = index

        // 注意：updates只会更新非0字段
        err := tx.Model(&common.DatasetTableField{}).Where("field_id = ?", fields[index].FieldId).Updates(fields[index]).Error
        if err != nil {
            tx.Rollback()
            return err
        }
    }


    // 同步cache
    for index, _ := range dataset.Fields.fields {
        if index2, ok := fieldsMap[dataset.Fields.fields[index].FieldId]; ok {
            dataset.Fields.fields[index].GroupType = fields[index2].GroupType
        }
    }

    tx.Commit()

    return nil
}


// DatasetCacheInit 加载数据集全表
// db后端数据库句柄

func (d *Datasets) datasetCacheInit(db *gorm.DB) error {
    var datasets []common.DatasetTable
    var dvs []Dataset
    
    err := db.Model(&common.DatasetTable{}).Scan(&datasets).Error
    if err != nil {
        return err
    }
    
    // 获取所有数据集信息
    for index, _ := range datasets {
        datasetId := datasets[index].DatasetId
        datasourceId := datasets[index].DatasourceId
        var dv Dataset
        dv.DatasetInfo = &datasets[index]
        fields, err := getDatasetFields(datasetId, db)
        if err != nil {
            return err
        }
        datasource, err := d.sources.GetDatasourceFromCache(datasourceId)
        if err != nil {
            return err
        }
        dv.Fields = fields
        dv.Datasource = datasource
    
        dvs = append(dvs, dv)
    }
    
    // 全部添加到map表中
    for index, _ := range dvs {
        datasetId := dvs[index].DatasetInfo.DatasetId
        dv := &dvs[index]
        d.datasetMap.Set(datasetId, dv)
    }
    
    return nil
}

func (d *Datasets) createDataset(dsTable *common.DatasetTable) (*Dataset, error) {
    var dv Dataset
    
    if dsTable.DatasetId == "" {
        dsTable.DatasetId = createDatasetId()
    }
    
    // 找到对应的数据源
    datasource, err := d.sources.GetDatasourceFromCache(dsTable.DatasourceId)
    if err != nil {
        return nil, err
    }
    
    // 调用驱动层获取fields, fieldId已在驱动层填充
    fields, err := datasource.DBDriver.GetDataFields(*dsTable)
    if err != nil {
        return nil, err
    }
    
    dv.DatasetInfo = dsTable
    dv.Fields = createDatasetFields(dsTable.DatasetId, fields)
    dv.Datasource = datasource
    
    return &dv, nil
}


// 根据数据集info建立field
// 更新数据库datasetTable以及field表。
// 更新datasetMap。

func (d *Datasets) DatasetAdd(dsTable *common.DatasetTable, db *gorm.DB) error {
    // 创建数据对象
    dv, err := d.createDataset(dsTable)
    if err != nil {
        return err
    }
    
    // 更新数据库
    tx := db.Begin()
    err = tx.Model(&common.DatasetTable{}).Create(&dv.DatasetInfo).Error
    if err != nil {
        // 回滚
        tx.Rollback()
        return err
    }
    
    err = tx.Model(&common.DatasetTableField{}).Create(&dv.Fields.fields).Error
    if err != nil {
        // 回滚
        tx.Rollback()
        return err
    }
    // 提交事务
    tx.Commit()
    
    // 更新map表
    d.datasetMap.Set(dv.DatasetInfo.DatasetId, dv)
    
    return nil
}

// 从数据库中查找所有数据集数据

func (d *Datasets) GetAllDatasetFromDB(db *gorm.DB) ([]common.DatasetTable, error) {
    var datasets []common.DatasetTable
    
    err := db.Model(&common.DatasetTable{}).Scan(&datasets).Error
    if err != nil {
        return nil, err
    }
    
    return datasets, nil
}

func (d *Datasets) DatasetDelBySourceID(datasrouceId string, db *gorm.DB) error {
    
    for k, v := range d.datasetMap.Items() {
        ds := v.(*Dataset)
        if ds.DatasetInfo.DatasourceId == datasrouceId {
            err := d.DatasetDel(k, db)
            if err != nil {
                return err
            }
            d.datasetMap.Remove(k)
        }
        
    }
    
    return nil
}


// 更新数据库datasetTable以及field表。
// 更新datasetMap。

func (d *Datasets) DatasetDel(datasetId string, db *gorm.DB) error {
    
    // 先清除数据库
    tx := db.Begin()
    // 先删除field
    err := tx.Where("dataset_id = ?", datasetId).Delete(&common.DatasetTableField{}).Error
    if err != nil {
        // 回滚
        tx.Rollback()
        return err
    }
    // 再删除dataset表
    err = tx.Where("dataset_id = ?", datasetId).Delete(&common.DatasetTableField{}).Error
    if err != nil {
        // 回滚
        tx.Rollback()
        return err
    }
    tx.Commit()
    
    // 再清除map表
    d.datasetMap.Remove(datasetId)
    
    return nil
}

func (d *Datasets) datasetModifyWithField(dsTable common.DatasetTable, db *gorm.DB) error {
    // 删除field以及dataset表
    err := d.DatasetDel(dsTable.DatasetId, db)
    if err != nil {
        return err
    }
    
    // 重新添加
    return d.DatasetAdd(&dsTable, db)
}

func (d *Datasets) datasetModifyWithoutField(dsTable common.DatasetTable, datasetVal *Dataset, db *gorm.DB) error {
    // 更新dataset内容
    err := db.Save(&dsTable).Error
    
    // 更新map节点
    datasetVal.DatasetInfo = &dsTable
    
    return err
}

// 更新数据库datasetTable以及field表。
// 更新datasetMap。

func (d *Datasets) DatasetModify(dsTable common.DatasetTable, db *gorm.DB) error {
    // 对于datasource_id/type/info发生变化的需要同步field表
    val, ok := d.datasetMap.Get(dsTable.DatasetId)
    if !ok {
        return errors.New(fmt.Sprintf("cannot find datasetVal from datasetMap!"))
    }
    datasetVal := val.(*Dataset)
    var err error
    if datasetVal.DatasetInfo.DatasourceId != dsTable.DatasourceId ||
        datasetVal.DatasetInfo.Type != dsTable.Type ||
        datasetVal.DatasetInfo.Info != dsTable.Info {
        err = d.datasetModifyWithField(dsTable, db)
    } else {
        err = d.datasetModifyWithoutField(dsTable, datasetVal, db)
    }
    
    return err
}



func NewDatasets(db *gorm.DB, sources *datasource.Datasources) (*Datasets, error) {
    ds := &Datasets{datasetMap: cmap.New(), sources: sources}
    err := ds.datasetCacheInit(db)
    if err != nil {
        return nil, err
    }
    
    return ds, nil
}
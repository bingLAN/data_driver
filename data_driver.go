package data_driver

import (
    "errors"
    "fmt"
    "github.com/bingLAN/data_driver/common"
    "github.com/bingLAN/data_driver/dataset"
    "github.com/bingLAN/data_driver/datasource"
    "github.com/bingLAN/data_driver/db_driver"
    "gorm.io/gorm"
)

type DataDriver struct {
    datasources     *datasource.Datasources
    datasets        *dataset.Datasets
}

// 根据datasetId找到对应的数据对象，然后调用对应的接口来获取数据
// sortNames: 排序字段，最终会组装成order by的参数
// sortOpt: 排序方式，asc/desc

func (d *DataDriver) GetData(datasetId string, db *gorm.DB, offset, limit int, sortNames []string, sortOpt string, filter string) (*common.DsResult, error) {
    ds, err := d.datasets.GetDatasetById(datasetId)
    if err != nil {
        return nil, err
    }
    
    return ds.GetData(datasetId, db, offset, limit, sortNames, sortOpt, filter)
}

// 该接口用于数据集填写还未下发时查询数据集数据样本

func (d *DataDriver) QueryDataByTable(dsTable common.DatasetTable, offset, limit int, sortNames []string, sortOpt string) (*common.DsResult, error) {
    datasourceId := dsTable.DatasourceId
    datasource, err := d.datasources.GetDatasourceFromCache(datasourceId)
    if err != nil {
        return nil, errors.New(fmt.Sprintf("datasourceId [%s] exist", datasourceId))
    }

    // 调用驱动层获取fields, fieldId已在驱动层填充
    fields, err := datasource.DBDriver.GetDataFields(dsTable)
    if err != nil {
        return nil, err
    }

    return datasource.DBDriver.GetData("", &dsTable, fields, offset, limit, sortNames, sortOpt, "")
}


// 添加数据源

func (d *DataDriver) AddDatasource(dt *common.DatasourceTable, db *gorm.DB) error {
    if dt.DatasourceId != "" {
        // 不支持自定义id
        return errors.New(fmt.Sprintf("datasourceId [%s] exist", dt.DatasourceId))
    }
    
    return d.datasources.CreateDatasource(dt, db)
}

// 删除数据源

func (d *DataDriver) DelDatasource(datasourceId string, db *gorm.DB) error {
    // 删除数据源关联的数据集
    err := d.datasets.DatasetDelBySourceID(datasourceId, db)
    if err != nil {
        return err
    }
    
    return d.datasources.DelDatasourceById(datasourceId, db)
}

// 修改数据源

func (d *DataDriver) ModifyDatasource(dt common.DatasourceTable, db *gorm.DB) error {
    return d.datasources.ModifyDatasource(dt, db)
}

// 查看数据源

func (d *DataDriver) ScanDatasource(db *gorm.DB) ([]common.DatasourceTable, error) {
    return d.datasources.GetDatasourceAll(db)
}

// 测试数据源

func (d *DataDriver) CheckDatasource(dt common.DatasourceTable, db *gorm.DB) (db_driver.DBConnStatus, error) {
    var status db_driver.DBConnStatus
    var err error
    
    datasourceId := dt.DatasourceId
    
    datasource, errC := d.datasources.GetDatasourceFromCache(datasourceId)
    if errC != nil {
        // 该dt还未创建datasource对象，尝试创建看能否成功
        status = d.datasources.TryCreateDatasource(dt)
    } else {
        // 已经创建过的datasource
        status, err = datasource.CheckDatasource(db)
    }

    return status, err
}

// 扫描所有数据集

func (d *DataDriver) ScanDatasets(db *gorm.DB) ([]common.DatasetTable, error) {
    return d.datasets.GetAllDatasetFromDB(db)
}

// 获取单个数据集信息

func (d *DataDriver) GetDataset(datasetId string, db *gorm.DB) (*dataset.Dataset, error) {
    return d.datasets.GetDatasetById(datasetId)
}

// 添加数据集

func (d *DataDriver) AddDataset(dsTable *common.DatasetTable, db *gorm.DB) error {
    return d.datasets.DatasetAdd(dsTable, db)
}

// 删除数据集

func (d *DataDriver) DelDataset(datasetId string, db *gorm.DB) error {
    return d.datasets.DatasetDel(datasetId, db)
}

// 修改数据集

func (d *DataDriver) ModifyDataset(dsTable common.DatasetTable, db *gorm.DB) error {
    return d.datasets.DatasetModify(dsTable, db)
}

func (d *DataDriver) Close() {
    d.datasources.Close()
}

// 查看指定数据集的所有field

func (d *DataDriver) ScanDatasetFields(datasetId string, db *gorm.DB) ([]common.DatasetTableField, error) {
    dataset, err := d.datasets.GetDatasetById(datasetId)
    if err != nil {
        return nil, err
    }

    return dataset.GetFields(), nil
}

// 修改数据集field

func (d *DataDriver) ModifyDatasetFields(fields []common.DatasetTableField, db *gorm.DB) error {
    return d.datasets.ModifyDatasetFields(fields, db)
}


// driver初始化，自动从数据库中加载数据源和数据集

func CreateDataDriver(db *gorm.DB) (*DataDriver, error) {
    // 创建数据源对象
    datasources, err := datasource.NewDatasource(db)
    if err != nil {
        return nil, err
    }
    
    // 创建数据集对象
    datasets, err := dataset.NewDatasets(db, datasources)
    if err != nil {
        return nil, err
    }

    return &DataDriver{datasources: datasources, datasets: datasets}, nil
}
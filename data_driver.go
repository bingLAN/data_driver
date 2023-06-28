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

func (d *DataDriver) GetData(datasetId string, db *gorm.DB, offset, limit int, sortNames []string, sortOpt string) (*common.DsResult, error) {
    ds, err := d.datasets.GetDatasetById(datasetId)
    if err != nil {
        return nil, err
    }
    
    return ds.GetData(datasetId, db, offset, limit, sortNames, sortOpt)
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
package datasource

import (
    "errors"
    "fmt"
    "github.com/bingLAN/data_driver/common"
    "github.com/bingLAN/data_driver/db_driver"
    cmap "github.com/orcaman/concurrent-map"
    "gorm.io/gorm"
)


type Datasource struct {
    datasourceType string
    tableInfo        common.DatasourceTable
    DBDriver         db_driver.DBDriver
}


// 测试数据源连接

func (s *Datasource) CheckDatasource(db *gorm.DB) (db_driver.DBConnStatus, error) {
    var err error
    
    oldStatus := s.tableInfo.Status
    nowStatus := s.DBDriver.CheckDBConnStatus()
    
    if oldStatus != nowStatus {
        // 状态发生了改变
        s.tableInfo.Status = nowStatus
        // 同步数据库
        err = db.Model(&common.DatasourceTable{}).Where("datasource_id = ?", s.tableInfo.DatasourceId).Update("status", nowStatus).Error
    }
    
    return nowStatus, err
}

type Datasources struct {
    dbDriverMap  cmap.ConcurrentMap     // id---*Datasource
}

func createDatasourceId() string {
    return common.GetUUID()
}

// 从数据库中查看所有数据源

func (ds *Datasources) GetDatasourceAll(db *gorm.DB) ([]common.DatasourceTable, error) {
    var sourceList []common.DatasourceTable
    
    err := db.Model(&common.DatasourceTable{}).Scan(&sourceList).Error
    if err != nil {
        return nil, err
    }

    return sourceList, nil
}

// 从缓存中获取数据源

func (ds *Datasources) GetDatasourceFromCache(datasourceId string) (*Datasource, error) {
    source, ok := ds.dbDriverMap.Get(datasourceId)
    if !ok {
        return nil, errors.New(fmt.Sprintf("dbDriverMap not have this source[%s]", datasourceId))
    }
    
    return source.(*Datasource), nil
}

// 删除数据源

func (ds *Datasources) DelDatasourceById(sourceId string, db *gorm.DB) error {
    datasource, ok := ds.dbDriverMap.Get(sourceId)
    if !ok {
        return errors.New(fmt.Sprintf("dbDriverMap not have this source[%s]", sourceId))
    }
    
    // 下发到driver层
    source := datasource.(Datasource)
     _ = source.DBDriver.Close()
    source.DBDriver = nil
    
    // 从缓存中移除
    ds.dbDriverMap.Remove(sourceId)
    
    // 从数据库中删除
    err := db.Where("datasource_id = ?", sourceId).Delete(&common.DatasourceTable{}).Error
    
    return err
}

// 修改数据源

func (ds *Datasources) ModifyDatasource(dt common.DatasourceTable, db *gorm.DB) error {
    // 删除源datasource，重新创建
    datasource, ok := ds.dbDriverMap.Get(dt.DatasourceId)
    if !ok {
        return errors.New(fmt.Sprintf("dbDriverMap not have this source[%s]", dt.DatasourceId))
    }
    
    // 下发到driver层
    source := datasource.(*Datasource)
    _ = source.DBDriver.Close()
    source.DBDriver = nil
    
    // 从缓存中移除
    ds.dbDriverMap.Remove(dt.DatasourceId)
    
    // 重新创建
    err := ds.createDatasourceStruct(&dt, db)
    if err != nil {
        return err
    }
    
    // 更新数据库
    err = db.Model(&common.DatasourceTable{}).Where("datasource_id = ?", dt.DatasourceId).Updates(dt).Error
    return err
}

// 若db为nil则不同步数据库

func (ds *Datasources) createDatasourceStruct(dt *common.DatasourceTable, db *gorm.DB) error {
    // 创建数据源id
    if dt.DatasourceId == "" {
        datasourceId := createDatasourceId()
        dt.DatasourceId = datasourceId
    }
    
    // 根据类型创建不同数据源
    var err error
    switch dt.Type {
    case db_driver.DatasourceCH:
        var dbDriver db_driver.DBDriver
        dbDriver, err = db_driver.NewClickhouseDriver(*dt)
        if err == nil {
            // 存入缓存表
            if dt.Status != db_driver.ConnSuccess {
                dt.Status = db_driver.ConnSuccess
                // 同步数据库
                if db != nil {
                    db.Model(&common.DatasourceTable{}).Where("datasource_id = ?", dt.DatasourceId).Update("status", dt.Status)
                }
            }
            
            ds.dbDriverMap.Set(dt.DatasourceId, &Datasource{
                datasourceType: dt.Type,
                tableInfo: *dt,
                DBDriver: dbDriver,
            })
        } else {
            dt.Status = db_driver.ConnFail
        }
    case db_driver.DatasourceMYSQL:
    
    default:
        err = errors.New(fmt.Sprintf("datasource type [%s] not support", dt.Type))
    }
    
    return err
}

// 新建数据源

func (ds *Datasources) CreateDatasource(dt *common.DatasourceTable, db *gorm.DB) error {
    // 创建数据对象
    err := ds.createDatasourceStruct(dt, db)
    if err != nil {
        return err
    }
    
    // 同步数据库
    dbErr := db.Model(dt).Create(dt).Error
    if dbErr != nil {
        return dbErr
    }
    
    return nil
}

// 检查临时数据源的连接

func (ds *Datasources) TryCreateDatasource(dt common.DatasourceTable) db_driver.DBConnStatus {
    err := ds.createDatasourceStruct(&dt, nil)
    if err != nil {
        return  db_driver.ConnFail
    }
    
    return db_driver.ConnSuccess
}

func (ds *Datasources) Close() {
    for _, v := range ds.dbDriverMap.Items() {
        source := v.(*Datasource)
        source.DBDriver.Close()
    }
}

func NewDatasource(db *gorm.DB) (*Datasources, error) {
    s := &Datasources{dbDriverMap: cmap.New()}
    
    // 从数据库中加载所有数据源
    sourceList, err := s.GetDatasourceAll(db)
    if err != nil {
        return nil, err
    }
    
    // 加载
    for index, _ := range sourceList {
        node := &sourceList[index]
        err = s.createDatasourceStruct(node, db)
        if err != nil {
            return nil, err
        }
    }
    
    return s, nil
}

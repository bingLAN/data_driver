package data_driver

import (
    "encoding/json"
    "fmt"
    "github.com/bingLAN/data_driver/common"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "testing"
)

func gormMysqlInit()(*gorm.DB, error) {
    dsn := "root" + ":" + "Password123@mysql" + "@tcp(" + "192.168.5.242" + ":" + "3306" + ")/" + "ds_test" + "?" + "charset=utf8mb4&parseTime=True&loc=Local"
    
    mysqlConfig := mysql.Config{
        DSN:                       dsn, // DSN data source name
        DefaultStringSize:         191,     // string 类型字段的默认长度
        SkipInitializeWithVersion: false,   // 根据版本自动配置
    }
    if db, err := gorm.Open(mysql.New(mysqlConfig), &gorm.Config{}); err != nil {
        return nil, err
    } else {
        sqlDB, _ := db.DB()
        sqlDB.SetMaxIdleConns(10)
        sqlDB.SetMaxOpenConns(100)
        return db, nil
    }
}

func TestData(t *testing.T) {
    db, err := gormMysqlInit()
    if err != nil {
        t.Fatal(err)
    }
    
    dd, err := CreateDataDriver(db)
    if err != nil  {
        t.Fatal(err)
    }
    
    // 创建数据源
    datasource := common.DatasourceTable{
        Name: "test",
        Type: "clickhouse",
        Config: common.Configuration{
            MinPoolSize: 5,
            MaxPoolSize: 50,
            MaxIdleTime: 30,
            ConnectTimeout: 5,
            QueryTimeout: 30,
            Host: "192.168.5.245",
            DataBase: "DPI_DB",
            Username: "default",
            Password: "RootSi314",
            Port: "9900",
        },
    }
    
    err = dd.AddDatasource(&datasource, db)
    if err != nil {
        t.Fatal(err)
    }
    
    // 创建数据集
    dataset := common.DatasetTable{
        Name: "set_test",
        DatasourceId: datasource.DatasourceId,
        Type: "sql",
        Info: "select dictGet('city_dictionary', 'item_str', toUInt64(server_prov)) as server_prov_str , sum(ul_byte_count) + sum(dl_byte_count) as total_bytes, sum(ul_pkt_count) + sum(ul_byte_count) as total_pps from com_table group by server_prov_str order by total_bytes",
    }
    
    err = dd.AddDataset(&dataset, db)
    if err != nil {
        t.Fatal(err)
    }

    // 查看数据
    res, err := dd.GetData(dataset.DatasetId, db, 0, 1000, nil, "", "")
    if err != nil {
        t.Fatal(err)
    }
    
    js, _ := json.Marshal(res)
    fmt.Printf("%s", js)
    dd.Close()
}

func TestQuery(t *testing.T) {
    db, err := gormMysqlInit()
    if err != nil {
        t.Fatal(err)
    }
    
    dd, err := CreateDataDriver(db)
    if err != nil  {
        t.Fatal(err)
    }
    
    s, err := dd.CheckDatasource(common.DatasourceTable{
        DatasourceId: "1dacaad4-dc23-4b39-bc8e-882345393dce",
    }, db)
    if err != nil {
        t.Fatal(err)
    }
    fmt.Println(s)
    
    res, err := dd.GetData("46b8ed35-9fa0-43b7-8249-3f4860516890", db, 0, 1000, nil, "", "server_prov_str='北京市'")
    if err != nil {
        t.Fatal(err)
    }
    
    js, _ := json.Marshal(res)
    fmt.Printf("%s", js)
    dd.Close()
}



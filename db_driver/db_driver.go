package db_driver

import (
    "github.com/bingLAN/data_driver/common"
)


type DBConnStatus = int

const (
    Unknown DBConnStatus = iota - 1
    ConnSuccess     // 0
    ConnFail        // 1
)


func string2DBConnStatus(s string) DBConnStatus {
    switch s {
    case "Success":
        return ConnSuccess
    case "Fail":
        return ConnFail
    }
    
    return Unknown
}


const (
    DatasourceCH string = "clickhouse"
    DatasourceMYSQL string = "mysql"
)

type DBDriverHandle struct {
    CreateFunc  func(datasourceInfo common.DatasourceTable) (DBDriver, error)
}

var DBDriverMap = map[string] DBDriverHandle {
    DatasourceCH: {CreateFunc: NewClickhouseDriver},
    DatasourceMYSQL: {CreateFunc: NewMysqlDriver},
}

type DBDriver interface {
    DBRecovery() error      // 建立池恢复
    Close() error           // 关闭
    GetDBConnStatus() DBConnStatus      // 查看数据记录的连接状态
    CheckDBConnStatus() DBConnStatus    // 调用api查看当前连接状态
    GetDataFields(dsTable common.DatasetTable) ([]common.DatasetTableField, error)  // 获取该数据集所有field域信息
    GetData(datasetId string, di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string) (*common.DsResult, error) // 数据访问
}

type FieldDef struct {
    Name string             //  字段名名
    GroupType string        //  维度/指标标识 d:维度，q:指标
    ColumnIndex int64       //  列位置
}

type FieldDefList []FieldDef

// 下列接口实现切片排序

func (f FieldDefList) Len() int {
    return len(f)
}

func (f FieldDefList) Swap(i, j int) {
    f[i], f[j] = f[j], f[i]
}

func (f FieldDefList) Less(i, j int) bool {
    return f[i].ColumnIndex > f[j].ColumnIndex
}

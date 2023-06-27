package common

import (
    "database/sql/driver"
    "encoding/json"
    "time"
)

type Configuration struct {
    ExtraParams     string      `json:"extraParams" form:"extraParams"`
    MinPoolSize     uint        `json:"minPoolSize" form:"minPoolSize"`
    MaxPoolSize     uint        `json:"maxPoolSize" form:"maxPoolSize"`
    MaxIdleTime     uint        `json:"maxIdleTime" form:"maxIdleTime"`
    ConnectTimeout  uint        `json:"connectTimeout" form:"connectTimeout"`
    QueryTimeout    uint        `json:"queryTimeout" form:"queryTimeout"`
    Host            string      `json:"host" form:"host"`
    DataBase        string      `json:"dataBase" form:"dataBase"`
    Username        string      `json:"username" form:"username"`
    Password        string      `json:"password" form:"password"`
    Port            string      `json:"port" form:"port"`
}

func (c Configuration) Value() (driver.Value, error) {
    marshal, err := json.Marshal(c)
    if err != nil {
        return nil, err
    }
    return string(marshal), nil
}

func (c *Configuration) Scan(value interface{}) error {
    if err := json.Unmarshal(value.([]byte), &c); err != nil {
        return err
    }
    return nil
}

type DatasourceTable struct {
    DatasourceId    string  `gorm:"column:datasource_id" db:"datasource_id" json:"datasource_id" form:"datasource_id"`  //  数据源id
    Name            string  `gorm:"column:name" db:"name" json:"name" form:"name"`  //  数据源名称
    Desc            string  `gorm:"column:desc" db:"desc" json:"desc" form:"desc"`  //  描述
    Type            string  `gorm:"column:type" db:"type" json:"type" form:"type"`  //  数据库类型
    Config          Configuration  `gorm:"column:configuration" db:"configuration" json:"configuration" form:"configuration"`  //  详细配置
    Status          int     `gorm:"column:status" db:"status" json:"status" form:"status"`  //  状态，0：成功，1：失败
    CreatTime       time.Time   `gorm:"column:create_time;autoCreateTime" db:"create_time" json:"create_time" form:"create_time"`
    UpdateTime      time.Time   `gorm:"column:update_time;autoUpdateTime" db:"update_time" json:"update_time" form:"update_time"`
    CreateBy        string  `gorm:"column:create_by" db:"create_by" json:"create_by" form:"create_by"`
}



func (DatasourceTable) TableName() string {
    return "datasource"
}

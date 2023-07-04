package common

import "time"

const (
    FieldDimension = "d"
    FieldQuota = "q"
)

const (
    DatasetTypeDB = "db"
    DatasetTypeSQL = "sql"
)

const (
    DSTypeVar int64 = 0     //文本
    DSTypeTime int64 = 1    //时间
    DSTypeInt int64 = 2     //整形
    DSTypeDEC int64 = 3     //浮点
    DSTypeBit int64 = 4
)

type DatasetTable struct {
    DatasetId string `gorm:"primaryKey;column:dataset_id" db:"dataset_id" json:"dataset_id" form:"dataset_id"`  //  数据集id
    Name string `gorm:"column:name" db:"name" json:"name" form:"name"`
    DatasourceId string `gorm:"column:datasource_id" db:"datasource_id" json:"datasource_id" form:"datasource_id"`  //  数据源id
    Type string `gorm:"column:type" db:"type" json:"type" form:"type"`  //  db,sql,excel,custom
    Mode int64 `gorm:"column:mode" db:"mode" json:"mode" form:"mode"`  //  连接模式：0-直连，1-定时同步
    Info string `gorm:"column:info" db:"info" json:"info" form:"info"`  //  数据集内容: DB/SQL
    CreateBy string `gorm:"column:create_by" db:"create_by" json:"create_by" form:"create_by"`  //  创建人id
    CreateTime time.Time `gorm:"column:create_time;autoCreateTime" db:"create_time" json:"create_time" form:"create_time"`  //  创建时间
    QrtzInstance string `gorm:"column:qrtz_instance" db:"qrtz_instance" json:"qrtz_instance" form:"qrtz_instance"`
    SyncStatus string `gorm:"column:sync_status" db:"sync_status" json:"sync_status" form:"sync_status"`
    LastUpdateTime time.Time `gorm:"column:last_update_time;autoUpdateTime" db:"last_update_time" json:"last_update_time" form:"last_update_time"`
    SqlVariableDetails string `gorm:"column:sql_variable_details" db:"sql_variable_details" json:"sql_variable_details" form:"sql_variable_details"`
}

func (DatasetTable) TableName() string {
    return "dataset_table"
}

type DatasetTableField struct {
    FieldId string `gorm:"primaryKey;column:field_id" db:"field_id" json:"field_id" form:"field_id"`  //  数据集域id
    DatasetId string `gorm:"column:dataset_id" db:"dataset_id" json:"dataset_id" form:"dataset_id"`  //  数据集id
    OriginName string `gorm:"column:origin_name" db:"origin_name" json:"-" form:"-"`  //  原始字段名
    Name string `gorm:"column:name" db:"name" json:"name" form:"name"`  //  字段名名
    GroupType string `gorm:"column:group_type" db:"group_type" json:"group_type" form:"group_type"`  //  维度/指标标识 d:维度，q:指标
    Type string `gorm:"column:type" db:"type" json:"type" form:"type"`  //  原始字段类型
    Size int64 `gorm:"column:size" db:"size" json:"-" form:"-"`
    DsType int64 `gorm:"column:ds_type" db:"ds_type" json:"-" form:"-"`  //  dataease字段类型：0-文本，1-时间，2-整型数值，3-浮点数值...
    ExtField int64 `gorm:"column:ext_field" db:"ext_field" json:"-" form:"-"`  //  是否扩展字段 0否 1是
    Checked int64 `gorm:"column:checked" db:"checked" json:"-" form:"-"`  //  是否选中 0:否 1：是
    ColumnIndex int64 `gorm:"column:column_index" db:"column_index" json:"column_index" form:"column_index"`  //  列位置
    LastSyncTime time.Time `gorm:"column:last_sync_time;autoCreateTime;autoUpdateTime" db:"last_sync_time" json:"-" form:"-"`  //  同步时间
    Accuracy int64 `gorm:"column:accuracy" db:"accuracy" json:"-" form:"-"`  //  精度
    DateFormat string `gorm:"column:date_format" db:"date_format" json:"-" form:"-"`
    DateFormatType string `gorm:"column:date_format_type" db:"date_format_type" json:"-" form:"-"`  //  时间格式类型
}

func (DatasetTableField) TableName() string {
    return "dataset_table_field"
}



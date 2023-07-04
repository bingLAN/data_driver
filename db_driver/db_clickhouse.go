package db_driver

import (
    "context"
    "errors"
    "fmt"
    "github.com/bingLAN/data_driver/common"
    "gorm.io/driver/clickhouse"
    "gorm.io/gorm"
    "sort"
    "strings"
    "time"
)

type ClickhouseDriver struct {
    dbConn              *gorm.DB
    datasourceInfo      common.DatasourceTable
}

func (c *ClickhouseDriver) updateStatus(myDB *gorm.DB) error {
    
    err := myDB.Model(&c.datasourceInfo).Update("status", c.datasourceInfo.Status).Error
    
    return err
}

// DBConn 创建连接
// 建立连接的同时根据结果更新数据库中source表对应状态信息

func (c *ClickhouseDriver) DBConn() error {
    // 封装dsn
    dsn := fmt.Sprintf("tcp://%s:%s?database=%s&username=%s&password=%s&dial_timeout=%d&read_timeout=%d",
        c.datasourceInfo.Config.Host, c.datasourceInfo.Config.Port, c.datasourceInfo.Config.DataBase, c.datasourceInfo.Config.Username, c.datasourceInfo.Config.Password, c.datasourceInfo.Config.ConnectTimeout,
        c.datasourceInfo.Config.QueryTimeout)
    if c.datasourceInfo.Config.ExtraParams != "" {
        dsn = dsn + "&" + c.datasourceInfo.Config.ExtraParams
    }
    
    // 创建连接池
    chConfig := clickhouse.Config{
        DSN: dsn,
    }
    db, err := gorm.Open(clickhouse.New(chConfig), &gorm.Config{})
    if err != nil {
        c.datasourceInfo.Status = ConnFail
        return err
    }
    
    sqlDB, _ := db.DB()
    sqlDB.SetMaxIdleConns(int(c.datasourceInfo.Config.MaxIdleTime))
    sqlDB.SetMaxOpenConns(int(c.datasourceInfo.Config.MaxPoolSize))
    sqlDB.SetConnMaxIdleTime(time.Duration(c.datasourceInfo.Config.ConnectTimeout) * time.Second)
    c.dbConn = db
    c.datasourceInfo.Status = ConnSuccess

    return nil
}

// DBRecovery 重新建立连接
// 恢复连接的同时根据结果更新数据库中source表对应状态信息

func (c *ClickhouseDriver) DBRecovery() error {
    sqlDB, err := c.dbConn.DB()
    if err == nil {
        _ = sqlDB.Close()
    }
    c.dbConn = nil
    
    return c.DBConn()
}

// 查看数据记录的连接状态

func (c *ClickhouseDriver) GetDBConnStatus() DBConnStatus {
    return c.datasourceInfo.Status
}

// GetDBConnStatus 获取连接状态，ConnSuccess: 连接可用，ConnFail：连接不可用

func (c *ClickhouseDriver) CheckDBConnStatus() DBConnStatus {
    if c.dbConn == nil {
        // 重新建立连接
        err := c.DBConn()
        if err != nil {
            return ConnFail
        }
        
        return ConnSuccess
    }
    
    sqlDB, _ := c.dbConn.DB()
    ctx, _ := context.WithTimeout(context.Background(), time.Duration(c.datasourceInfo.Config.ConnectTimeout) * time.Second)
    
    err := sqlDB.PingContext(ctx)
    if err != nil {
        c.datasourceInfo.Status = ConnFail
        return ConnFail
    }
    
    return ConnSuccess
}

// 删除连接

func (c *ClickhouseDriver) Close() error {
    sqlDB, err := c.dbConn.DB()
    if err != nil {
        _ = sqlDB.Close()
    }
    c.dbConn = nil
    
    return nil
}

func (c *ClickhouseDriver) sqlSortFieldCheck(fields []common.DatasetTableField, sortNames []string) error {
    fieldNameMap := make(map[string]struct{})
    for index, _ := range fields {
        fieldNameMap[fields[index].Name] = struct{}{}
    }
    
    // 检查sortName是否在数据集中有定义
    for index, _ := range sortNames {
        if _, ok := fieldNameMap[sortNames[index]]; !ok {
            // sort字段未定义
            return errors.New(fmt.Sprintf("sort name [%s] not define in dataset", sortNames[index]))
        }
    }
    
    return nil
}

// 构建排序字段
func (c *ClickhouseDriver) sqlSortBuild(fields []common.DatasetTableField, sortNames []string, sortOpt string) (string, error) {
    if sortNames == nil {
        // 不需要排序
        return "", nil
    }
    
    // 检查排序字段是否都存在
    fieldErr := c.sqlSortFieldCheck(fields, sortNames)
    if fieldErr != nil {
        return "", fieldErr
    }
    output := strings.Join(sortNames, ` `)
    sortSql := fmt.Sprintf("%s %s", output, sortOpt)
    
    return sortSql, nil
}

func (c *ClickhouseDriver) sqlBuildDB(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string, filter string) (string, error) {
    var sql string
    
    sortSql, err := c.sqlSortBuild(fields, sortNames, sortOpt)
    if err != nil {
        return "", err
    }
    if filter == "" {
        sql = fmt.Sprintf("select * from %s", di.Info)
    } else {
        sql = fmt.Sprintf("select * from %s where %s", di.Info, filter)
    }

    if sortSql != "" {
        sql += fmt.Sprintf(" order by %s", sortSql)
    }
    
    // 仅在分页或limit字段有效时才构建
    if !(offset == 0 && limit == 0) {
        sql += fmt.Sprintf(" limit %d offset %d", limit, offset)
    }
    
    return sql, nil
}

func (c *ClickhouseDriver) sqlBuildSQL(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string, filter string) (string, error) {
    var sql string
    
    sortSql, err := c.sqlSortBuild(fields, sortNames, sortOpt)
    if err != nil {
        return "", err
    }
    if filter == "" {
        sql = fmt.Sprintf("select * from (%s)", di.Info)
    } else {
        sql = fmt.Sprintf("select * from (%s) where %s", di.Info, filter)
    }

    if sortSql != "" {
        sql += fmt.Sprintf(" order by %s", sortSql)
    }
    
    // 仅在分页或limit字段有效时才构建
    if !(offset == 0 && limit == 0) {
        sql += fmt.Sprintf(" limit %d offset %d", limit, offset)
    }
    
    return sql, nil
}

func (c *ClickhouseDriver) sqlBuild(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string, filter string) (string, error) {
    
    // 根据db/sql类型分别组装sql
    var sql string
    var err error
    switch di.Type {
    case common.DatasetTypeDB:
        sql, err = c.sqlBuildDB(di, fields, offset, limit, sortNames, sortOpt, filter)
    case common.DatasetTypeSQL:
        sql, err = c.sqlBuildSQL(di, fields, offset, limit, sortNames, sortOpt, filter)
    default:
        return "", errors.New(fmt.Sprintf("dataset type [%s] not define", c.datasourceInfo.Type))
    }
    if err != nil {
        return "", err
    }
    
    return sql, nil
}

func (c *ClickhouseDriver) sqlExec(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string, filter string) ([]common.SqlRes, error) {
    sql, err := c.sqlBuild(di, fields, offset, limit, sortNames, sortOpt, filter)
    if err != nil {
        return nil, err
    }
    
    // 执行sql
    var result []common.SqlRes
    dbErr := c.dbConn.Raw(sql).Scan(&result).Error
    if dbErr != nil {
        return nil, dbErr
    }
    
    return result, nil
}

// 遍历TableRow，根据维度信息以及列序号封装X结构
func (c *ClickhouseDriver) xAxis(sqlRes []common.SqlRes, fields []common.DatasetTableField) ([]string, FieldDefList) {
    var dimensionList FieldDefList
    
    // 整理维度field，并按照ColumnIndex排序
    for index, _ := range fields {
        field := fields[index]
        if field.GroupType == common.FieldDimension {
            dimensionList = append(dimensionList,
                FieldDef{
                    Name: field.Name,
                    GroupType: field.GroupType,
                    ColumnIndex: field.ColumnIndex,
                },
            )
        }
    }
    
    sort.Sort(dimensionList)
    
    // 根据维度序列，组装xAxis
    var xAxis []string
    for index, _ := range sqlRes {
        row := sqlRes[index]
        // 多个维度字段之间用"\n"隔开
        var names []string
        for _, dimension := range dimensionList {
            if v, ok := row[dimension.Name]; ok {
                s := fmt.Sprintf("%v", v)
                names = append(names, s)
            }
        }
        x := strings.Join(names, "\n")
        
        xAxis = append(xAxis, x)
    }
    
    return xAxis, dimensionList
}

func (c *ClickhouseDriver) getDimensionFromSqlRes(row common.SqlRes, dimensionList FieldDefList) []string {
    var dimension []string
    // 组装维度值
    for _, dim := range dimensionList {
        if dimV, ok := row[dim.Name]; ok {
            dimStr := fmt.Sprintf("%v", dimV)
            dimension = append(dimension, dimStr)
        }
    }
    
    return dimension
}

// 根据指标字段分类展示各维度的value值
func (c *ClickhouseDriver) series(sqlRes []common.SqlRes, fields []common.DatasetTableField, dimensionList FieldDefList) []common.DsSeries {
    var quotaList FieldDefList
    
    // 整理指标field，并按照ColumnIndex排序
    for index, _ := range fields {
        field := fields[index]
        if field.GroupType == common.FieldQuota {
            quotaList = append(quotaList,
                FieldDef{
                    Name: field.Name,
                    GroupType: field.GroupType,
                    ColumnIndex: field.ColumnIndex,
                },
            )
        }
    }
    
    var quotaMap map[string][]common.DsData
    quotaMap = make(map[string][]common.DsData)
    // 遍历每一行res
    for index, _ := range sqlRes {
        // 组装改行的所有维度值
        dimension := c.getDimensionFromSqlRes(sqlRes[index], dimensionList)
        for _, quota := range quotaList {
            quotaName := quota.Name
            // 组装该指标的data数据
            quotaMap[quotaName] = append(quotaMap[quotaName],
                common.DsData{
                    Value: sqlRes[index][quotaName],
                    Name:  dimension,
                },
            )
        }
    }
    
    var series []common.DsSeries
    // 每行根据指标字段，分类维度-指标值
    for k, v := range quotaMap {
        series = append(series,
            common.DsSeries{
                Name: k,
                Data: v,
            },
        )
    }
    
    return series
}

// 根据sql执行结果，封装DsResult结构

func (c *ClickhouseDriver) GetData(datasetId string, di *common.DatasetTable, fields []common.DatasetTableField,
    offset, limit int, sortNames []string, sortOpt string, filter string) (*common.DsResult, error) {
    sqlRes, err := c.sqlExec(di, fields, offset, limit, sortNames, sortOpt, filter)
    if err != nil {
        return nil, err
    }
    
    var dsRes common.DsResult
    var dimensionList FieldDefList
    
    dsRes.X, dimensionList = c.xAxis(sqlRes, fields)
    dsRes.Fields = fields
    dsRes.TableRow = sqlRes
    
    dsRes.Series = c.series(sqlRes, fields, dimensionList)
    
    return &dsRes, nil
}

func (c *ClickhouseDriver) buildDBTypeSQL(table string) string {
    return fmt.Sprintf("SELECT * FROM %s LIMIT 1", table)
}

func (c *ClickhouseDriver) buildSqlTypeSQL(sql string) string {
    return  fmt.Sprintf("%s LIMIT 1", sql)
}

func (c *ClickhouseDriver) getFieldsBySQL(sql string, datasetId string) ([]common.DatasetTableField, error) {
    var datasetFields []common.DatasetTableField
    
    db := c.dbConn
    
    rows, err := db.Raw(sql).Rows()
    if err != nil {
        return nil, err
    }
    
    colTypes, err := rows.ColumnTypes()
    if err != nil {
        return nil, err
    }
    
    for index, _ := range colTypes {
        col := colTypes[index]
        size := int64(0)
        size, _ = col.Length()
        
        baseType := col.DatabaseTypeName()
        dsType := getDatasetTypeCH(baseType)
        
        datasetFields = append(datasetFields,
            common.DatasetTableField{
                FieldId: getDatasetFieldId(),
                DatasetId: datasetId,
                OriginName: col.Name(),
                Name: col.Name(),
                GroupType: common.FieldDimension,
                Type: baseType,
                Size: size,
                DsType: dsType,
                Checked: 1,
                ColumnIndex: int64(index),
            })
    }
    
    return datasetFields, nil
}



func (c *ClickhouseDriver) GetDataFields(dsTable common.DatasetTable) ([]common.DatasetTableField, error) {
    var sql string
    
    switch dsTable.Type {
    case common.DatasetTypeDB:
        sql = c.buildDBTypeSQL(dsTable.Info)
    case common.DatasetTypeSQL:
        sql = c.buildSqlTypeSQL(dsTable.Info)
    default:
        return nil, errors.New(fmt.Sprintf("dataset type [%s] not support", dsTable.Type))
    
    }
    
    return c.getFieldsBySQL(sql, dsTable.DatasetId)
    
}

func getDatasetTypeCH(baseType string) int64 {
    switch baseType {
    case "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256":
        return common.DSTypeInt
    case "Int8", "Int16", "Int32", "Int64", "Int128", "Int256":
        return common.DSTypeInt
    case "Float32", "Float64", "Decimal":
        return common.DSTypeDEC
    case "Date", "Date32", "DateTime", "DateTime64":
        return common.DSTypeTime
    default:
        return common.DSTypeVar
    }
}

func getDatasetFieldId() string {
    return common.GetUUID()
}

func NewClickhouseDriver(datasourceInfo common.DatasourceTable) (DBDriver, error) {
    source := &ClickhouseDriver{datasourceInfo: datasourceInfo}
    err := source.DBConn()
    if err != nil {
        return nil, err
    }
    
    return source, nil
}

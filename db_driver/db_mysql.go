package db_driver

import (
    "context"
    "errors"
    "fmt"
    "github.com/bingLAN/data_driver/common"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "sort"
    "strings"
    "time"
)

type MysqlDriver struct {
    dbConn              *gorm.DB
    datasourceInfo      common.DatasourceTable
}

// DBConn 创建连接
// 建立连接的同时根据结果更新数据库中source表对应状态信息

func (m *MysqlDriver) DBConn() error {
    // 封装dsn
    dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
        m.datasourceInfo.Config.Username, m.datasourceInfo.Config.Password, m.datasourceInfo.Config.Host, m.datasourceInfo.Config.Port,
        m.datasourceInfo.Config.DataBase)
    if m.datasourceInfo.Config.ExtraParams != "" {
        dsn = dsn + "&" + m.datasourceInfo.Config.ExtraParams
    }

    mysqlConfig := mysql.Config{
        DSN:                       dsn, // DSN data source name
        DefaultStringSize:         191,     // string 类型字段的默认长度
        SkipInitializeWithVersion: false,   // 根据版本自动配置
    }

    // 创建连接池
    db, err := gorm.Open(mysql.New(mysqlConfig), &gorm.Config{})
    if err != nil {
        return err
    }

    sqlDB, _ := db.DB()
    sqlDB.SetMaxIdleConns(int(m.datasourceInfo.Config.MaxIdleTime))
    sqlDB.SetMaxOpenConns(int(m.datasourceInfo.Config.MaxPoolSize))
    sqlDB.SetConnMaxIdleTime(time.Duration(m.datasourceInfo.Config.ConnectTimeout) * time.Second)
    m.dbConn = db
    m.datasourceInfo.Status = ConnSuccess

    return nil
}

func (m *MysqlDriver) sqlSortFieldCheck(fields []common.DatasetTableField, sortNames []string) error {
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
func (m *MysqlDriver) sqlSortBuild(fields []common.DatasetTableField, sortNames []string, sortOpt string) (string, error) {
    if sortNames == nil {
        // 不需要排序
        return "", nil
    }

    // 检查排序字段是否都存在
    fieldErr := m.sqlSortFieldCheck(fields, sortNames)
    if fieldErr != nil {
        return "", fieldErr
    }
    output := strings.Join(sortNames, ` `)
    sortSql := fmt.Sprintf("%s %s", output, sortOpt)

    return sortSql, nil
}

func (m *MysqlDriver) sqlBuildDB(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string) (string, error) {
    var sql string

    sortSql, err := m.sqlSortBuild(fields, sortNames, sortOpt)
    if err != nil {
        return "", err
    }
    if sortSql == "" {
        sql = fmt.Sprintf("select * from %s", di.Info)
    } else {
        sql = fmt.Sprintf("select * from %s order by %s", di.Info, sortSql)
    }

    // 仅在分页或limit字段有效时才构建
    if !(offset == 0 && limit == 0) {
        sql += fmt.Sprintf(" limit %d offset %d", limit, offset)
    }

    return sql, nil
}

func (m *MysqlDriver) sqlBuildSQL(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string) (string, error) {
    var sql string

    sortSql, err := m.sqlSortBuild(fields, sortNames, sortOpt)
    if err != nil {
        return "", err
    }
    if sortSql == "" {
        sql = fmt.Sprintf("select * from (%s) t", di.Info)
    } else {
        sql = fmt.Sprintf("select * from (%s) t order by %s", di.Info, sortSql)
    }

    // 仅在分页或limit字段有效时才构建
    if !(offset == 0 && limit == 0) {
        sql += fmt.Sprintf(" limit %d offset %d", limit, offset)
    }

    return sql, nil
}

func (m *MysqlDriver) sqlBuild(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string) (string, error) {

    // 根据db/sql类型分别组装sql
    var sql string
    var err error
    switch di.Type {
    case common.DatasetTypeDB:
        sql, err = m.sqlBuildDB(di, fields, offset, limit, sortNames, sortOpt)
    case common.DatasetTypeSQL:
        sql, err = m.sqlBuildSQL(di, fields, offset, limit, sortNames, sortOpt)
    default:
        return "", errors.New(fmt.Sprintf("dataset type [%s] not define", m.datasourceInfo.Type))
    }
    if err != nil {
        return "", err
    }

    return sql, nil
}

func (m *MysqlDriver) sqlExec(di *common.DatasetTable, fields []common.DatasetTableField, offset, limit int, sortNames []string, sortOpt string) ([]common.SqlRes, error) {
    sql, err := m.sqlBuild(di, fields, offset, limit, sortNames, sortOpt)
    if err != nil {
        return nil, err
    }

    // 执行sql
    var result []common.SqlRes
    dbErr := m.dbConn.Raw(sql).Scan(&result).Error
    if dbErr != nil {
        return nil, dbErr
    }

    return result, nil
}

// 遍历TableRow，根据维度信息以及列序号封装X结构
func (m *MysqlDriver) xAxis(sqlRes []common.SqlRes, fields []common.DatasetTableField) ([]string, FieldDefList) {
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

func (m *MysqlDriver) getDimensionFromSqlRes(row common.SqlRes, dimensionList FieldDefList) []string {
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
func (m *MysqlDriver) series(sqlRes []common.SqlRes, fields []common.DatasetTableField, dimensionList FieldDefList) []common.DsSeries {
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
        dimension := m.getDimensionFromSqlRes(sqlRes[index], dimensionList)
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

func (m *MysqlDriver) GetData(datasetId string, di *common.DatasetTable, fields []common.DatasetTableField,
    offset, limit int, sortNames []string, sortOpt string) (*common.DsResult, error) {
    sqlRes, err := m.sqlExec(di, fields, offset, limit, sortNames, sortOpt)
    if err != nil {
        return nil, err
    }

    var dsRes common.DsResult
    var dimensionList FieldDefList

    dsRes.X, dimensionList = m.xAxis(sqlRes, fields)
    dsRes.Fields = fields
    dsRes.TableRow = sqlRes

    dsRes.Series = m.series(sqlRes, fields, dimensionList)

    return &dsRes, nil
}

func (m *MysqlDriver) buildDBTypeSQL(table string) string {
    return fmt.Sprintf("SELECT * FROM %s LIMIT 1", table)
}

func (m *MysqlDriver) buildSqlTypeSQL(sql string) string {
    return  fmt.Sprintf("%s LIMIT 1", sql)
}

func getDatasetTypeMysql(baseType string) int64 {
    switch strings.ToUpper(baseType) {
    case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT":
        return common.DSTypeInt
    case "FLOAT", "DOUBLE", "REAL", "DECIMAL ":
        return common.DSTypeDEC
    case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
        return common.DSTypeTime
    case "BIT":
        return common.DSTypeBit
    default:
        return common.DSTypeVar
    }
}

func (m *MysqlDriver) getFieldsBySQL(sql string, datasetId string) ([]common.DatasetTableField, error) {
    var datasetFields []common.DatasetTableField

    db := m.dbConn

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
        dsType := getDatasetTypeMysql(baseType)

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

// 根据数据集信息获取所有field

func (m *MysqlDriver) GetDataFields(dsTable common.DatasetTable) ([]common.DatasetTableField, error) {
    var sql string

    switch dsTable.Type {
    case common.DatasetTypeDB:
        sql = m.buildDBTypeSQL(dsTable.Info)
    case common.DatasetTypeSQL:
        sql = m.buildSqlTypeSQL(dsTable.Info)
    default:
        return nil, errors.New(fmt.Sprintf("dataset type [%s] not support", dsTable.Type))

    }

    return m.getFieldsBySQL(sql, dsTable.DatasetId)

}

// 查看数据记录的连接状态

func (m *MysqlDriver) GetDBConnStatus() DBConnStatus {
    return m.datasourceInfo.Status
}

// GetDBConnStatus 获取连接状态，ConnSuccess: 连接可用，ConnFail：连接不可用

func (m *MysqlDriver) CheckDBConnStatus() DBConnStatus {
    if m.dbConn == nil {
        // 重新建立连接
        err := m.DBConn()
        if err != nil {
            return ConnFail
        }

        return ConnSuccess
    }

    sqlDB, _ := m.dbConn.DB()
    ctx, _ := context.WithTimeout(context.Background(), time.Duration(m.datasourceInfo.Config.ConnectTimeout) * time.Second)

    err := sqlDB.PingContext(ctx)
    if err != nil {
        m.datasourceInfo.Status = ConnFail
        return ConnFail
    }

    return ConnSuccess
}

// DBRecovery 重新建立连接
// 恢复连接的同时根据结果更新数据库中source表对应状态信息

func (m *MysqlDriver) DBRecovery() error {
    sqlDB, err := m.dbConn.DB()
    if err == nil {
        _ = sqlDB.Close()
    }
    m.dbConn = nil

    return m.DBConn()
}

// 删除连接

func (m *MysqlDriver) Close() error {
    sqlDB, err := m.dbConn.DB()
    if err != nil {
        _ = sqlDB.Close()
    }
    m.dbConn = nil

    return nil
}

func NewMysqlDriver(datasourceInfo common.DatasourceTable) (DBDriver, error) {
    source := &MysqlDriver{datasourceInfo: datasourceInfo}
    err := source.DBConn()
    if err != nil {
        return nil, err
    }

    return source, nil
}
package common

type SqlRes = map[string]interface{}

type TableTypesMap = map[string]string

type DimensionList struct {
    Name    string      `json:"name" form:"name"`
    Value   string      `json:"value" form:"value"`
}

type QuotaList struct {
    Name    string      `json:"name" form:"name"`
}

type DsData struct {
    Value interface{} `json:"value" form:"value"`
    Name  []string    `json:"name" form:"name"`
}

type DsSeries struct {
    Name        string              `json:"name" form:"name"`
    Data        []DsData            `json:"data" form:"data"`
}

type DsResult struct {
    X           []string                    `json:"x" form:"x"`
    Fields      []DatasetTableField         `json:"fields" form:"fields"`
    TableRow    []SqlRes                    `json:"tableRow" form:"tableRow"`
    Series      []DsSeries                  `json:"series" form:"series"`
}


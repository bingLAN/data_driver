package data_driver

import (
    "fmt"
    "reflect"
    "testing"
)

func MakeStruct(args ...interface{}) reflect.Value {
    var sfs []reflect.StructField
    
    for k, v := range args {
        typ := reflect.TypeOf(v)
        structField := reflect.StructField{
            Name: fmt.Sprintf("F%d", k + 1),
            Type: typ,
        }
        sfs = append(sfs, structField)
    }
    st := reflect.StructOf(sfs)
    for i := 0; i < st.NumField(); i++ {
        fmt.Println(st.Field(i).Name)
    }
    so := reflect.New(st)   //生成指定类型的反射指针对象
    return so
}

func TestMakeStruct(t *testing.T) {
    sr := MakeStruct(0, "你好", []int{1, 2, 3})
    sr.Elem().Field(0).SetInt(100)
    sr.Elem().Field(1).SetString("hello")
    sr.Elem().Field(2).Set(reflect.ValueOf([]int{5, 6, 7}))
    
    
    for i := 0; i < sr.Elem().NumField(); i++ {
        fmt.Println(sr.Elem().Field(i))
    }
}


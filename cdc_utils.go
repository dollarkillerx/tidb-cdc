package tidb_cdc

import (
	"errors"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/dollarkillerx/easy_reflect"
	"gorm.io/gorm/schema"
)

type CDCSchema struct {
	Database string                 `json:"database"` // 数据库
	Table    string                 `json:"table"`    // 表
	Type     CDCType                `json:"type"`     // delete, insert, update
	Ts       int                    `json:"ts"`       // 时间
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old"`
}

type CDCType string

const (
	DELETE CDCType = "delete"
	INSERT CDCType = "insert"
	UPDATE CDCType = "update"
)

func MaxwellUnmarshal(kMap map[string]interface{}, r interface{}) error {
	ref := easy_reflect.NewReflect(r)
	if ref.Kind() != reflect.Ptr {
		return errors.New("必须是指针结构体")
	}
	if ref.Elem().Kind() != reflect.Struct {
		return errors.New("必须是指针结构体")
	}

	elem := ref.Elem()
	fields := GetModelFields(elem.GetValue())
	for _, field := range fields {
		unmarshal(kMap, field, elem)
	}

	return nil
}

func unmarshal(kMap map[string]interface{}, field reflect.StructField, elem *easy_reflect.ReflectItem) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Recover: ", err)
		}
	}()

	gTag := field.Tag.Get("gorm")
	// 结构体中field名称
	fieldName := field.Name
	column := GetColumnNameFromTag(gTag)
	if column == "" {
		column = schema.NamingStrategy{}.ColumnName("", fieldName)
	}

	value := kMap[column]
	if value == nil {
		return
	}

	if field.Type == reflect.TypeOf(time.Time{}) {
		_, ok := value.(float64)
		if !ok {
			t, err := formatTime(value.(string))
			if err == nil {
				elem.GetValue().FieldByName(fieldName).Set(reflect.ValueOf(*t))
			}
		} else {
			elem.GetValue().FieldByName(fieldName).Set(reflect.ValueOf(time.Unix(int64(value.(float64))/1000, 0)))
		}
		return
	}
	// 指针类型的  updated_at, deleted_at
	if field.Type == reflect.TypeOf(&time.Time{}) {
		elem.GetValue().FieldByName(fieldName).Set(reflect.New(elem.GetValue().FieldByName(fieldName).Type().Elem()))
		_, ok := value.(float64)
		if !ok {
			t, err := formatTime(value.(string))
			if err == nil {
				elem.GetValue().FieldByName(fieldName).Elem().Set(reflect.ValueOf(*t))
			}
		} else {
			elem.GetValue().FieldByName(fieldName).Elem().Set(reflect.ValueOf(time.Unix(int64(value.(float64))/1000, 0)))
		}
		return
	}
	// 根据字段的类型，进行赋值
	switch field.Type.Kind() {
	// 这里如何判断 bool 为 true,false;需要根据具体的情况进行判断
	case reflect.Bool:
		if value.(float64) == 1 {
			elem.GetValue().FieldByName(fieldName).SetBool(true)
		}
	case reflect.String:
		elem.GetValue().FieldByName(fieldName).SetString(value.(string))
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		elem.GetValue().FieldByName(fieldName).SetInt(int64(value.(float64)))
	case reflect.Float32, reflect.Float64:
		elem.GetValue().FieldByName(fieldName).SetFloat(value.(float64))
		// 指针类型
	case reflect.Ptr:
		elem.GetValue().FieldByName(fieldName).Set(reflect.New(elem.GetValue().FieldByName(fieldName).Type().Elem()))
		switch field.Type.Elem().Kind() {
		case reflect.Bool:
			if value.(float64) == 1 {
				elem.GetValue().FieldByName(fieldName).Elem().SetBool(true)
			}
		case reflect.String:
			elem.GetValue().FieldByName(fieldName).Elem().SetString(value.(string))
		case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
			elem.GetValue().FieldByName(fieldName).Elem().SetInt(int64(value.(float64)))
		case reflect.Float32, reflect.Float64:
			elem.GetValue().FieldByName(fieldName).Elem().SetFloat(value.(float64))
		}
	}
}

func GetModelFields(v reflect.Value) []reflect.StructField {
	sf := make([]reflect.StructField, 0)
	if v.Type().Kind() != reflect.Struct {
		panic("CDC Connector Deserialization Please pass in the structure ！！！！")
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		if strings.TrimSpace(field.Tag.Get("gorm")) == "-" {
			continue
		}
		if field.Type.Kind() == reflect.Struct && IsInnerStruct(field) {
			innerSF := GetModelFields(v.Field(i))
			sf = append(sf, innerSF...)
		} else {
			sf = append(sf, field)
		}
	}
	return sf
}

func IsInnerStruct(structField reflect.StructField) bool {
	if structField.Type == reflect.TypeOf(time.Time{}) || structField.Type == reflect.TypeOf(&time.Time{}) {
		return false
	}
	if strings.Contains(structField.Tag.Get("gorm"), "foreignKey") {
		return false
	}

	return true
}

package tidb_cdc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestUnmarshal(t *testing.T) {
	type DeletedAt sql.NullTime

	type Model struct {
		ID        string     `gorm:"primarykey;type:varchar(250)" json:"id"`
		CreatedAt time.Time  `gorm:"index;type:TIMESTAMP" json:"created_at"`
		UpdatedAt *time.Time `gorm:"index" json:"updated_at"`
		DeletedAt DeletedAt  `gorm:"index" json:"deleted_at"`
	}

	type TsModel struct {
		Model
		Name       string `gorm:"column:entity_name;type:varchar(250)"`
		EntityType int
		AgeTT      string `gorm:"-"`
	}

	var r TsModel
	fj, err := ioutil.ReadFile("test/fff.json")
	if err != nil {
		panic(err)
	}

	//fmt.Println(fj)
	var rm CDCSchema
	err = json.Unmarshal(fj, &rm)
	if err != nil {
		panic(err)
	}

	r = r
	fmt.Println(rm)

	err = MaxwellUnmarshal(rm.Data, &r)
	if err != nil {
		panic(err)
	}

	cp(r)
}

func cp(r interface{}) {
	marshal, err := json.Marshal(r)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(string(marshal))
}

func TestMb(t *testing.T) {
	type DeletedAt sql.NullTime

	type Model struct {
		ID        string     `gorm:"primarykey;type:varchar(250)" json:"id"`
		CreatedAt time.Time  `gorm:"index;type:TIMESTAMP" json:"created_at"`
		UpdatedAt *time.Time `gorm:"index" json:"updated_at"`
		DeletedAt DeletedAt  `gorm:"index" json:"deleted_at"` // 这样反射 无法 of.FieldByName("CreatedAt")找到
	}

	type TsModel struct {
		Model             // 反射 可以  of.FieldByName("CreatedAt") 找到
		Name       string `gorm:"column:entity_name;type:varchar(250)"`
		EntityType int
		AgeTT      string `gorm:"-"`
	}

	p := TsModel{}
	of := reflect.TypeOf(p)

	for i := 0; i < of.NumField(); i++ {
		fmt.Println(of.Field(i).Name)
	}

	//name, ok := of.FieldByName("CreatedAt")
	//if ok {
	//	fmt.Println(name.Type.String())
	//}
}

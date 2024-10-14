package ih08_proc

import (
	"os"
	"fmt"
	"io/ioutil"
	"reflect"
	// "runtime"
	"regexp"
	"strings"
	"log"
	"strconv"
	"encoding/csv"
	"encoding/json"
	"database/sql"
	"taxo2/schema"
	"taxo2/dbops"
	"golang.org/x/text/encoding/charmap"
)

const mercosur_pattern = `([a-zA-Z]{2})[^a-zA-Z0-9]?(\d{3})[^a-zA-Z0-9]?([a-zA-Z]{2})`
const original_pattern = `([a-zA-Z]{3})[^a-zA-Z0-9]?(\d{3})`

const TIPO_2_KEY = "Tipo 2"
const TIPO_25_KEY = "Tipo 2.5"
const STATUS_KEY = "Status usuario"
const DENOM_KEY = "Denominación Estado"
const MATRICULA_KEY = "Matrícul.vehíc."

var (
	TIPO_2_FILTERS []*regexp.Regexp
	TIPO_25_FILTERS []*regexp.Regexp
	STATUS_USUARIO_FILTERS []*regexp.Regexp
	DENOMINACION_1_FILTERS []*regexp.Regexp
	MERCOSUR_PATTERN *regexp.Regexp
	ORIGINAL_PATTERN *regexp.Regexp
)

func toFilter(vals []string) (out []*regexp.Regexp) {
	for _, v := range vals {
		out = append(out, regexp.MustCompile(v))
	}
	return
}

func init() {
	tipo_2_filters := []string{"LIV", "PES"}
	tipo_25_filters := []string{"AUT", "CAM", "MIN", "PKP", "APM"}
	status_usuario_filters := []string{"EBAJ", "EVEN"} 
	denominacion_1_filters := []string{"REMAT"}

	TIPO_2_FILTERS = toFilter(tipo_2_filters)
	TIPO_25_FILTERS = toFilter(tipo_25_filters)
	STATUS_USUARIO_FILTERS = toFilter(status_usuario_filters)
	DENOMINACION_1_FILTERS = toFilter(denominacion_1_filters)

	MERCOSUR_PATTERN = regexp.MustCompile(mercosur_pattern)
	ORIGINAL_PATTERN = regexp.MustCompile(original_pattern)
}


func NewNullString(s string) sql.NullString {

	if s == "" {
		return sql.NullString{
				String: "",
				Valid:  false, // Set to false if the string is null
		}
	}
	return sql.NullString{
			String: s,
			Valid:  true,
	}
}

func NewNullInt64(s string) sql.NullInt64 {
	if s=="" {
		return sql.NullInt64{
			Int64: 0,
			Valid: false,
		}
	}
	if val, err := strconv.ParseInt(s,0,64); err!=nil {
		return sql.NullInt64{
			Int64: 0,
			Valid: false,
		}
	} else {
		return sql.NullInt64{
			Int64: int64(val),
			Valid: true,
		}
	}
}

func ReadTranslation(filename string) (map[string]string, error) {

	f, err := os.Open(filename)
	if err != nil {
			log.Fatal(err)
	}
	defer f.Close()

	// Create a new CSV reader

	byteData, err := ioutil.ReadAll(f)
	if err != nil {
			log.Fatal(err)
	}

	var translation map[string]string
	err = json.Unmarshal(byteData, &translation)

	return translation, err
}

func Parse(csvFilename string, translationFilename string) (schema.IH08, error) {

	translation, err := ReadTranslation(translationFilename)

	f, err := os.Open(csvFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Create a new CSV reader
	reader := charmap.ISO8859_1.NewDecoder().Reader(f)
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'
	records, err := csvReader.ReadAll()
	if err != nil {
			log.Fatal("Parsing error ", err)
	}

	var eqs []schema.IH08Post

	header := records[0]

	for i, r := range records {
		if i == 0 {
			continue
		}
		var eq schema.IH08Post
		val := reflect.ValueOf(&eq).Elem()
		
		for j, value := range r {
			key := translation[header[j]]
			field := val.FieldByName(key)

			// log.Printf("FIELD %s -- idx %d -- key %s -- header %s", field, j, key, header[j])
			// log.Println(header)
			if field.IsValid() && field.CanSet() {
				if field.Kind() == reflect.Int64 {
					val, err := strconv.Atoi(value)
					if err != nil {
						log.Println("Couldn't Cast => ", val, value, field)
						break
					}
					field.Set(reflect.ValueOf(int64(val)))

				} else if field.Type() == reflect.TypeOf(sql.NullString{}){
					val := NewNullString(value)
					field.Set(reflect.ValueOf(val))
				} else if field.Type() == reflect.TypeOf(sql.NullInt64{}) {
					val := NewNullInt64(value)
					field.Set(reflect.ValueOf(val))
				} else {
					field.Set(reflect.ValueOf(value))
				}
			}
		}
		eqs = append(eqs, eq)

	}
	return eqs, nil
}

func includeValues(f string, needles []*regexp.Regexp) (out bool) {
	for _, n := range needles {
		ms := n.FindAllString(f, 1)
		out = out || len(ms) > 0
	}
	return
}

func includePost(eq *schema.IH08Post) bool {

	tipo_2 := includeValues(eq.Tipo2, TIPO_2_FILTERS)
	tipo_25 := includeValues(eq.Tipo25, TIPO_25_FILTERS)
	status := !includeValues(eq.StatusUsuario, STATUS_USUARIO_FILTERS)
	denom := !includeValues(eq.DenominacionEstado, DENOMINACION_1_FILTERS)

	return tipo_2 && tipo_25 && status && denom
}

func Filter(eqs schema.IH08, ) (out schema.IH08) {
	for _, eq := range eqs {
		if includePost(&eq) {
			out = append(out, eq)
		}
	}
	return
}

func adjustPlate(p string) (parsedPlate sql.NullString, err error) {

	if strings.Contains(p, "INTERNO") {
		err = fmt.Errorf("INVALID PLATE, INTERNO")
		return
	} else if strings.Contains(p, "INTTERNO") {
		err = fmt.Errorf("INVALID PLATE, INTTERNO")
		return
	} else if strings.Contains(p, "BIS") {
		p = strings.Replace(p, "BIS", "", -1)
	}
	
	if ms := MERCOSUR_PATTERN.FindAllStringSubmatch(p, -1); ms!=nil {
		parsedPlate = NewNullString(strings.Join(ms[0][1:], ""))
	} else if os := ORIGINAL_PATTERN.FindAllStringSubmatch(p, -1); os!=nil{
		parsedPlate = NewNullString(strings.Join(os[0][1:], ""))
	} else {
		// log.Printf("INPUT => %s -- MERC %#v OP %#v", p, ms, os)
		err = fmt.Errorf("INVALID PLATE, UNKNOWN")
	}

	return
}


func AdjustPlates(eqs schema.IH08) (out schema.IH08) {
	for _, eq := range eqs {
		if parsed_plate, err := adjustPlate(eq.MatriculVehic); err!= nil {
			// log.Printf("Couldn't parse the plate! => %s ERR ", eq.MatriculVehic)
			// log.Println(err)
		} else {
			eq.UpdatePlate(parsed_plate)
		}
		
		out = append(out, eq)
	}
	return
}

func RemoveErrors(eqs schema.IH08) (out schema.IH08) {
	for _, eq := range eqs {
		// log.Printf("PLATE =>> %#v", eq.ParsedPlate)
		if eq.ParsedPlate.Valid {
			out = append(out, eq)
		}
	}
	return
}

// TODO Type constraint:
// type Useful interface {
// 	schema.IH08
// }

func RemoveDups(eqs schema.IH08) (out schema.IH08, dup_count int) {
	ps := make(map[string]bool, len(eqs))
	for _, eq := range eqs {
		if _, ok := ps[eq.ParsedPlate.String]; !ok {
			ps[eq.ParsedPlate.String] = true
			out = append(out, eq)
		} else {
			dup_count++
		}
	}
	return
}

func ToFixed(eqs schema.IH08, db dbops.DB) (out schema.IH08F) {
	for _, eq := range eqs {
		if eqf, err := schema.NewIH08FPost(eq, &db); err!=nil {
			log.Println("ERROR WHILE BUILDING IH08F -> ", eq)
			continue
		} else {
			out = append(out, eqf)
		}

	}
	return
}

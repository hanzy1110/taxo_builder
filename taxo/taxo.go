package taxo

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"taxo2/dbops"
	"taxo2/schema"
)

var (
	t2Translations map[string]string
	t3Translations map[string]string
)

func init() {
	t2Translations = map[string]string{
		"LIV": "Liviano",
		"PES": "Pesado",
		"AUX": "Auxiliares",
		"APE": "AmarilloPesado",
		"ELE": "Elevacion",
		"TEL": "TendidoElectrico",
	}
	t3Translations = map[string]string{
		"LIV PKP": "PKP",
		"LIV MIN": "MIN",
		"LIV AUT": "AUT",
		"PES CAM": "CAM",
		"PES ACS": "ACS",
		"ELE GRP": "GruaPluma",
		"ELE PAR": "Plataforma",
		"ELE AEV": "Autoelevador",
		// Uno hay que borrar!!
		"ELE HDG": "HidroGrua",
		"ELE HDR": "HidroGrua",
		"APE EMS": "Viales",
		"APE TTP": "Piping",
		"APE TSN": "EspecialesPesado",
		"TEL ETC": "TendidoElectricoCabrestante",
		"AUX TDA": "AUXTDuctos",
		"AUX AXL": "AuxiliaresMisc",
		"AUX TRC": "ContObrador",
	}
}

func get_t1(ih08p schema.IH08FPost, t0 any) (out ClasesEquipo, err error) {

	if ih08p.Tipo2 != "" {
		out = ClasesEquipo{Nombre: ih08p.Tipo2}
		return
	} else {
		err = fmt.Errorf("Sin tipo 2 %v", ih08p)
		return
	}
}

func get_t2(ih08p schema.IH08FPost, t1 ClasesEquipo) (t T2Builder, err error) {

	switch t1.Nombre {
	case "LIV":
		t = NewLiviano(ih08p.Tipo25)
	case "PES":
		t = NewPesado(ih08p.Tipo25)
	case "APE":
		t = NewAmarilloPesado(ih08p.Tipo25)
	case "ELE":
		// Aca esta el error, estoy desambiguando!!
		switch ih08p.Tipo25 {
		case "PAR":
			if strings.Contains(ih08p.DenominacionEquipo, "PLATAF") {
				t = NewElevacion("PAR")
			} else {
				t = NewElevacion("AEV")
			}
		case "EIZ":
			if strings.Contains(ih08p.DenominacionEquipo, "HIDRO") {
				t = NewElevacion("HDG")	
			} else {
				t = NewElevacion("GRP")	
			}
		default:
			t = NewElevacion(ih08p.Tipo25)
		} 

	case "TEL":
		t = NewTendidoElectrico(ih08p.Tipo25)
	case "AUX":
		t = NewAuxiliares(ih08p.Tipo25)
	default:
		err = fmt.Errorf("INVALID TYPE!!")
	}
	return
}

func get_t3(ih08p schema.IH08FPost, t2 T2Builder, db dbops.DB) (T2Builder, T3Builder, error) {

	v := t2.GetClaseV()
	// log.Println(v)
	// log.Println(ih08p)

	switch v {

	case "LIV PKP":
		pkp := &PKP{}
		pkp.BuildT3(ih08p, db)
		return nil, pkp, nil

	case "LIV MIN":
		min := &MIN{}
		min.BuildT3(ih08p, db)
		return nil, min, nil

	case "LIV AUT":
		aut := &AUT{}
		aut.BuildT3(ih08p, db)
		return nil, aut, nil

	case "PES CAM":
		cam := &CAM{}
		cam.BuildT3(ih08p, db)
		return nil, cam, nil

	case "PES ACS":
		acs := &Acoplado{}
		acs.BuildT3(ih08p, db)
		return nil, acs, nil

	case "ELE EIZ":
		// Si desambiguo arriba cuando creo dps aca es innecesario...
		if strings.Contains(ih08p.DenominacionEquipo, "HIDRO") {
			eq := &HidroGrua{}
			eq.BuildT3(ih08p, db)
			ele := &Elevacion{ClasePeso: "ELE", ClaseV: "HDG"}
			return ele, eq, nil

		} else {
			eq := &GruaPluma{}
			eq.BuildT3(ih08p, db)
			ele := &Elevacion{ClasePeso: "ELE", ClaseV: "GRP"}
			return ele, eq, nil
		}

	case "ELE PAR":
		if strings.Contains(ih08p.DenominacionEquipo, "PLATAF") {
			eq := &Plataforma{}
			eq.BuildT3(ih08p, db)
			ele := &Elevacion{ClasePeso: "ELE", ClaseV: "PAR"}
			return ele, eq, nil
		} else {
			eq := &Autoelevador{}
			eq.BuildT3(ih08p, db)
			ele := &Elevacion{ClasePeso: "ELE", ClaseV: "AEV"}
			return ele, eq, nil
		}
	
	case "ELE AEV":
		eq := &Autoelevador{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "ELE HDG":
		eq := &HidroGrua{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "ELE GRP":
		eq := &GruaPluma{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "APE EMS":
		eq := &Viales{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "APE TSN":
		eq := &EspecialesPesado{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "APE TTP":
		eq := &Piping{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "TEL ETC":
		eq := &TendidoElectricoCabrestante{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "TEL AXL":
		eq := &AuxTendido{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "AUX TRC":
		eq := &ContObrador{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "AUX TDA":
		eq := &AUXTDuctos{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil

	case "AUX AXL":
		eq := &AuxiliaresMisc{}
		eq.BuildT3(ih08p, db)
		return nil, eq, nil
	// TENGO QUE DESAMBIGUAR LOS TIPOS 2/ 2.5 DE SAP PARA PODER MAPPEAR BIEN LOS TIPOS DE
	// LA TAXO!

	default:
		// Devuelvo nil pointer!
		// TODO Aqui incorporar lo no tipificado o los casos que sobran!

		if strings.Contains(ih08p.DenominacionEquipo, "HIDRO") {
			eq := &HidroGrua{}
			eq.BuildT3(ih08p, db)
			ele := &Elevacion{ClasePeso: "ELE", ClaseV: "HDG"}
			return ele, eq, nil
		}
		return nil, nil, fmt.Errorf("INVALID T3 TYPE! %v - %v", ih08p, t2)
	}

}

func get_unique[T comparable](ts []T) (ul []T) {
	um := make(map[any]bool)

	for _, t := range ts {

		var key any
		val := reflect.ValueOf(t)
		if val.Kind() == reflect.Ptr {
			key = val.Elem().Interface()
		} else {
			key = t
		}

		if _, exists := um[key]; !exists {
			um[key] = true
			ul = append(ul, t)
		}
	}
	return
}

func LoadTaxo(ih08 schema.IH08F, db dbops.DB) (err error) {

	var t1s []ClasesEquipo
	var t2s []T2Builder
	var t3s []T3Builder

	for _, ih08p := range ih08 {

		t1, err := get_t1(ih08p, nil)
		if err != nil {
			continue
		}
		t1s = append(t1s, t1)

		t2, err := get_t2(ih08p, t1s[len(t1s)-1])
		if err != nil {
			continue
		}

		t2s = append(t2s, t2)

		t2ptr, t3, err := get_t3(ih08p, t2s[len(t2s)-1], db)
		if err != nil {
			continue
		} else if t2ptr != nil {

			switch v := t2ptr.(type) {
			case *Elevacion:
				t2s = append(t2s, *v)
			default:
				log.Fatal("INVALID DISAMBIGUATION ", v)
			}

		}
		t3s = append(t3s, t3)
	}

	t1s = get_unique[ClasesEquipo](t1s)
	t2s = get_unique[T2Builder](t2s)
	t3s = get_unique[T3Builder](t3s)

	for _, t := range t1s {
		if err := t.Insert(db); err != nil {
			log.Fatal("While INSERTING T1 => ", err)
		}
	}
	for _, t := range t2s {
		if err := t.Insert(db); err != nil {
			log.Fatal("While INSERTING T2 => ", err)
		}
	}
	for _, t := range t3s {
		if err := t.Insert(db); err != nil {
			log.Fatal("While INSERTING T3 => ", err)
		}
	}

	return
}

func IntoTaxo(ih08 schema.IH08F, db dbops.DB) (err error) {
	for _, ih08p := range ih08 {
		t1, err := get_t1(ih08p, nil)
		if err != nil {
			// log.Printf("INVALID T1 %v -- %v", ih08p, err)
			continue
		} 
		t1_id := t1.Find(db)
		t2, err := get_t2(ih08p, t1)
		if err != nil {
			// log.Printf("INVALID T2 %v -- %v", ih08p, err)
			continue
		}
		t2_id := t2.Find(db)
		_, t3, err := get_t3(ih08p, t2, db)
		if err != nil {
			// log.Printf("INVALID T3 %v -- %v", ih08p, err)
			continue
		} 
		t3_id := t3.Find(db)

		if !(t1_id.Valid && t2_id.Valid && t3_id.Valid) {
			log.Println("INVALID IDS => ", t1_id, t2_id, t3_id)
			continue
		}

		aux := Taxonomia{
			Equipo:       ih08p.Equipo,
			T1:           t1_id,
			T2:           t2_id,
			T3:           t3_id,
			Denominacion: sql.NullString{String: ih08p.DenominacionEquipo, Valid: true},
		}
		if err = aux.Insert(db); err != nil {
			log.Println("WHILE INSERTING TAXO =>> ", err)
		}
	}
	return
}

func retrieve_t1(t1_id sql.NullInt64, db dbops.DB) (c ClasesEquipo) {
	db.DB.Get(&c, "SELECT * FROM ClasesEquipo WHERE id=?", t1_id)
	return
}

func retrieve_t2(t2_id sql.NullInt64, t2_name string, db dbops.DB) (T2Builder, error) {

	t2Trans, exists := t2Translations[t2_name]
	if !exists {
		return Elevacion{}, fmt.Errorf("INVALID T2 name =>> ", t2_name, " -- TRANSLATED => ", t2Trans)
	}
	q := fmt.Sprintf("SELECT * FROM %s WHERE id=?", t2Trans)
	// log.Println("T2 name =>> ", t2_name, " -- TRANSLATED => ", t2Trans)
	// log.Println("QUERY ==> ", q)

	switch t2Trans {
	case "Liviano":
		eq := Liviano{}
		err := db.DB.Get(&eq, q, t2_id)
		return eq, err
	case "Pesado":
		eq := Pesado{}
		err := db.DB.Get(&eq, q, t2_id)
		return eq, err
	case "Auxiliares":
		eq := Auxiliares{}
		err := db.DB.Get(&eq, q, t2_id)
		return eq, err
	case "TendidoElectrico":
		eq := TendidoElectrico{}
		err := db.DB.Get(&eq, q, t2_id)
		return eq, err
	case "AmarilloPesado":
		eq := AmarilloPesado{}
		err := db.DB.Get(&eq, q, t2_id)
		return eq, err
	case "Elevacion":
		eq := Elevacion{}
		err := db.DB.Get(&eq, q, t2_id)
		return eq, err
	default:
		err := fmt.Errorf("INVALID T2 to RETRIEVE")
		return Elevacion{}, err
	}
}

func retrieve_t3(t3_id sql.NullInt64, t3_name string, db dbops.DB) (T3Builder, error) {

	t3Trans, exists := t3Translations[t3_name]
	if !exists {
		return nil, fmt.Errorf("INVALID T3 name =>> ", t3_name, " -- TRANSLATED => ", t3Trans)
	}

	q := fmt.Sprintf("SELECT * FROM %s WHERE id=?", t3Trans)

	// log.Println("T3 name =>> ", t3_name, " -- TRANSLATED => ", t3Trans)
	// log.Println("QUERY ==> ", q)

	switch t3Trans {
	case "PKP":
		eq := &PKP{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "MIN":
		eq := &MIN{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "AUT":
		eq := &AUT{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "CAM":
		eq := &CAM{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "ACS":
		eq := &Acoplado{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "HidroGrua":
		eq := &HidroGrua{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "GruaPluma":
		eq := &GruaPluma{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "Autoelevador":
		log.Println("GOT AUTOELEVADOR")
		eq := &Autoelevador{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "Plataforma":
		eq := &Plataforma{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "Viales":
		eq := &Viales{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "Piping":
		eq := &Piping{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "EspecialesPesado":
		eq := &EspecialesPesado{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "TendidoElectricoCabrestante":
		eq := &TendidoElectricoCabrestante{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "ContObrador":
		eq := &ContObrador{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "AuxiliaresMisc":
		eq := &AuxiliaresMisc{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "AUXTDuctos":
		eq := &AUXTDuctos{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "AuxTendido":
		eq := &AuxTendido{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	default:
		err := fmt.Errorf("INVALID T3 to RETRIEVE =>> ", t3_name, " -- TRANSLATED=>", t3Trans)
		return nil, err
	}
}

func writeCSV(reports []TaxoReport, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';'

	defer writer.Flush()

	// Write CSV header
	header := []string{"T2", "T2.5", "T3", "TipoBien", "Equipo"}
	writer.Write(header)

	// Write data
	for _, report := range reports {
		record := []string{
			report.T2,
			report.T25,
			report.T3,
			report.TipoBien,
			strconv.Itoa(int(report.Equipo)),
		}
		writer.Write(record)
	}
	return nil
}

func IntoReport(repPath string, db dbops.DB) (err error) {
	var taxo []Taxonomia
	var rep []TaxoReport
	var missT2 []string
	var missT3 []string

	err = db.DB.Select(&taxo, "SELECT * FROM Taxonomia")
	if err != nil {
		log.Fatal("While Loading from DB", err)
	}
	for _, t := range taxo {

		t1 := retrieve_t1(t.T1, db)
		t2, err := retrieve_t2(t.T2, t1.Nombre, db)

		if err != nil {
			missT2 = append(missT2, t1.Nombre)
			continue
		}

		t3, err := retrieve_t3(t.T3, t2.GetClaseV(), db)

		if err != nil {
			missT3 = append(missT3, t2.GetClaseV())
			log.Println("ERR => ", err, "T2 => ", t2.GetClaseV())
			continue
		}

		r := TaxoReport{
			Equipo:   t.Equipo,
			T2:       t1.Nombre,
			T25:      processClaseV(t2.GetClaseV()),
			T3:       t3.GetFuncion(),
			TipoBien: t3.GetTipoBien(),
		}
		rep = append(rep, r)
	}

	for _, m := range get_unique(missT2) {
		log.Println("MISSING T2 =>> ", m)
	}

	for _, m := range get_unique(missT3) {
		log.Println("MISSING T3 =>> ", m)
	}

	if err := writeCSV(rep, repPath); err != nil {
		log.Fatal("WHILE WRITING REPORT ->> ", err)
	}
	return nil
}

func processClaseV(cls string) string {
	words := strings.Split(cls, " ")
	return words[len(words)-1]
}

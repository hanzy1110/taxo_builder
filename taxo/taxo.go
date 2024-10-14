package taxo

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"golang.org/x/text/encoding/charmap"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"taxo2/dbops"
	"taxo2/schema"
)

var (
	t1New          map[string]bool
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
		"ADV": "AditamentoVehiculo",
	}
	t3Translations = map[string]string{
		"LIV PKP": "PKP",
		"LIV MIN": "MIN",
		"LIV FUR": "FUR",
		"LIV AUT": "AUT",
		"PES CAM": "CAM",
		"PES ACS": "ACS",
		"ELE GRP": "GruaPluma",
		"ELE PAR": "Plataforma",
		"ELE AEV": "Autoelevador",
		// Uno hay que borrar!!
		"ADV HDG": "HidroGrua",
		"ADV HDR": "HidroGrua",

		"ADV TQE": "Tanque",
		"ADV PLA": "PlataformaVehiculo",

		"ADV ADM": "AditamentoMinicargadora",
		"ADV ADE": "AditamentoExcavadora",
		"ADV ADT": "AditamentoTiendetubo",
		"ADV ADC": "AditamentoCamion",

		"APE EMS": "Viales",
		"APE TTP": "Piping",
		"APE TSN": "EspecialesPesado",
		"APE EOG": "EqObrGral",
		"TEL ETC": "TendidoElectricoCabrestante",
		"AUX TDA": "AUXTDuctos",
		"AUX AXL": "AuxiliaresMisc",
		"AUX TRC": "ContObrador",
	}

}

func get_t1(ih08p schema.IH08FPost, t0 any) (ClasesEquipo, error) {
	// No tener tipo 2 no es mas un error!
	// No poder clasificar el equipo si lo es

	out := getClaseEq(ih08p)
	if out.Nombre == "" {
		return out, fmt.Errorf("Sin tipo 2 %v", ih08p)
	}
	return out, nil
}

func getClaseEq(ih08p schema.IH08FPost) (out ClasesEquipo) {

	switch {
	case strings.Contains(ih08p.DenominacionEquipo, "MARTILLO"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "BALDE CRI"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "BRAZO"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "PALLET"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "ROLO"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "HIDROGR"):
		if !strings.Contains(ih08p.DenominacionEquipo, "ELEVADOR") {
			out = ClasesEquipo{Nombre: "ADV"}
		}

	case strings.Contains(ih08p.DenominacionEquipo, "MIDWESTERN"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "TANQUE"):
		switch {
		case strings.Contains(ih08p.DenominacionEquipo, "ACOPLADO"):
			out = ClasesEquipo{Nombre: "PES"}
		case strings.Contains(ih08p.DenominacionEquipo, "TANQUES ACEITE"):
			out = ClasesEquipo{Nombre: ih08p.Tipo2}
		default:
			out = ClasesEquipo{Nombre: "ADV"}
		}
	case strings.Contains(ih08p.DenominacionEquipo, "VUELCO TRASERO"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "HIDROGRUA"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "ENGRASE"):
		out = ClasesEquipo{Nombre: "ADV"}

	case strings.Contains(ih08p.DenominacionEquipo, "VOLCADORA"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "COMPACTADOR"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "COMPACTADOR DE CARGA"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "ENGRASE"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "PRESENTADOR DE CAÑOS"):
		out = ClasesEquipo{Nombre: "ADV"}
	case strings.Contains(ih08p.DenominacionEquipo, "HOYADORA"):
		out = ClasesEquipo{Nombre: "ADV"}

	case strings.Contains(ih08p.DenominacionEquipo, "EXCAVADORA"):
		out = ClasesEquipo{Nombre: "APE"}
	case strings.Contains(ih08p.DenominacionEquipo, "RETROEX"):
		out = ClasesEquipo{Nombre: "APE"}
	case strings.Contains(ih08p.DenominacionEquipo, "TIENDETUBOS"):
		out = ClasesEquipo{Nombre: "APE"}
	case strings.Contains(ih08p.DenominacionEquipo, "MINICARGADORA"):
		out = ClasesEquipo{Nombre: "APE"}
	case strings.Contains(ih08p.DenominacionEquipo, "MINZANJADORA"):
		out = ClasesEquipo{Nombre: "APE"}

	case strings.Contains(ih08p.DenominacionEquipo, "CAMION"):
		out = ClasesEquipo{Nombre: "PES"}
	case strings.Contains(ih08p.DenominacionEquipo, "ACOPLADO"):
		out = ClasesEquipo{Nombre: "PES"}
	case strings.Contains(ih08p.DenominacionEquipo, "SEMIRREMOLQUE"):
		out = ClasesEquipo{Nombre: "PES"}

	case strings.Contains(ih08p.DenominacionEquipo, "PUENTE"):
		out = ClasesEquipo{Nombre: "ELE"}
	case strings.Contains(ih08p.DenominacionEquipo, "ELEVADOR"):
		out = ClasesEquipo{Nombre: "ELE"}

	case strings.Contains(ih08p.DenominacionEquipo, "FURG"):
		out = ClasesEquipo{Nombre: "LIV"}
	case strings.Contains(ih08p.DenominacionEquipo, "AUTOMOVIL"):
		out = ClasesEquipo{Nombre: "LIV"}
	case strings.Contains(ih08p.DenominacionEquipo, "AUTOMÓVIL"):
		out = ClasesEquipo{Nombre: "LIV"}
	case strings.Contains(ih08p.DenominacionEquipo, "MINIBUS"):
		out = ClasesEquipo{Nombre: "LIV"}
	case strings.Contains(ih08p.DenominacionEquipo, "PICK UP"):
		out = ClasesEquipo{Nombre: "LIV"}
	case strings.Contains(ih08p.DenominacionEquipo, "PICK-UP"):
		out = ClasesEquipo{Nombre: "LIV"}

	default:
		out = ClasesEquipo{Nombre: ih08p.Tipo2}
	}
	return
}

func get_t2(ih08p schema.IH08FPost, t1 ClasesEquipo) (t T2Builder, err error) {

	switch t1.Nombre {
	case "LIV":
		switch ih08p.Tipo25 {
		case "MIN":
			if strings.Contains(ih08p.DenominacionEquipo, "FURGON") {
				t = NewLiviano("FUR")
			} else if strings.Contains(ih08p.DenominacionEquipo, "FURGÓN") {
				t = NewLiviano("FUR")
			} else {
				t = NewLiviano("MIN")
			}
		default:
			switch {
			case strings.Contains(ih08p.DenominacionEquipo, "PICK-UP"):
				t = NewLiviano("PKP")
			case strings.Contains(ih08p.DenominacionEquipo, "PICK UP"):
				t = NewLiviano("PKP")
			case strings.Contains(ih08p.DenominacionEquipo, "AUTOMOVIL"):
				t = NewLiviano("AUT")
			case strings.Contains(ih08p.DenominacionEquipo, "AUTOMÓVIL"):
				t = NewLiviano("AUT")
			case strings.Contains(ih08p.DenominacionEquipo, "MINI"):
				t = NewLiviano("MIN")
			case strings.Contains(ih08p.DenominacionEquipo, "FURG"):
				t = NewLiviano("FUR")
			default:
				t = NewLiviano(ih08p.Tipo25)
			}
		}
	case "PES":
		switch {
		case strings.Contains(ih08p.DenominacionEquipo, "CAMION"):
			t = NewPesado("CAM")
		case strings.Contains(ih08p.DenominacionEquipo, "ACOPLADO"):
			t = NewPesado("ACS")
		case strings.Contains(ih08p.DenominacionEquipo, "SEMIRREMOLQUE"):
			t = NewPesado("ACS")
		case strings.Contains(ih08p.DenominacionEquipo, "CARRETON"):
			t = NewPesado("ACS")
		default:
			t = NewPesado(ih08p.Tipo25)
		}
	case "ADV":
		switch {

		case strings.Contains(ih08p.DenominacionEquipo, "MARTILLO"):
			t = NewAditamentoVehiculo("ADM")

		case strings.Contains(ih08p.DenominacionEquipo, "BALDE CRI"):
			t = NewAditamentoVehiculo("ADE")

		case strings.Contains(ih08p.DenominacionEquipo, "BRAZO"):
			t = NewAditamentoVehiculo("ADE")

		case strings.Contains(ih08p.DenominacionEquipo, "PALLET"):
			t = NewAditamentoVehiculo("ADM")

		case strings.Contains(ih08p.DenominacionEquipo, "HOYADORA"):
			t = NewAditamentoVehiculo("ADM")

		case strings.Contains(ih08p.DenominacionEquipo, "ROLO"):
			t = NewAditamentoVehiculo("ADE")

		case strings.Contains(ih08p.DenominacionEquipo, "PRESENTADOR"):
			t = NewAditamentoVehiculo("ADE")

		case strings.Contains(ih08p.DenominacionEquipo, "HIDROG"):
			t = NewAditamentoVehiculo("ADC")

		case strings.Contains(ih08p.DenominacionEquipo, "MIDWEST"):
			t = NewAditamentoVehiculo("ADT")

		case strings.Contains(ih08p.DenominacionEquipo, "TANQUE"):
			switch {
			case strings.Contains(ih08p.DenominacionEquipo, "ACOPLADO"):
				t = NewPesado("ACS")
			default:
				t = NewAditamentoVehiculo("ADC")
			}

		case strings.Contains(ih08p.DenominacionEquipo, "COMPACTADOR"):
			t = NewAditamentoVehiculo("ADC")
		case strings.Contains(ih08p.DenominacionEquipo, "CAJA METALICA VUELCO"):
			t = NewAditamentoVehiculo("ADC")
		case strings.Contains(ih08p.DenominacionEquipo, "VOLCADORA"):
			t = NewAditamentoVehiculo("ADC")
		case strings.Contains(ih08p.DenominacionEquipo, "COMPACTADOR DE CARGA"):
			t = NewAditamentoVehiculo("ADC")
		case strings.Contains(ih08p.DenominacionEquipo, "ENGRASE"):
			t = NewAditamentoVehiculo("ADC")
		case strings.Contains(ih08p.DenominacionEquipo, "VOLQUETE"):
			t = NewAditamentoVehiculo("ADC")
		}

	case "APE":
		switch {
		case strings.Contains(ih08p.DenominacionEquipo, "RETRO"):
			t = NewAmarilloPesado("EOG")
		case strings.Contains(ih08p.DenominacionEquipo, "MINICARGADORA"):
			t = NewAmarilloPesado("EOG")
		case strings.Contains(ih08p.DenominacionEquipo, "MINZANJADORA"):
			t = NewAmarilloPesado("EOG")
		case strings.Contains(ih08p.DenominacionEquipo, "XCAVADORA"):
			t = NewAmarilloPesado("EOG")
		case strings.Contains(ih08p.DenominacionEquipo, "TIENDETUBOS"):
			t = NewAmarilloPesado("TTP")
		default:
			t = NewAmarilloPesado(ih08p.Tipo25)
		}
	case "ELE":
		switch ih08p.Tipo25 {
		case "PAR":
			if strings.Contains(ih08p.DenominacionEquipo, "PLATAF") {
				t = NewElevacion("PAR")
			} else {
				t = NewElevacion("AEV")
			}
		case "EIZ":
			if strings.Contains(ih08p.DenominacionEquipo, "HIDROGR") {
				if !strings.Contains(ih08p.DenominacionEquipo, "ELEVADOR") {
					t = NewAditamentoVehiculo("ADC")
				}
			} else if strings.Contains(ih08p.DenominacionEquipo, "ELEVADOR") {
				t = NewElevacion("AEV")
			} else {
				t = NewElevacion("GRP")
			}
		default:
			switch {
			case strings.Contains(ih08p.DenominacionEquipo, "ELEVADOR"):
				t = NewElevacion("AEV")
			default:
				t = NewElevacion(ih08p.Tipo25)
			}
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

func get_t3(ih08p schema.IH08FPost, t2 T2Builder, db dbops.DB) (T3Builder, error) {

	v := t2.GetClaseV()
	// log.Println(v)
	// log.Println(ih08p)

	switch v {

	case "LIV PKP":
		pkp := &PKP{}
		pkp.BuildT3(ih08p, db)
		return pkp, nil

	case "LIV MIN":
		min := &MIN{}
		min.BuildT3(ih08p, db)
		return min, nil

	case "LIV FUR":
		min := &FUR{}
		min.BuildT3(ih08p, db)
		return min, nil

	case "LIV AUT":
		aut := &AUT{}
		aut.BuildT3(ih08p, db)
		return aut, nil

	case "PES CAM":
		cam := &CAM{}
		cam.BuildT3(ih08p, db)
		return cam, nil

	case "PES ACS":
		acs := &Acoplado{}
		acs.BuildT3(ih08p, db)
		return acs, nil

	case "ELE EIZ":
		if strings.Contains(ih08p.DenominacionEquipo, "HIDROGR") {
			eq := &AditamentoCamion{}
			eq.BuildT3(ih08p, db)
			return eq, nil

		} else {
			eq := &GruaPluma{}
			eq.BuildT3(ih08p, db)
			return eq, nil
		}

	case "ELE PAR":
		if strings.Contains(ih08p.DenominacionEquipo, "PLAT") {
			eq := &Plataforma{}
			eq.BuildT3(ih08p, db)
			// ele := &Elevacion{ClasePeso: "ELE", ClaseV: "PAR"}
			return eq, nil
		} else if strings.Contains(ih08p.DenominacionEquipo, "ELE") {
			eq := &Autoelevador{}
			eq.BuildT3(ih08p, db)
			// ele := &Elevacion{ClasePeso: "ELE", ClaseV: "AEV"}
			return eq, nil
		}

	case "ELE AEV":
		eq := &Autoelevador{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "ADV ADC":
		// TODO : Tener cuidado que vuelve de aqui -- HidroGrua ya no existe!
		// eq := getTipoADC(ih08p)
		eq := &AditamentoCamion{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	// case "ADV ADC":
	// 	eq := &AditamentoCamion{}
	// 	eq.BuildT3(ih08p, db)
	// 	return eq, nil

	case "ADV ADM":
		eq := &AditamentoMinicargadora{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "ADV ADE":
		eq := &AditamentoExcavadora{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "ADV ADT":
		eq := &AditamentoTiendetubo{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "ELE GRP":
		eq := &GruaPluma{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "APE EMS":
		eq := &Viales{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "APE TSN":
		eq := &EspecialesPesado{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "APE TTP":
		eq := &Piping{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "APE EOG":
		eq := &EqObrGral{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "TEL ETC":
		eq := &TendidoElectricoCabrestante{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "TEL AXL":
		eq := &AuxTendido{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "AUX TRC":
		eq := &ContObrador{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "AUX TDA":
		eq := &AUXTDuctos{}
		eq.BuildT3(ih08p, db)
		return eq, nil

	case "AUX AXL":
		eq := &AuxiliaresMisc{}
		eq.BuildT3(ih08p, db)
		return eq, nil
	// TENGO QUE DESAMBIGUAR LOS TIPOS 2/ 2.5 DE SAP PARA PODER MAPPEAR BIEN LOS TIPOS DE
	// LA TAXO!

	default:
		// Devuelvo nil pointer!
		// TODO Aqui incorporar lo no tipificado o los casos que sobran!

		if strings.Contains(ih08p.DenominacionEquipo, "HIDROGR") {
			eq := &AditamentoCamion{}
			eq.BuildT3(ih08p, db)
			// ele := &AditamentoVehiculo{ClasePeso: "ELE", ClaseV: "HDG"}
			return eq, nil
		}
		return nil, fmt.Errorf("INVALID T3 TYPE! %v - %v", ih08p, t2)
	}
	return nil, nil
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

		if t2 == nil {
			log.Printf("T1 => %#v -- T2 => %#v -- EQ => %#v", t1, t2, ih08p)
			log.Fatal("INVALID T2!")
		}

		t2s = append(t2s, t2)

		t3, err := get_t3(ih08p, t2s[len(t2s)-1], db)
		if err != nil {
			continue
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
	var (
		missT1 []schema.IH08FPost
		missT2 []schema.IH08FPost
		missT3 []schema.IH08FPost
	)

	for _, ih08p := range ih08 {
		t1, err := get_t1(ih08p, nil)
		if err != nil {
			// log.Printf("T1 => %#v T2 => %#v T3 => %#v", t1, t2, t3)
			// log.Printf("INVALID T1 => %#v -- EQ => %#v", t1, ih08p)
			missT1 = append(missT1, ih08p)
			continue
		}
		t1_id := t1.Find(db)
		t2, err := get_t2(ih08p, t1)
		if err != nil {
			missT2 = append(missT2, ih08p)
			continue
		}
		t2_id := t2.Find(db)
		t3, err := get_t3(ih08p, t2, db)
		if err != nil {
			missT3 = append(missT3, ih08p)
			continue
		}
		t3_id := t3.Find(db)

		// if t1.Nombre=="ADV" {
		// 	log.Printf("T1 => %#v T2 => %#v T3 => %#v", t1, t2, t3)
		// }

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

	for _, i := range missT1 {
		if err = i.InsertMissing(db); err != nil {
			log.Println("WHILE INSERTING MISSING T1 =>> ", err)
		}
	}

	for _, i := range missT2 {
		if err = i.InsertMissing(db); err != nil {
			log.Println("WHILE INSERTING MISSING T2 =>> ", err)
		}
	}

	for _, i := range missT3 {
		if err = i.InsertMissing(db); err != nil {
			log.Println("WHILE INSERTING MISSING T3 =>> ", err)
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
	case "AditamentoVehiculo":
		eq := AditamentoVehiculo{}
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
	case "FUR":
		eq := &FUR{}
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
	case "AditamentoCamion":
		eq := &AditamentoCamion{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "GruaPluma":
		eq := &GruaPluma{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "Autoelevador":
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
	case "EqObrGral":
		eq := &EqObrGral{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err

	case "AditamentoMinicargadora":
		eq := &AditamentoMinicargadora{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "AditamentoTiendetubo":
		eq := &AditamentoTiendetubo{}
		err := db.DB.Get(eq, q, t3_id)
		return eq, err
	case "AditamentoExcavadora":
		eq := &AditamentoExcavadora{}
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

	w := charmap.ISO8859_1.NewEncoder().Writer(file)
	// csvReader := csv.NewReader(reader)
	writer := csv.NewWriter(w)
	writer.Comma = ';'

	defer writer.Flush()

	// Write CSV header
	header := []string{"T2", "Desc T2",
		"T2.5", "Desc T2.5",
		"T3", "Desc T3",
		"TipoBien", "DenominacionEquipo", "Equipo"}
	writer.Write(header)

	// Write data
	for _, report := range reports {
		record := []string{
			report.T2,
			report.DescT2,
			report.T25,
			report.DescT25,
			report.T3,
			report.DescT3,
			report.TipoBien,
			report.DenomEquipo,
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
			continue
		}

		r := TaxoReport{
			Equipo:      t.Equipo,
			T2:          t1.Nombre,
			DescT2:      t1.Nombre,
			T25:         processClaseV(t2.GetClaseV()),
			DescT25:     getT25Desc(t2.GetClaseV(), db),
			T3:          t3.GetFuncion(),
			DescT3:      getT3Desc(t3.GetFuncion(), db),
			TipoBien:    t3.GetTipoBien(),
			DenomEquipo: t.Denominacion.String,
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

func getT25Desc(clv string, db dbops.DB) string {
	clv = processClaseV(clv)
	QUERY := `
		SELECT * FROM Clases WHERE clase LIKE ?
	`
	c := DescClases{}
	err := db.DB.Get(&c, QUERY, clv)
	if err != nil {
		log.Printf("WHILE GETTING CLASS DESC =>> %s  , %#v", clv, err)
		log.Println(c)
		return clv
	}
	return c.DescClase
}

func getT3Desc(clv string, db dbops.DB) string {
	QUERY := `
		SELECT * FROM Clases WHERE subclase LIKE ?
	`
	c := DescClases{}
	err := db.DB.Get(&c, QUERY, clv)
	if err != nil {
		log.Printf("WHILE GETTING CLASS DESC =>> %s  , %#v", clv, err)
		return clv
	}
	return c.DescSubClase
}

func processClaseV(cls string) string {
	words := strings.Split(cls, " ")
	return words[len(words)-1]
}

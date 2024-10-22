package taxo

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"taxo2/dbops"
	"taxo2/schema"
)

var (
	t3CAMTrans map[string]string
	t3PKPTrans map[string]string
	t3EMSTrans map[string]string
)

func init() {
	t3CAMTrans = map[string]string{
		"CAMION CHASIS CON HIDRO":    "CCH",
		"CAMION CHASIS CON ELEVADOR": "CCE",
		"CAMION CHASIS SIN HIDRO":    "CCS",
		"CAMION TRACTOR CON HIDRO":   "CTH",
		"CAMION TRACTOR SIN HIDRO":   "CTS",
		"CAMION VOLCADOR":            "CVO",
		"CAMION REGADOR":             "CRE",
		"CAMION COMPACTADOR":         "CCO",
		"CAMION PLATAFORMA":          "CPA",
		"CAMION PORTA VOLQUETE":      "CPV",
		"CAMION DE VACIO":            "CVA",
	}
	t3PKPTrans = map[string]string{
		"C/D 4X4": "PCD",
		"S/C 4X4": "PCS",
		"C/S 4X4": "PCS",
		"C/S 4X2": "PSC",
		"C/D 4X2": "PDS",
		"S/C 4X2": "PSC",
	}
	t3EMSTrans = map[string]string{
		"MOTONIVELADORA":                "MNV",
		"TOPADORA":                      "TPO",
		"PALA CARGADORA":                "PLC",
		"RODILLO VIBRATORIO MOTORIZADO": "RVM",
		"RODILLO COMPACTACION S/RUEDAS": "RNC",
	}

}

type T2Builder interface {
	Find(dbops.DB) sql.NullInt64
	GetClaseV() string
	Insert(dbops.DB) error
}

type T3Builder interface {
	BuildT3(schema.IH08FPost, dbops.DB)
	Find(dbops.DB) sql.NullInt64
	Insert(dbops.DB) error
	GetFuncion() string
	GetTipoBien() string
}

type Taxonomia struct {
	Equipo       int64          `db:"equipo"`
	T1           sql.NullInt64  `db:"t1"`
	T2           sql.NullInt64  `db:"t2"`
	T3           sql.NullInt64  `db:"t3"`
	Denominacion sql.NullString `db:"denominacion"`
}

func (t Taxonomia) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Taxonomia(equipo, t1, t2, t3, denominacion) 
		VALUES(:equipo, :t1, :t2, :t3, :denominacion)
	`
	if _, err := db.DB.NamedExec(QUERY, &t); err != nil {
		return err
	}
	return
}

type TaxoReport struct {
	Equipo      int64
	T2          string
	DescT2      string
	T25         string
	DescT25     string
	T3          string
	DescT3      string
	TipoBien    string
	DenomEquipo string
}

type DescClases struct {
	Id           int    `db:"id"`
	Clase        string `db:"clase"`
	SubClase     string `db:"subclase"`
	DescClase    string `db:"denominacion_clase"`
	DescSubClase string `db:"denominacion_subclase"`
}

// func (t *Taxonomia) build_desc(self, *args, **kwargs): pass

type ClasesEquipo struct {
	Nombre string        `db:"nombre"`
	Id     sql.NullInt64 `db:"id"`
}

func (eq ClasesEquipo) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO ClasesEquipo(id, nombre) VALUES(:id, :nombre)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (v *ClasesEquipo) Find(db dbops.DB) sql.NullInt64 {
	v2 := ClasesEquipo{}
	err := db.DB.Get(&v2, "SELECT * FROM ClasesEquipo WHERE nombre LIKE ?", v.Nombre)
	if err != nil {
		log.Fatal("While Finding ClaseEQ => ", err)
	}
	return v2.Id
}

type Liviano struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v Liviano) Find(db dbops.DB) sql.NullInt64 {
	v2 := Liviano{}
	err := db.DB.Get(&v2, "SELECT * FROM Liviano WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding Liviano => ", err)
	}
	return v2.Id
}

func NewLiviano(tipo2_5 string) (v Liviano) {
	v.ClasePeso = "LIV"
	v.ClaseV = tipo2_5
	return
}

func (v Liviano) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq Liviano) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Liviano(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

//--**--//

type Pesado struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v Pesado) Find(db dbops.DB) sql.NullInt64 {
	v2 := Pesado{}
	err := db.DB.Get(&v2, "SELECT * FROM Pesado WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding Pesado => ", err)
	}
	return v2.Id
}

func NewPesado(tipo2_5 string) (v Pesado) {
	v.ClasePeso = "PES"
	v.ClaseV = tipo2_5
	return
}

func (v Pesado) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq Pesado) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Pesado(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

// -- ** -- //

type Elevacion struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v Elevacion) Find(db dbops.DB) sql.NullInt64 {
	v2 := Elevacion{}
	err := db.DB.Get(&v2, "SELECT * FROM Elevacion WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding ELE => ", err)
	}
	return v2.Id
}

func NewElevacion(tipo2_5 string) (v Elevacion) {
	v.ClasePeso = "ELE"
	v.ClaseV = tipo2_5
	return
}

func (v Elevacion) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq Elevacion) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Elevacion(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

// -- ** -- //

type AmarilloPesado struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v AmarilloPesado) Find(db dbops.DB) sql.NullInt64 {
	v2 := AmarilloPesado{}
	err := db.DB.Get(&v2, "SELECT * FROM AmarilloPesado WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding APE => ", err)
	}
	return v2.Id
}

func NewAmarilloPesado(tipo2_5 string) (v AmarilloPesado) {
	v.ClasePeso = "APE"
	v.ClaseV = tipo2_5
	return
}

func (v AmarilloPesado) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq AmarilloPesado) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AmarilloPesado(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

// -- ** -- //

type Auxiliares struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v Auxiliares) Find(db dbops.DB) sql.NullInt64 {
	v2 := Auxiliares{}
	err := db.DB.Get(&v2, "SELECT * FROM Auxiliares WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding AUX => ", err)
	}
	return v2.Id
}

func NewAuxiliares(tipo2_5 string) (v Auxiliares) {
	v.ClasePeso = "AUX"
	v.ClaseV = tipo2_5
	return
}

func (v Auxiliares) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq Auxiliares) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Auxiliares(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

//--**--//

type TendidoElectrico struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v TendidoElectrico) Find(db dbops.DB) sql.NullInt64 {
	v2 := TendidoElectrico{}
	err := db.DB.Get(&v2, "SELECT * FROM TendidoElectrico WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding TEL => ", err)
	}
	return v2.Id
}

func NewTendidoElectrico(tipo2_5 string) (v TendidoElectrico) {
	v.ClasePeso = "TEL"
	v.ClaseV = tipo2_5
	return
}

func (v TendidoElectrico) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq TendidoElectrico) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO TendidoElectrico(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

//--**--//

type AditamentoVehiculo struct {
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v AditamentoVehiculo) Find(db dbops.DB) sql.NullInt64 {
	v2 := AditamentoVehiculo{}
	err := db.DB.Get(&v2, "SELECT * FROM AditamentoVehiculo WHERE clase_v LIKE ?", v.ClaseV)
	if err != nil {
		log.Fatal("While Finding TEL => ", err)
	}
	return v2.Id
}

func NewAditamentoVehiculo(tipo2_5 string) (v AditamentoVehiculo) {
	v.ClasePeso = "ADV"
	v.ClaseV = tipo2_5
	return
}

func (v AditamentoVehiculo) GetClaseV() string {
	return fmt.Sprintf("%s %s", v.ClasePeso, v.ClaseV)
}

func (eq AditamentoVehiculo) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AditamentoVehiculo(id, clase_v, clase_peso) VALUES(:id, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

//--**--//

type PKP struct {
	Cabinas   string        `db:"cabinas"`
	Traccion  string        `db:"traccion"`
	Modelo    string        `db:"modelo"`
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v *PKP) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.Cabinas = getCabinas(ih08p.DenominacionEquipo)
	v.Traccion = ih08p.GetTraccion(db)
	v.Modelo = ih08p.DenominacionEquipo
	v.ClaseV = "PKP"
	v.ClasePeso = "LIV"
}

func (v PKP) Find(db dbops.DB) sql.NullInt64 {
	v2 := PKP{}
	err := db.DB.Get(&v2, "SELECT * FROM PKP WHERE Modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding PKP => ", err)
	}
	return v2.Id
}

func (eq PKP) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO PKP(id, clase_v, cabinas, modelo, traccion, clase_peso)
		VALUES(:id, :clase_v, :cabinas, :modelo, :traccion, :clase_peso)
	`
	// log.Printf("PKP => %#v", eq)
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq PKP) GetFuncion() string {
	val := fmt.Sprintf("%s %s", eq.Cabinas, eq.Traccion)
	if t, e := t3PKPTrans[val]; !e {
		return val
	} else {
		return t
	}
}

func (eq PKP) GetTipoBien() string {
	return fmt.Sprintf("%s %s %s", eq.ClaseV, eq.Traccion, eq.Cabinas)
}

func getCabinas(desc string) string {

	switch {
	case strings.Contains(desc, "DC"):
		return "C/D"
	case strings.Contains(desc, "DOBLE"):
		return "C/D"
	case strings.Contains(desc, "SIMPLE"):
		return "C/S"
	case strings.Contains(desc, "D/C"):
		return "C/D"
	case strings.Contains(desc, "C/D"):
		return "C/D"
	case strings.Contains(desc, "C/S"):
		return "C/S"
	default:
		return "C/S"
	}
}

//--**--//

type MIN struct {
	Modelo        string         `db:"modelo"`
	Tipo          sql.NullString `db:"tipo"`
	CantPasajeros sql.NullString `db:"cant_pasajeros"`
	ClasePeso     string         `db:"clase_peso"`
	ClaseV        string         `db:"clase_v"`
	Id            sql.NullInt64  `db:"id"`
}

func (v *MIN) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "LIV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = sql.NullString{String: "MBN", Valid: true}
	v.ClaseV = "MIN"
	v.CantPasajeros = sql.NullString{}
}

func (v MIN) Find(db dbops.DB) sql.NullInt64 {
	v2 := MIN{}
	err := db.DB.Get(&v2, "SELECT * FROM MIN WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding MIN => ", err)
	}
	return v2.Id
}

func (eq MIN) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO MIN(id, clase_v, modelo, cant_pasajeros, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :cant_pasajeros, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq MIN) GetFuncion() string {
	return "MBN"
}

func (eq MIN) GetTipoBien() string {
	return fmt.Sprintf("MIN %s", eq.CantPasajeros.String)
}

//--**--//

type FUR struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *FUR) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "LIV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = sql.NullString{String: "FGD", Valid: true}
	v.ClaseV = "FUR"
}

func (v FUR) Find(db dbops.DB) sql.NullInt64 {
	v2 := FUR{}
	err := db.DB.Get(&v2, "SELECT * FROM FUR WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding FUR => ", err)
	}
	return v2.Id
}

func (eq FUR) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO FUR(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq FUR) GetFuncion() string {
	return eq.Tipo.String
}

func (eq FUR) GetTipoBien() string {
	return eq.Modelo
}

//--**--//

type AUT struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AUT) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "LIV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
	v.ClaseV = "AUT"
}

func (v AUT) Find(db dbops.DB) sql.NullInt64 {
	v2 := AUT{}
	err := db.DB.Get(&v2, "SELECT * FROM AUT WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AUT => ", err)
	}
	return v2.Id
}

func (eq AUT) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AUT(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo,:clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AUT) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AUT) GetTipoBien() string {
	return eq.Modelo
}

//--**--//

type CAM struct {
	Traccion  string        `db:"traccion"`
	Accesorio string        `db:"accesorio"`
	Capacidad string        `db:"capacidad"`
	Modelo    string        `db:"modelo"`
	Funcion   string        `db:"funcion"`
	ClaseV    string        `db:"clase_v"`
	ClasePeso string        `db:"clase_peso"`
	Id        sql.NullInt64 `db:"id"`
}

func (v *CAM) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	accesorio, _ := getAccesorio(ih08p, db)

	v.ClasePeso = "PES"
	v.ClaseV = "CAM"

	v.Traccion = ih08p.GetTraccion(db)
	v.Accesorio = accesorio.DenominacionEquipo
	v.Capacidad = getCapacidad(accesorio)
	v.Modelo = ih08p.DenominacionEquipo
	v.Funcion = getFuncion(ih08p, accesorio)
}

func (v CAM) Find(db dbops.DB) sql.NullInt64 {
	v2 := CAM{}
	err := db.DB.Get(&v2,
		"SELECT * FROM CAM WHERE modelo LIKE ? AND accesorio LIKE ? AND funcion LIKE ?",
		v.Modelo, v.Accesorio, v.Funcion)
	if err != nil {
		log.Fatal("While Finding CAM => ", err)
	}
	return v2.Id
}

func (eq CAM) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO CAM(id, traccion, accesorio, capacidad, modelo, funcion, clase_v, clase_peso)
		VALUES(:id, :traccion, :accesorio, :capacidad, :modelo, :funcion, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq CAM) GetFuncion() string {
	if t, e := t3CAMTrans[eq.Funcion]; !e {
		return eq.Funcion
	} else {
		return t
	}
}

func (eq CAM) GetTipoBien() string {
	// t, e := t3CAMTrans[eq.Funcion]
	unCap := "tn"
	return fmt.Sprintf("%s %s %s %s", eq.Funcion, eq.Capacidad, unCap, eq.Traccion)
}

func getCapacidad(accesorio schema.IH08FPost) string {

	d := accesorio.DenominacionEquipo
	if strings.Contains(d, "HIDRO") {
		return "15TM"
	} else if strings.Contains(d, "LTS") || strings.Contains(d, "lts") {
		words := strings.Split(d, " ")
		if len(words) < 3 {
			return strings.Join(words, " ")
		}
		return strings.Join(words[len(words)-3:], " ")
	} else if strings.Contains(d, "m3") {
		words := strings.Split(d, " ")
		if len(words) < 3 {
			return strings.Join(words, " ")
		}
		return strings.Join(words[len(words)-3:], " ")
	}
	return "N/A"
}

func getAccesorio(ih08p schema.IH08FPost, db dbops.DB) (acc schema.IH08FPost, err error) {
	if ih08p.EQInferior.Valid {
		err = db.DB.Get(&acc,
			"SELECT * FROM IH08F WHERE equipo=?",
			ih08p.EQInferior.Int64)

		if err != nil {
			log.Fatal("WHILE SELECT ACCESORIO => ", err)
		}

		// log.Printf("EQUIPO =>> %#v -- ACCESORIO =>> %#v", ih08p, acc)
		return
	}
	return
}

func getFuncion(ih08p schema.IH08FPost, accesorio schema.IH08FPost) string {
	d := accesorio.DenominacionEquipo
	switch {
	case strings.Contains(d, "VUELCO") || strings.Contains(d, "VOLCADOR"):
		return "CAMION VOLCADOR"
	case strings.Contains(d, "HIDRO"):
		return "CAMION CON HIDRO"
	case strings.Contains(d, "REGADOR"):
		return "CAMION REGADOR"
	case strings.Contains(d, "COMBUSTIBLE"):
		return "CAMION COMBUSTIBLE"
	case strings.Contains(d, "VACIO"):
		return "CAMION DE VACIO"
	default:
		return "CAMION CHASIS"
	}

}

//--**--//

type Acoplado struct {
	Tipo       sql.NullString `db:"tipo"`
	Modelo     string         `db:"modelo"`
	Aditamento string         `db:"aditamento"`
	Capacidad  string         `db:"capacidad"`
	CantEjes   string         `db:"cant_ejes"`
	ClaseV     string         `db:"clase_v"`
	ClasePeso  string         `db:"clase_peso"`
	Id         sql.NullInt64  `db:"id"`
}

func (v *Acoplado) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	aditamento, _ := getAccesorio(ih08p, db)

	v.ClaseV = "ACS"
	v.ClasePeso = "PES"

	v.Aditamento = aditamento.DenominacionEquipo
	v.Capacidad = getCapacidad(ih08p)
	v.Modelo = getDenomACS(ih08p.DenominacionEquipo)
	v.Tipo = getTipoACS(ih08p)
	v.CantEjes = getEjes(ih08p)
}

func (v Acoplado) Find(db dbops.DB) sql.NullInt64 {
	v2 := Acoplado{}
	err := db.DB.Get(&v2,
		"SELECT * FROM ACS WHERE modelo LIKE ? AND aditamento LIKE ? AND cant_ejes LIKE ?",
		v.Modelo, v.Aditamento, v.CantEjes)
	if err != nil {
		log.Fatal("While Finding ACS => ", err, v)
	}
	return v2.Id
}

func (eq Acoplado) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO ACS(id, tipo, modelo, aditamento, capacidad, cant_ejes, clase_v, clase_peso)
		VALUES(:id, :tipo, :modelo, :aditamento, :capacidad, :cant_ejes, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq Acoplado) GetFuncion() string {
	return eq.Tipo.String
}

func (eq Acoplado) GetTipoBien() string {
	// t, e := t3CAMTrans[eq.Funcion]
	var unCap string
	if strings.Contains(eq.Modelo, "REGADOR") {
		unCap = "m3"
	} else {
		unCap = "tn"
	}
	return fmt.Sprintf("%s %s %s %s", eq.Tipo.String, eq.CantEjes, eq.Capacidad, unCap)
}

func getEjes(ih08p schema.IH08FPost) string {
	d := ih08p.DenominacionEquipo

	re := regexp.MustCompile(`(\d) EJE[S]{0,1}`)
	if m := re.FindStringSubmatch(d); len(m) > 1 {
		return m[1]
	}
	return ""
}

func getDenomACS(d string) string {
	if strings.Contains(d, "SEMIREMOLQUE") {
		return strings.ReplaceAll(d, "SEMIREMOLQUE", "SEMIRREMOLQUE")
	}
	return d
}
func getTipoACS(ih08p schema.IH08FPost) sql.NullString {

	d := strings.ReplaceAll(ih08p.DenominacionEquipo,
		"SEMIREMOLQUE", "SEMIRREMOLQUE")

	switch {
	case strings.Contains(d, "SEMIRREMOLQUE TANQUE"):
		return sql.NullString{String: "SRT", Valid: true}
	case strings.Contains(d, "SEMIRREMOLQUE VOLCADOR"):
		return sql.NullString{String: "SVL", Valid: true}
	case strings.Contains(d, "SEMIRREMOLQUE VUELCO"):
		return sql.NullString{String: "SVL", Valid: true}
	case strings.Contains(d, "SEMIRREMOLQUE CISTERNA"):
		return sql.NullString{String: "SRT", Valid: true}
	case strings.Contains(d, "SEMIRREMOLQUE"):
		return sql.NullString{String: "SAC", Valid: true}
	case strings.Contains(d, "CISTERNA"):
		return sql.NullString{String: "ATS", Valid: true}
	case strings.Contains(d, "CARRETON"):
		return sql.NullString{String: "CAR", Valid: true}
	case strings.Contains(d, "TANQUE"):
		return sql.NullString{String: "ATS", Valid: true}
	default:
		return sql.NullString{String: "ACP", Valid: true}
	}
}

//--*--//

type GruaPluma struct {
	Tipo           sql.NullString `db:"tipo"`
	LongitudMax    sql.NullInt64  `db:"longitud_maxima"`
	CapacidadIzaje sql.NullString `db:"capacidad_de_izaje"`
	Modelo         string         `db:"modelo"`
	ClaseV         string         `db:"clase_v"`
	ClasePeso      string         `db:"clase_peso"`
	Id             sql.NullInt64  `db:"id"`
}

func (v *GruaPluma) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {

	v.ClaseV = "GRP"
	v.ClasePeso = "ELE"

	v.LongitudMax = sql.NullInt64{}
	v.CapacidadIzaje = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3

}

func (v GruaPluma) Find(db dbops.DB) sql.NullInt64 {
	v2 := GruaPluma{}
	err := db.DB.Get(&v2, "SELECT * FROM GruaPluma WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding GRPL => ", err)
	}
	return v2.Id
}

func (eq GruaPluma) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO GruaPluma( id, tipo, longitud_maxima, capacidad_de_izaje,
    modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :longitud_maxima, :capacidad_de_izaje,
		:modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq GruaPluma) GetFuncion() string {
	return eq.Tipo.String
}

func (eq GruaPluma) GetTipoBien() string {
	return fmt.Sprintf("%s %s m %s tn", eq.Tipo.String,
		strconv.Itoa(int(eq.LongitudMax.Int64)),
		eq.CapacidadIzaje.String)
}

//--**--//
//--**--//

type Autoelevador struct {
	Tipo           sql.NullString `db:"tipo"`
	LongitudMax    sql.NullInt64  `db:"longitud_maxima"`
	CapacidadIzaje sql.NullString `db:"capacidad_de_izaje"`
	Modelo         string         `db:"modelo"`
	ClaseV         string         `db:"clase_v"`
	ClasePeso      string         `db:"clase_peso"`
	Id             sql.NullInt64  `db:"id"`
}

func (v *Autoelevador) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {

	v.ClaseV = "AEV"
	v.ClasePeso = "ELE"

	v.LongitudMax = sql.NullInt64{}
	v.CapacidadIzaje = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoAEV(ih08p)
}

func (v Autoelevador) Find(db dbops.DB) sql.NullInt64 {
	v2 := Autoelevador{}
	err := db.DB.Get(&v2, "SELECT * FROM Autoelevador WHERE Modelo LIKE ?", v.Modelo)

	if err != nil {
		log.Fatal("While Finding AUTOELE => ", err)
	}
	return v2.Id
}

func (eq Autoelevador) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Autoelevador( id, tipo, longitud_maxima, capacidad_de_izaje,
    modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :longitud_maxima, :capacidad_de_izaje,
		:modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func getTipoAEV(i schema.IH08FPost) sql.NullString {
	switch {
	case strings.Contains(i.DenominacionEquipo, "AUTOS"):
		return sql.NullString{String: "PFE", Valid: true}
	case strings.Contains(i.DenominacionEquipo, "VEHÍCULOS"):
		return sql.NullString{String: "PFE", Valid: true}
	default:
		return sql.NullString{String: "AEV", Valid: true}

	}
}

func (eq Autoelevador) GetFuncion() string {
	return eq.Tipo.String
}

func (eq Autoelevador) GetTipoBien() string {
	return fmt.Sprintf("%s %s m %s tn",
		eq.Tipo.String,
		strconv.Itoa(int(eq.LongitudMax.Int64)),
		eq.CapacidadIzaje.String)
}

//--**--//

type Plataforma struct {
	Tipo         sql.NullString `db:"tipo"`
	AlturaMaxima sql.NullInt64  `db:"altura_maxima"`
	CargaMaxima  sql.NullString `db:"carga_maxima"`
	Modelo       string         `db:"modelo"`
	ClaseV       string         `db:"clase_v"`
	ClasePeso    string         `db:"clase_peso"`
	Id           sql.NullInt64  `db:"id"`
}

func (v *Plataforma) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {

	v.ClaseV = "PAR"
	v.ClasePeso = "ELE"

	v.AlturaMaxima = sql.NullInt64{}
	v.CargaMaxima = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3

}

func (v Plataforma) Find(db dbops.DB) sql.NullInt64 {
	v2 := Plataforma{}
	err := db.DB.Get(&v2, "SELECT * FROM Plataforma WHERE Modelo LIKE ?", v.Modelo)

	if err != nil {
		log.Fatal("While Finding PLAT => ", err)
	}
	return v2.Id
}

func (eq Plataforma) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Plataforma(id, tipo, altura_maxima, carga_maxima,
    modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :altura_maxima, :carga_maxima,
		:modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq Plataforma) GetFuncion() string {
	return eq.Tipo.String
}

func (eq Plataforma) GetTipoBien() string {
	return fmt.Sprintf("%s %s m %s tn",
		eq.Tipo.String,
		strconv.Itoa(int(eq.AlturaMaxima.Int64)),
		eq.CargaMaxima.String)
}

//--**--//

type Viales struct {
	Tipo      sql.NullString `db:"tipo"`
	PesoEq    sql.NullString `db:"peso_eq"`
	CantVias  sql.NullString `db:"cant_vias"`
	FzaExc    sql.NullString `db:"fza_excavacion"`
	Modelo    string         `db:"modelo"`
	ClaseV    string         `db:"clase_v"`
	ClasePeso string         `db:"clase_peso"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *Viales) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClaseV = "EMS"
	v.ClasePeso = "APE"
	v.PesoEq = sql.NullString{}
	v.CantVias = sql.NullString{}
	v.FzaExc = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
}

func (v Viales) Find(db dbops.DB) sql.NullInt64 {
	v2 := Viales{}
	err := db.DB.Get(&v2, "SELECT * FROM Viales WHERE Modelo LIKE ?", v.Modelo)

	if err != nil {
		log.Fatal("While Finding VIAL => ", err)
	}
	return v2.Id
}

func (eq Viales) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Viales(id, tipo, peso_eq, cant_vias, fza_excavacion, modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :peso_eq, :cant_vias, :fza_excavacion, :modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq Viales) GetFuncion() string {
	if t, e := t3EMSTrans[eq.Tipo.String]; !e {
		return eq.Tipo.String
	} else {
		return t
	}
}

func (eq Viales) GetTipoBien() string {
	return fmt.Sprintf("%s %s tn %s Vias",
		eq.Tipo.String,
		eq.PesoEq.String,
		eq.CantVias.String)
}

//--**--//

type EqObrGral struct {
	Tipo       sql.NullString `db:"tipo"`
	Aditamento sql.NullString `db:"aditamento"`
	Modelo     string         `db:"modelo"`
	PesoEq     sql.NullString `db:"peso_eq"`
	CantVias   sql.NullString `db:"cant_vias"`
	FzaExc     sql.NullString `db:"fza_excavacion"`
	ClaseV     string         `db:"clase_v"`
	ClasePeso  string         `db:"clase_peso"`
	Id         sql.NullInt64  `db:"id"`
}

func (v *EqObrGral) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {

	accesorio, _ := getAccesorio(ih08p, db)

	v.ClaseV = "EOG"
	v.ClasePeso = "APE"
	v.PesoEq = sql.NullString{}
	v.CantVias = sql.NullString{}
	v.FzaExc = sql.NullString{}
	v.Aditamento = sql.NullString{String: accesorio.DenominacionEquipo, Valid: true}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoEOG(ih08p)
}

func (v EqObrGral) Find(db dbops.DB) sql.NullInt64 {
	v2 := EqObrGral{}
	err := db.DB.Get(&v2, "SELECT * FROM EqObrGral WHERE Modelo LIKE ?", v.Modelo)

	if err != nil {
		log.Fatal("While Finding EaObrGral => ", err)
	}
	return v2.Id
}

func (eq EqObrGral) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO EqObrGral(id, tipo, aditamento, modelo, peso_eq, cant_vias, fza_excavacion, clase_v, clase_peso)
		VALUES( :id, :tipo, :aditamento, :modelo, :peso_eq, :cant_vias, :fza_excavacion, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq EqObrGral) GetFuncion() string {
	return eq.Tipo.String
}

func (eq EqObrGral) GetTipoBien() string {
	return fmt.Sprintf("%s %s tn %s Vias", eq.Tipo.String, eq.PesoEq.String, eq.CantVias.String)
}

func getTipoEOG(i schema.IH08FPost) sql.NullString {
	switch {
	case strings.Contains(i.DenominacionEquipo, "RETRO"):
		return sql.NullString{String: "PRE", Valid: true}
	case strings.Contains(i.DenominacionEquipo, "EXCAVADORA"):
		return sql.NullString{String: "EXC", Valid: true}
	case strings.Contains(i.DenominacionEquipo, "MINI"):
		return sql.NullString{String: "MCD", Valid: true}
	default:
		return i.Tipo3
	}
}

//--**--//

type Piping struct {
	Tipo        sql.NullString `db:"tipo"`
	CargaMaxima sql.NullString `db:"carga_maxima"`
	PesoEq      sql.NullString `db:"peso_eq"`
	Modelo      string         `db:"modelo"`
	ClaseV      string         `db:"clase_v"`
	ClasePeso   string         `db:"clase_peso"`
	Id          sql.NullInt64  `db:"id"`
}

func (v *Piping) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClaseV = "TTP"
	v.ClasePeso = "APE"
	v.CargaMaxima = sql.NullString{}
	v.PesoEq = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoTTP(ih08p)
}

func (v Piping) Find(db dbops.DB) sql.NullInt64 {
	v2 := Piping{}
	err := db.DB.Get(&v2, "SELECT * FROM Piping WHERE Modelo LIKE ?", v.Modelo)

	if err != nil {
		log.Fatal("While Finding PIPE => ", err)
	}
	return v2.Id
}

func (eq Piping) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO Piping(id, tipo, peso_eq, carga_maxima, modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :peso_eq, :carga_maxima, :modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq Piping) GetFuncion() string {
	return eq.Tipo.String
}

func (eq Piping) GetTipoBien() string {
	return fmt.Sprintf("%s %s tn", eq.Tipo.String, eq.CargaMaxima.String)
}

func getTipoTTP(i schema.IH08FPost) sql.NullString {
	// switch {
	// case strings.Contains(i.DenominacionEquipo, "TIENDETUBO"):
	// 	return sql.NullString{String: "TIT", Valid: true}
	// default:
	// 	return i.Tipo3
	// }
	return sql.NullString{String: "TIT", Valid: true}
}

// --**--//
type EspecialesPesado struct {
	Tipo        sql.NullString `db:"tipo"`
	CargaMaxima sql.NullString `db:"carga_maxima"`
	Modelo      string         `db:"modelo"`
	ClaseV      string         `db:"clase_v"`
	ClasePeso   string         `db:"clase_peso"`
	Id          sql.NullInt64  `db:"id"`
}

func (v *EspecialesPesado) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClaseV = "TSN"
	v.ClasePeso = "APE"
	v.CargaMaxima = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
}

func (v EspecialesPesado) Find(db dbops.DB) sql.NullInt64 {
	v2 := EspecialesPesado{}
	err := db.DB.Get(&v2, "SELECT * FROM EspecialesPesado WHERE Modelo LIKE ?", v.Modelo)

	if err != nil {
		log.Fatal("While Finding ESP PES => ", err)
	}
	return v2.Id
}

func (eq EspecialesPesado) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO EspecialesPesado(id, tipo, carga_maxima, modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :carga_maxima, :modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq EspecialesPesado) GetFuncion() string {
	return eq.Tipo.String
}

func (eq EspecialesPesado) GetTipoBien() string {
	return eq.Modelo
}

//--**--//

type AuxiliaresMisc struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AuxiliaresMisc) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "AUX"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
	v.ClaseV = "AXL"
}

func (v AuxiliaresMisc) Find(db dbops.DB) sql.NullInt64 {
	v2 := AuxiliaresMisc{}
	err := db.DB.Get(&v2, "SELECT * FROM AuxiliaresMisc WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AUXMISC => ", err)
	}
	return v2.Id
}

func (eq AuxiliaresMisc) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AuxiliaresMisc(id, tipo, modelo, clase_v, clase_peso)
		VALUES(:id, :tipo, :modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AuxiliaresMisc) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AuxiliaresMisc) GetTipoBien() string {
	return eq.Modelo
}

// --**--//
type ContObrador struct {
	Modelo    string        `db:"modelo"`
	Tipo      string        `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
}

func (v *ContObrador) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "AUX"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoObrador(ih08p.DenominacionEquipo)
	v.ClaseV = "TRC"
}

func (v ContObrador) Find(db dbops.DB) sql.NullInt64 {
	v2 := ContObrador{}
	err := db.DB.Get(&v2, "SELECT * FROM ContObrador WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding CONTOBR => ", err)
	}
	return v2.Id
}

func (eq ContObrador) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO ContObrador(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq ContObrador) GetFuncion() string {
	return eq.Tipo
}

func (eq ContObrador) GetTipoBien() string {
	return eq.Modelo
}

func getTipoObrador(d string) string {
	switch {
	case strings.Contains(d, "ONTEN"):
		return "CONTENEDOR"
	case strings.Contains(d, "CONTAINER"):
		return "CONTENEDOR"
	case strings.Contains(d, "MODU"):
		return "MODULO"
	case strings.Contains(d, "TRAIL"):
		return "TRAILER"
	case strings.Contains(d, "CASILLA"):
		return "CASILLA"
	default:
		return d
	}
}

//--**--//

type AUXTDuctos struct {
	Modelo    string        `db:"modelo"`
	Tipo      string        `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
}

func (v *AUXTDuctos) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "AUX"
	v.ClaseV = "TDA"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoATD(ih08p.Tipo3.String)
}

func (v AUXTDuctos) Find(db dbops.DB) sql.NullInt64 {
	v2 := AUXTDuctos{}
	err := db.DB.Get(&v2, "SELECT * FROM AUXTDuctos WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AUXTDuctos => ", err)
	}
	return v2.Id
}

func (eq AUXTDuctos) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AUXTDuctos(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AUXTDuctos) GetFuncion() string {
	return eq.Tipo
}

func (eq AUXTDuctos) GetTipoBien() string {
	return eq.Modelo
}

func getTipoATD(d string) string {
	return d
}

//--**--//

type TendidoElectricoCabrestante struct {
	Modelo    string        `db:"modelo"`
	Tipo      string        `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
}

func (v *TendidoElectricoCabrestante) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "TEL"
	v.ClaseV = "ETC"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoTEC(ih08p.Tipo3.String)
}

func (v TendidoElectricoCabrestante) Find(db dbops.DB) sql.NullInt64 {
	v2 := TendidoElectricoCabrestante{}
	err := db.DB.Get(&v2, "SELECT * FROM TendidoElectricoCabrestante WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding ETC => ", err)
	}
	return v2.Id
}

func (eq TendidoElectricoCabrestante) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO TendidoElectricoCabrestante(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq TendidoElectricoCabrestante) GetFuncion() string {
	return eq.Tipo
}

func (eq TendidoElectricoCabrestante) GetTipoBien() string {
	return eq.Modelo
}

func getTipoTEC(d string) string {
	return d
}

//--**--//

type AuxTendido struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AuxTendido) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "TEL"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoAuxTendido(ih08p.Tipo3)
	v.ClaseV = "ETC"
}

func (v AuxTendido) Find(db dbops.DB) sql.NullInt64 {
	v2 := AuxTendido{}
	err := db.DB.Get(&v2, "SELECT * FROM AuxTendido WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AuxTendido => ", err)
	}
	return v2.Id
}

func (eq AuxTendido) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AuxTendido(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AuxTendido) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AuxTendido) GetTipoBien() string {
	return eq.Modelo
}

func getTipoAuxTendido(d sql.NullString) sql.NullString {
	return d
}

//--**--//

type AditamentoMinicargadora struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AditamentoMinicargadora) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "ADV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoAditamentoMinicargadora(ih08p)
	v.ClaseV = "ADM"
}

func (v AditamentoMinicargadora) Find(db dbops.DB) sql.NullInt64 {
	v2 := AditamentoMinicargadora{}
	err := db.DB.Get(&v2, "SELECT * FROM AditamentoMinicargadora WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AditamentoMinicargadora => ", err)
	}
	return v2.Id
}

func (eq AditamentoMinicargadora) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AditamentoMinicargadora(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AditamentoMinicargadora) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AditamentoMinicargadora) GetTipoBien() string {
	return fmt.Sprintf("%s %s", eq.Tipo.String, eq.Modelo)
}

func getTipoAditamentoMinicargadora(i schema.IH08FPost) sql.NullString {
	switch {
	case strings.Contains(i.DenominacionEquipo, "MARTILLO"):
		return sql.NullString{String: "MHP", Valid: true}
	case strings.Contains(i.DenominacionEquipo, "HOYADORA"):
		return sql.NullString{String: "HOP", Valid: true}
	default:
		return i.Tipo3
	}
}

//--**--//

type AditamentoExcavadora struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AditamentoExcavadora) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "ADV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoAditamentoExcavadora(ih08p)
	v.ClaseV = "ADE"
}

func (v AditamentoExcavadora) Find(db dbops.DB) sql.NullInt64 {
	v2 := AditamentoExcavadora{}
	err := db.DB.Get(&v2, "SELECT * FROM AditamentoExcavadora WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AditamentoExcavadora => ", err)
	}
	return v2.Id
}

func (eq AditamentoExcavadora) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AditamentoExcavadora(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AditamentoExcavadora) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AditamentoExcavadora) GetTipoBien() string {
	// Tengo que ver que hay que ponerle!
	return fmt.Sprintf("%s", eq.Modelo)
}

func getTipoAditamentoExcavadora(i schema.IH08FPost) sql.NullString {
	switch {
	case strings.Contains(i.DenominacionEquipo, "PRESENTADOR"):
		return sql.NullString{String: "PRI", Valid: true}
	case strings.Contains(i.DenominacionEquipo, "BRAZO"):
		return sql.NullString{String: "BRE", Valid: true}
	default:
		return i.Tipo3
	}
}

//--**--//

type AditamentoTiendetubo struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AditamentoTiendetubo) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "ADV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoAditamentoTiendetubo(ih08p.Tipo3)
	v.ClaseV = "ADT"
}

func (v AditamentoTiendetubo) Find(db dbops.DB) sql.NullInt64 {
	v2 := AditamentoTiendetubo{}
	err := db.DB.Get(&v2, "SELECT * FROM AditamentoTiendetubo WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AditamentoTiendetubo => ", err)
	}
	return v2.Id
}

func (eq AditamentoTiendetubo) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AditamentoTiendetubo(id, clase_v, modelo, tipo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AditamentoTiendetubo) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AditamentoTiendetubo) GetTipoBien() string {
	return eq.Modelo
}

func getTipoAditamentoTiendetubo(d sql.NullString) sql.NullString {
	return sql.NullString{String: "ATT", Valid: true}
}

//--**--//

type AditamentoCamion struct {
	Modelo    string         `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	Capacidad sql.NullString `db:"capacidad"`
	ClasePeso string         `db:"clase_peso"`
	ClaseV    string         `db:"clase_v"`
	Id        sql.NullInt64  `db:"id"`
}

func (v *AditamentoCamion) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "ADV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = getTipoADC(ih08p)
	v.ClaseV = "ADC"
	// TODO : Esta parte quedo mal escrita!
	capacidad := getCapacidad(ih08p)
	v.Capacidad = sql.NullString{String: getCapacidad(ih08p), Valid: capacidad != ""}
}

func (v AditamentoCamion) Find(db dbops.DB) sql.NullInt64 {
	v2 := AditamentoCamion{}
	err := db.DB.Get(&v2, "SELECT * FROM AditamentoCamion WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding AditamentoCamion => ", err)
	}
	return v2.Id
}

func (eq AditamentoCamion) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO AditamentoCamion(id, clase_v, modelo, tipo, capacidad, clase_peso)
		VALUES(:id, :clase_v, :modelo, :tipo, :capacidad, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AditamentoCamion) GetFuncion() string {
	return eq.Tipo.String
}

func (eq AditamentoCamion) GetTipoBien() string {
	var unCap string
	if strings.Contains(eq.Modelo, "TANQUE") {
		unCap = "m3"
	} else if eq.Tipo.String == "CAV" {
		unCap = "m3"
	} else if eq.Capacidad.String == "N/A" {
		unCap = ""
	} else {
		unCap = "tn"
	}
	return fmt.Sprintf("%s %s %s", eq.Tipo.String, eq.Capacidad.String, unCap)
}

func getTipoADC(ih08p schema.IH08FPost) sql.NullString {
	if ih08p.Tipo3.Valid {
		return ih08p.Tipo3
	} else {
		switch {

		case strings.Contains(ih08p.DenominacionEquipo, "HIDROG"):
			return sql.NullString{String: "HIG", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "VACIO"):
			return sql.NullString{String: "TKV", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "REGADOR"):
			return sql.NullString{String: "TKV", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "ENGRASE"):
			return sql.NullString{String: "PEC", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "COMBUSTIBLE"):
			return sql.NullString{String: "TKF", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "TANQUE DE AGUA"):
			return sql.NullString{String: "TKR", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "CHUPADOR"):
			return sql.NullString{String: "TKC", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "CAJA METALICA"):
			return sql.NullString{String: "CAV", Valid: true}
		case strings.Contains(ih08p.DenominacionEquipo, "COMPACTADOR"):
			return sql.NullString{String: "CCO", Valid: true}

		default:
			return sql.NullString{}
		}
	}
}

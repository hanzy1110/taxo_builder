package taxo

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"taxo2/dbops"
	"taxo2/schema"
)

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
	Equipo   int64
	T2       string
	T25      string
	T3       string
	TipoBien string
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
	return fmt.Sprintf("PKP %s %s", eq.Traccion, eq.Cabinas)
}

func (eq PKP) GetTipoBien() string {
	return eq.Modelo
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
	Modelo    string        `db:"modelo"`
	Tipo      sql.NullString `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
}

func (v *MIN) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClasePeso = "LIV"
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
	v.ClaseV = "MIN"
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
		INSERT INTO MIN(id, clase_v, modelo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq MIN) GetFuncion() string {
	return fmt.Sprintf("MIN %s", eq.Modelo)
}

func (eq MIN) GetTipoBien() string {
	return eq.Modelo
}

//--**--//

type AUT struct {
	Modelo    string        `db:"modelo"`
	Tipo      sql.NullString        `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
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
		INSERT INTO AUT(id, clase_v, modelo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq AUT) GetFuncion() string {
	return fmt.Sprintf("AUT %s", eq.Modelo)
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
	return fmt.Sprintf("%s", eq.Funcion)
}

func (eq CAM) GetTipoBien() string {
	return eq.Modelo
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
		return "CAMION VACIO"
	default:
		return "CAMION CHASIS"
	}

}

//--**--//

type Acoplado struct {
	Tipo       sql.NullString `db:"tipo"`
	Modelo     string        `db:"modelo"`
	Aditamento string        `db:"aditamento"`
	Capacidad  string        `db:"capacidad"`
	CantEjes   string        `db:"cant_ejes"`
	ClaseV     string        `db:"clase_v"`
	ClasePeso  string        `db:"clase_peso"`
	Id         sql.NullInt64 `db:"id"`
}

func (v *Acoplado) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	aditamento, _ := getAccesorio(ih08p, db)

	v.ClaseV = "ACS"
	v.ClasePeso = "PES"

	v.Aditamento = aditamento.DenominacionEquipo
	v.Capacidad = getCapacidad(ih08p)
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
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
	return eq.Tipo.String
}

func getEjes(ih08p schema.IH08FPost) string {
	d := ih08p.DenominacionEquipo

	re := regexp.MustCompile(`(\d) EJE[S]{0,1}`)
	if m := re.FindStringSubmatch(d); len(m) > 1 {
		return m[1]
	}
	return ""
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
	return eq.Modelo
}

//--**--//

type HidroGrua struct {
	Tipo           sql.NullString `db:"tipo"`
	LongitudMax    sql.NullInt64  `db:"longitud_maxima"`
	CapacidadIzaje sql.NullString `db:"capacidad_de_izaje"`
	Modelo         string         `db:"modelo"`
	ClaseV         string         `db:"clase_v"`
	ClasePeso      string         `db:"clase_peso"`
	Id             sql.NullInt64  `db:"id"`
}

func (v *HidroGrua) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {

	v.ClaseV = "HDG"
	v.ClasePeso = "ELE"

	v.LongitudMax = sql.NullInt64{}
	v.CapacidadIzaje = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3

}

func (v HidroGrua) Find(db dbops.DB) sql.NullInt64 {
	v2 := HidroGrua{}
	err := db.DB.Get(&v2, "SELECT * FROM HidroGrua WHERE modelo LIKE ?", v.Modelo)
	if err != nil {
		log.Fatal("While Finding HIDRO => ", err)
	}
	return v2.Id
}

func (eq HidroGrua) Insert(db dbops.DB) (err error) {
	const QUERY = `
		INSERT INTO HidroGrua( id, tipo, longitud_maxima, capacidad_de_izaje,
    modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :longitud_maxima, :capacidad_de_izaje,
		:modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq HidroGrua) GetFuncion() string {
	// Claramente hay conflictos con lo que hay en sap y desambiguar
	return eq.Tipo.String
}

func (eq HidroGrua) GetTipoBien() string {
	return eq.Modelo
}

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

	v.ClaseV = "Autoelevador"
	v.ClasePeso = "ELE"

	v.LongitudMax = sql.NullInt64{}
	v.CapacidadIzaje = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3

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

func (eq Autoelevador) GetFuncion() string {
	return eq.Tipo.String
}

func (eq Autoelevador) GetTipoBien() string {
	return eq.Modelo
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

	v.ClaseV = "Plataforma"
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
	return eq.Modelo
}

//--**--//

type Viales struct {
	Tipo        sql.NullString `db:"tipo"`
	CargaMaxima sql.NullString `db:"carga_maxima"`
	Modelo      string         `db:"modelo"`
	ClaseV      string         `db:"clase_v"`
	ClasePeso   string         `db:"clase_peso"`
	Id          sql.NullInt64  `db:"id"`
}

func (v *Viales) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClaseV = "EMS"
	v.ClasePeso = "APE"
	v.CargaMaxima = sql.NullString{}
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
		INSERT INTO Viales(id, tipo, carga_maxima, modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :carga_maxima, :modelo, :clase_v, :clase_peso)
	`
	if _, err := db.DB.NamedExec(QUERY, &eq); err != nil {
		return err
	}
	return
}

func (eq Viales) GetFuncion() string {
	return eq.Tipo.String
}

func (eq Viales) GetTipoBien() string {
	return eq.Modelo
}

//--**--//

type Piping struct {
	Tipo        sql.NullString `db:"tipo"`
	CargaMaxima sql.NullString `db:"carga_maxima"`
	Modelo      string         `db:"modelo"`
	ClaseV      string         `db:"clase_v"`
	ClasePeso   string         `db:"clase_peso"`
	Id          sql.NullInt64  `db:"id"`
}

func (v *Piping) BuildT3(ih08p schema.IH08FPost, db dbops.DB) {
	v.ClaseV = "TTP"
	v.ClasePeso = "APE"
	v.CargaMaxima = sql.NullString{}
	v.Modelo = ih08p.DenominacionEquipo
	v.Tipo = ih08p.Tipo3
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
		INSERT INTO Piping(id, tipo, carga_maxima, modelo, clase_v, clase_peso)
		VALUES( :id, :tipo, :carga_maxima, :modelo, :clase_v, :clase_peso)
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
	return eq.Modelo
}

//--**--//
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
	Modelo    string        `db:"modelo"`
	Tipo    sql.NullString `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
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

//--**--//
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
	Modelo    string        `db:"modelo"`
	Tipo      sql.NullString        `db:"tipo"`
	ClasePeso string        `db:"clase_peso"`
	ClaseV    string        `db:"clase_v"`
	Id        sql.NullInt64 `db:"id"`
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
		INSERT INTO AuxTendido(id, clase_v, modelo, clase_peso)
		VALUES(:id, :clase_v, :modelo, :clase_peso)
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

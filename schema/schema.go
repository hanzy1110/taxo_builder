package schema

import (
	// "fmt"
	// "encoding/json"
	"database/sql"
	"taxo2/dbops"

	// "reflect"
	"strings"
	"log"
	// "time"
	// "github.com/jmoiron/sqlx"
)

const QUERY_I_IH08 = `
INSERT INTO IH08 (
    equipo, ano_construc, n_inventario, denominacion_estado, sociedad,
    centro_coste, ce_emplazam, status_usuario, elemento_pep, modificado_por,
    ubicac_tecnica, matricul_vehic, numero_de_serie, tipo_2_5, tipo_2, tipo_3,
    parsed_plate, denominacion_equipo, fabr_n_serie, eq_superior
) VALUES (
    :equipo, :ano_construc, :n_inventario, :denominacion_estado, :sociedad,
    :centro_coste, :ce_emplazam, :status_usuario, :elemento_pep, :modificado_por,
	:ubicac_tecnica, :matricul_vehic, :numero_de_serie, :tipo_2_5, :tipo_2, :tipo_3,
    :parsed_plate, :denominacion_equipo, :fabr_n_serie, :eq_superior
)
	`

const QUERY_I_IH08FIXED = `
INSERT INTO IH08F (
    equipo, 
    equipo_inferior,
    denominacion_del_equipo,
    accesorio,
    centro_coste,
    tipo_2,
    tipo_2_5,
		tipo_3,
    parsed_plate
  ) VALUES (
    :equipo, 
    :equipo_inferior,
    :denominacion_del_equipo,
    :accesorio,
    :centro_coste,
    :tipo_2,
    :tipo_2_5,
    :tipo_3,
    :parsed_plate
)
`
const QUERY_I_IH08FIXED_MISSING = `
INSERT INTO IH08F_MISSING (
    equipo, 
    equipo_inferior,
    denominacion_del_equipo,
    accesorio,
    centro_coste,
    tipo_2,
    tipo_2_5,
		tipo_3,
    parsed_plate
  ) VALUES (
    :equipo, 
    :equipo_inferior,
    :denominacion_del_equipo,
    :accesorio,
    :centro_coste,
    :tipo_2,
    :tipo_2_5,
    :tipo_3,
    :parsed_plate
)
`
type Inserter interface{
	Insert(*dbops.DB) error
}

func InsertEQS[T Inserter](eqs []T, db *dbops.DB) (err error) {

	for _, eq := range eqs {
		if err = eq.Insert(db); err!=nil {
			log.Printf("WHILE INSERTING => %v, %v", err, eq)
		}
	}

	return
}

type IH08 = []IH08Post
type IH08Post struct {
	Equipo              int64 `db:"equipo"`
	AnoConstruc         string `db:"ano_construc"`
	NInventario         string `db:"n_inventario"`
	DenominacionEstado  string `db:"denominacion_estado"`
	Sociedad            string `db:"sociedad"`
	CentroCoste         string `db:"centro_coste"`
	CeEmplazam          string `db:"ce_emplazam"`
	StatusUsuario       string `db:"status_usuario"`
	ElementoPep         string `db:"elemento_pep"`
	ModificadoPor       string `db:"modificado_por"`
	UbicacTecnica       string `db:"ubicac_tecnica"`
	MatriculVehic       string `db:"matricul_vehic"`
	NumeroDeSerie       string `db:"numero_de_serie"`
	Tipo25              string `db:"tipo_2_5"`
	Tipo2               string `db:"tipo_2"`
	Tipo3               sql.NullString `db:"tipo_3"`
	ParsedPlate         sql.NullString `db:"parsed_plate"`
	DenominacionEquipo  string `db:"denominacion_equipo"`
	FabrNSerie          sql.NullString `db:"fabr_n_serie"`
	EQSuperior          sql.NullInt64 `db:"eq_superior"`
}

func (eq IH08Post) Insert(db *dbops.DB) (err error) {
	if _, err := db.DB.NamedExec(QUERY_I_IH08, &eq); err!=nil {
		return err
	} 
	return
}

func (i *IH08Post) UpdatePlate(p sql.NullString) {
	i.ParsedPlate = p
}

type IH08F = []IH08FPost
type IH08FPost struct {
	Equipo              int64 `db:"equipo"`
	CentroCoste         string `db:"centro_coste"`
	Accesorio           sql.NullString `db:"accesorio"`
	Tipo2               string `db:"tipo_2"`
	Tipo25              string `db:"tipo_2_5"`
	Tipo3               sql.NullString `db:"tipo_3"`
	ParsedPlate         sql.NullString `db:"parsed_plate"`
	DenominacionEquipo  string `db:"denominacion_del_equipo"`
	EQInferior          sql.NullInt64 `db:"equipo_inferior"`
}

func (eq IH08FPost) Insert(db *dbops.DB) (err error) {
	if _, err:=db.DB.NamedExec(QUERY_I_IH08FIXED, &eq); err!=nil {
		return err
	}
	return
}


func (eq IH08FPost) InsertMissing(db dbops.DB) (err error) {
	if _, err:=db.DB.NamedExec(QUERY_I_IH08FIXED_MISSING, &eq); err!=nil {
		return err
	}
	return
}

func NewIH08FPost(eq IH08Post, db *dbops.DB) (eqf IH08FPost, err error) {
	eq_inf := getEQInferior(eq.Equipo, db)
	var accesorio sql.NullString
	var eq_inf_int sql.NullInt64

	if eq_inf.Equipo!=0 {
		accesorio = sql.NullString{String:eq_inf.DenominacionEquipo, Valid:true}
		eq_inf_int = sql.NullInt64{Int64:eq_inf.Equipo, Valid:true}
	} 

	eqf = IH08FPost{
		Equipo: eq.Equipo,
		CentroCoste: eq.CentroCoste,
		ParsedPlate: eq.ParsedPlate,
		Accesorio: accesorio, 
		Tipo2: eq.Tipo2,
		Tipo25: eq.Tipo25,
		Tipo3: eq.Tipo3,
		DenominacionEquipo: eq.DenominacionEquipo,
		EQInferior: eq_inf_int,
	}
	return
}

func getEQInferior(e int64, db *dbops.DB) (eq IH08Post) {
	if err:=db.DB.Get(&eq, "SELECT * FROM IH08 WHERE eq_superior=?", e); err!=nil {
		// log.Println("WHILE GETTING EQ INFERIOR =>> ", err, e)
	} else {
		log.Println("EQ INF FOUND => ", eq.Equipo)
	}
	return
}

func (eq IH08FPost) GetTraccion(db dbops.DB) string {
	switch {
	case strings.Contains(eq.Tipo3.String, "DOBLE TRACCION"):
		return "4X4"
	case strings.Contains(eq.DenominacionEquipo, "X4"):
		if eq.Tipo25 == "CAM" {
			return "6X4"
		}
		return "4X4"
	case strings.Contains(eq.DenominacionEquipo, "x4"):
		return "4X4"
	default:
		return "4X2"
	}
}

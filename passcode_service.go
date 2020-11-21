package passcode

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (

	DriverPostgres   = "postgres"
	DriverMysql      = "mysql"
	DriverMssql      = "mssql"
	DriverOracle     = "oracle"
	DriverNotSupport = "no support"
)

type PasscodeService struct {
	db            *sql.DB
	tableName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeService(db *sql.DB, tableName string, idName string, passcodeName string, expiredAtName string) *PasscodeService {
	return &PasscodeService{
		db:            db,
		tableName:     strings.ToLower(tableName),
		idName:        strings.ToLower(idName),
		passcodeName:  strings.ToLower(passcodeName),
		expiredAtName: strings.ToLower(expiredAtName),
	}
}

func NewDefaultPasscodeService(db *sql.DB, tableName string) *PasscodeService {
	return NewPasscodeService(db, tableName, "id", "passcode", "expiredat")
}

type BatchStatement struct {
	Query         string
	Values        []interface{}
	Keys          []string
	Columns       []string
	Attributes    map[string]interface{}
	AttributeKeys map[string]interface{}
}

func (s *PasscodeService) Save(ctx context.Context, id string, passcode string, expireAt time.Time) (int64, error) {
	mainScope := BatchStatement{}
	mainScope.Values = append(mainScope.Values,id)
	mainScope.Values = append(mainScope.Values,passcode)
	mainScope.Values = append(mainScope.Values,expireAt)
	mainScope.Values = append(mainScope.Values,id)
	mainScope.Values = append(mainScope.Values,passcode)
	mainScope.Values = append(mainScope.Values,expireAt)
	var placeholder []string
	columns := []string{s.idName, s.passcodeName, s.expiredAtName}
	for i := 0; i < 3; i++ {
		placeholder = append(placeholder, "?")
	}
	var queryString string
	dialect := GetDriverName(s.db)
	if  dialect== DriverPostgres  {
		setColumns := make([]string, 0)
		for _, key := range columns {
			setColumns = append(setColumns, key +" = ?")
		}
		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES  %s  ON CONFLICT (%s) DO UPDATE SET %s",
			(s.tableName),
			strings.Join(columns, ", "),
			"("+strings.Join(placeholder, ", ")+")",
			s.idName,
			strings.Join(setColumns, ", "),
		)
	} else if dialect == DriverMysql {
		setColumns := make([]string, 0)
		for _, key := range columns {
			setColumns = append(setColumns, key+" = ?")
		}

		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			(s.tableName),
			strings.Join(columns, ", "),
			"("+strings.Join(placeholder, ", ")+")",
			strings.Join(setColumns, ", "),
		)
	} else if dialect == DriverMssql {
		setColumns := make([]string, 0)
		onDupe := s.tableName + "." + s.idName + " = " + "temp." + s.idName
		for _, key := range columns {
			setColumns = append(setColumns, key+" = temp."+key)
		}
		queryString = fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
			s.tableName,
			strings.Join(placeholder, ", "),
			strings.Join(columns, ", "),
			onDupe,
			strings.Join(setColumns, ", "),
			strings.Join(columns, ", "),
			strings.Join(placeholder, ", "),
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", dialect)
	}
	mainScope.Query = ReplaceQueryparam(dialect,queryString,len(mainScope.Values))

	x,err := s.db.Exec(mainScope.Query, mainScope.Values...)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func (s *PasscodeService) Load(ctx context.Context, id string) (string, time.Time, error) {
	driverName := GetDriverName(s.db)
	arr := make(map[string]interface{})
	strSql := `SELECT * FROM ` + s.tableName + ` WHERE ` + s.idName + ` = ?`
	strSql = ReplaceQueryparam(driverName,strSql,1)
	rows,err := s.db.Query(strSql,id)
	if err != nil {
		return "", time.Now().Add(-24 * time.Hour), err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return "", time.Now().Add(-24 * time.Hour), err
		}

		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			arr[colName] = *val
		}
	}

	err2 := rows.Err()
	if err2 != nil {
		return "", time.Now().Add(-24 * time.Hour), err2
	}

	if len(arr) == 0 {
		return "", time.Now().Add(-24 * time.Hour), nil
	}

	var code string
	if driverName == DriverPostgres {
		code = arr[s.passcodeName].(string)
	} else {
		code = string(arr[s.passcodeName].([]byte))
	}
	expiredAt := arr[s.expiredAtName].(time.Time)
	return code, expiredAt, nil
}

func (s *PasscodeService) Delete(ctx context.Context, id string) (int64, error) {
	strSQL := `DELETE FROM ` + s.tableName + ` WHERE ` + s.idName + ` = '` + id + `'`
	x,err := s.db.Exec(strSQL)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func ReplaceQueryparam(driver string, query string, n int) string {
	if driver == DriverOracle || driver == DriverPostgres {
		var x string
		if driver == DriverOracle {
			x = ":val"
		} else {
			x = "$"
		}
		for i := 0; i < n; i++ {
			count := i + 1
			query = strings.Replace(query, "?", x+fmt.Sprintf("%v", count), 1)
		}
	}
	return query
}

func GetDriverName(db *sql.DB) string {
	if db == nil {
		return DriverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*godror.drv":
		return DriverOracle
	default:
		return DriverNotSupport
	}
}

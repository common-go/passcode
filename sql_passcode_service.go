package passcode

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
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

type SQLPasscodeService struct {
	db            *sql.DB
	tableName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeService(db *sql.DB, tableName string, options ...string) *SQLPasscodeService {
	var idName, passcodeName, expiredAtName string
	if len(options) >= 1 && len(options[0]) > 0 {
		expiredAtName = options[0]
	} else {
		expiredAtName = "expiredat"
	}
	if len(options) >= 2 && len(options[1]) > 0 {
		idName = options[1]
	} else {
		idName = "id"
	}
	if len(options) >= 3 && len(options[2]) > 0 {
		passcodeName = options[2]
	} else {
		passcodeName = "passcode"
	}
	return &SQLPasscodeService{
		db:            db,
		tableName:     strings.ToLower(tableName),
		idName:        strings.ToLower(idName),
		passcodeName:  strings.ToLower(passcodeName),
		expiredAtName: strings.ToLower(expiredAtName),
	}
}

func (s *SQLPasscodeService) Save(ctx context.Context, id string, passcode string, expireAt time.Time) (int64, error) {
	var placeholder []string
	columns := []string{s.idName, s.passcodeName, s.expiredAtName}
	var queryString string
	driverName := GetDriverName(s.db)
	for i := 0; i < 3; i++ {
		placeholder = append(placeholder, BuildParam(i+1, driverName))
	}
	if driverName == DriverPostgres {
		setColumns := make([]string, 0)
		for i, key := range columns {
			setColumns = append(setColumns, key+" = "+BuildParam(i+4, driverName))
		}
		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES  %s  ON CONFLICT (%s) DO UPDATE SET %s",
			(s.tableName),
			strings.Join(columns, ", "),
			"("+strings.Join(placeholder, ", ")+")",
			s.idName,
			strings.Join(setColumns, ", "),
		)
	} else if driverName == DriverMysql {
		setColumns := make([]string, 0)
		for i, key := range columns {
			setColumns = append(setColumns, key+" = "+BuildParam(i+3, driverName))
		}

		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			(s.tableName),
			strings.Join(columns, ", "),
			"("+strings.Join(placeholder, ", ")+")",
			strings.Join(setColumns, ", "),
		)
	} else if driverName == DriverMssql {
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
	} else if driverName == DriverOracle {
		var placeholderOracle []string
		for i := 0; i < 3; i++ {
			placeholderOracle = append(placeholderOracle, BuildParam(i+4, driverName))
		}
		setColumns := make([]string, 0)
		onDupe := s.tableName + "." + s.idName + " = " + "temp." + s.idName
		for _, key := range columns {
			if key == s.idName {
				continue
			}
			setColumns = append(setColumns, key+" = temp."+key)
		}
		queryString = fmt.Sprintf("MERGE INTO %s USING (SELECT %s as %s, %s as %s, %s as %s  FROM dual) temp ON (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			s.tableName,
			BuildParam(1, driverName), s.idName,
			BuildParam(2, driverName), s.passcodeName,
			BuildParam(3, driverName), s.expiredAtName,
			onDupe,
			strings.Join(setColumns, ", "),
			strings.Join(columns, ", "),
			strings.Join(placeholderOracle, ", "),
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", driverName)
	}
	x, err := s.db.Exec(queryString, id, passcode, expireAt, id, passcode, expireAt)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func (s *SQLPasscodeService) Load(ctx context.Context, id string) (string, time.Time, error) {
	driverName := GetDriverName(s.db)
	arr := make(map[string]interface{})
	strSql := `SELECT * FROM ` + s.tableName + ` WHERE ` + s.idName + ` = ` + BuildParam(1, driverName)
	rows, err := s.db.Query(strSql, id)
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
	var expiredAt time.Time
	if driverName == DriverPostgres {
		code = arr[s.passcodeName].(string)
	} else if driverName == DriverOracle {
		code = arr[strings.ToUpper(s.passcodeName)].(string)
	} else {
		code = string(arr[s.passcodeName].([]byte))
	}
	if driverName == DriverOracle {
		expiredAt = arr[strings.ToUpper(s.expiredAtName)].(time.Time)
	} else {
		expiredAt = arr[s.expiredAtName].(time.Time)
	}
	return code, expiredAt, nil
}

func (s *SQLPasscodeService) Delete(ctx context.Context, id string) (int64, error) {
	driverName := GetDriverName(s.db)
	strSQL := `DELETE FROM ` + s.tableName + ` WHERE ` + s.idName + ` =  ` + BuildParam(1, driverName)
	x, err := s.db.Exec(strSQL, id)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func BuildParam(index int, driver string) string {
	switch driver {
	case DriverPostgres:
		return "$" + strconv.Itoa(index)
	case DriverOracle:
		return ":val" + strconv.Itoa(index)
	default:
		return "?"
	}
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

// V$LOGMNR_CONTENTS contains log history information. To query this view, you must have the SELECT ANY TRANSACTION privilege.
//SCN			System change number (SCN) when the database change was made

//START_SCN 	System change number (SCN) when the transaction that contains this change started;
//				only meaningful if the COMMITTED_DATA_ONLY option was chosen in a DBMS_LOGMNR.START_LOGMNR() invocation, NULL otherwise.
//				This column may also be NULL if the query is run over a time/SCN range that does not contain the start of the transaction.

package main3

import (
	"database/sql"
	"fmt"
	_ "gopkg.in/goracle.v2"
)

type logmnr_content struct {
	threadnumber   int
	scn            int64
	start_scn      int64
	commit_scn     int64
	timestamp      int64
	operation_code int
	operation      string
	status         int
	seg_type_name  string
	infotxt        string
	seg_owner      string
	table_name     string
	username       string
	sql_redo       string
	row_id         string
	csf            string
	TABLE_SPACE    string
	SESSION_INFO   string
	RS_ID          string
	RBASQN         string
	RBABLK         string
	SEQUENCE       string
	TX_NAME        string
	SEG_NAME       string
	SEG_TYPE_NAME  string
}

func main() {
	lc := logmnr_content{}

	// query el ultimo SCN exportado al sink
	var scn_inicial string

	// connectar a oracle
	db, err := sql.Open("goracle", "scott/tiger@10.0.1.127:1521/orclpdb1")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	//execute anonymous block and use sql.Out interface for extracting bind variable value
	// db.Exec("begin :1 := f_get_dept_avg_sal(:2); end;", sql.Out{Dest: &avgSal}, deptId)
	db.Exec("BEGIN sys.DBMS_LOGMNR_D.build (dictionary_filename => 'lgmnrdict.ora',	dictionary_location => 'LOG_DIR')	END;)")

	var member []string

	rows, err := db.Query("select member from v$logfile")
	if err != nil {
		fmt.Println("Error running query")
		fmt.Println(err)
		return
	}
	defer rows.Close()
	var i int

	for rows.Next() {
		rows.Scan(&member[i])
		i++
	}
	// query para sacar los DML comiteados entre dos SCN
	var q, q2 string

	q = `SELECT thread#, scn, start_scn, commit_scn,timestamp, operation_code, operation,status, SEG_TYPE_NAME ,
	info,seg_owner, table_name, username, sql_redo ,row_id, csf, TABLE_SPACE, SESSION_INFO, RS_ID, RBASQN, RBABLK, SEQUENCE#, TX_NAME, SEG_NAME, 
	SEG_TYPE_NAME 
	FROM  v$logmnr_contents  
	WHERE OPERATION_CODE in (1,2,3) and commit_scn >= `

	q2 = `SELECT scn, sql_redo 	FROM  v$logmnr_contents  WHERE OPERATION_CODE in (1,2,3) and commit_scn >= ` + scn_inicial

	rows, err = db.Query(q2)
	if err != nil {
		fmt.Println("Error running query")
		fmt.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&lc.scn, &lc.sql_redo)
	}

}

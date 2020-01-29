// V$LOGMNR_CONTENTS contains log history information. To query this view, you must have the SELECT ANY TRANSACTION privilege.
//SCN			System change number (SCN) when the database change was made

//START_SCN 	System change number (SCN) when the transaction that contains this change started;
//				only meaningful if the COMMITTED_DATA_ONLY option was chosen in a DBMS_LOGMNR.START_LOGMNR() invocation, NULL otherwise.
//				This column may also be NULL if the query is run over a time/SCN range that does not contain the start of the transaction.

package main3

import (
	"database/sql"
	"fmt"
	"github.com/Shopify/sarama"
	_ "gopkg.in/goracle.v2"
	"log"
	"time"
	"encoding/json"
	"io/ioutil"
	"strings"
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

type OracleInstance struct {
	InstanceName string `json:"InstanceName`
	InstancePort string `json:"InstancePort"`
	LastSCN		 string `json:"LastSCN"`
	Username	 string `json:"Username"`
	Password	 string `json:"Password"`
	Hostname	 string `json:"Hostname"`
}
type OracleInstances struct {
	ListInstances []OracleInstance `json:"ListInstances"`
}

func main() {
	var topic string
	topic = "foo"
	brokerList := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
	lc := logmnr_content{}

	// read from configuration file
	file,_ := ioutil.ReadFile("OracleKafka.Conf.json")
	data := OracleInstances{}
	_ = json.Unmarshall([]byte(file), &data)

	// connectar a oracle
	db, err := sql.Open("goracle", strings.Trim(data[0].Username)+"/"+strings.Trim(data[0].Password)+"@"+strings.Trim(data[0].Hostname)+":"+strings.Trim(data[0].InstancePort)+"/"+strings.Trim(data[0].InstanceName))
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
	var q2 string
	var scn_inicial, scn_final int64
	scn_inicial = int(data[0].LastSCN)
	scn_final = scn_final + 1000000

	//q = `SELECT thread#, scn, start_scn, commit_scn,timestamp, operation_code, operation,status, SEG_TYPE_NAME ,
	//info,seg_owner, table_name, username, sql_redo ,row_id, csf, TABLE_SPACE, SESSION_INFO, RS_ID, RBASQN, RBABLK, SEQUENCE#, TX_NAME, SEG_NAME,
	//SEG_TYPE_NAME
	//FROM  v$logmnr_contents
	//WHERE OPERATION_CODE in (1,2,3) and commit_scn >= `

	p := ProduceMessageAsync(brokerList, topic)

	for {

		q2 = `SELECT scn, sql_redo 	
				FROM  v$logmnr_contents  
				WHERE OPERATION_CODE in (1,2,3) and commit_scn between ` + string(scn_inicial) + ` and ` + string(scn_final)

		rows, err = db.Query(q2)
		if err != nil {
			fmt.Println("Error running query")
			fmt.Println(err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			rows.Scan(&lc.scn, &lc.sql_redo)
			message := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(lc.sql_redo),
			}
			fmt.Println(message.Value)
			p.Input() <- message

		}
		scn_inicial = scn_final + 1
		scn_final = scn_final + 1000000
	}
}

func ProduceMessageAsync(brokerlist []string, topic string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	p, err := sarama.NewAsyncProducer(brokerlist, config)
	if err != nil {
		log.Fatalln("failed to start Sarama Producer:", err)
	}
	go func() {
		for err := range p.Errors() {
			log.Println("failed to write access log entry:", err)
		}
	}()
	return p
}

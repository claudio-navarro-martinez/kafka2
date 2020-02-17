// V$LOGMNR_CONTENTS contains log history information. To query this view, you must have the SELECT ANY TRANSACTION privilege.
//SCN                   System change number (SCN) when the database change was made

//START_SCN     System change number (SCN) when the transaction that contains this change started;
//                              only meaningful if the COMMITTED_DATA_ONLY option was chosen in a DBMS_LOGMNR.START_LOGMNR() invocation, NULL otherwise.
//                              This column may also be NULL if the query is run over a time/SCN range that does not contain the start of the transaction.

package main

import (
        "database/sql"
        "fmt"
        "github.com/Shopify/sarama"
        goracle "github.com/godror/godror"
        "log"
        "strings"
        "time"
)

type lcs struct {
        scn int64
        sql_redo string
}

func main() {
        lc := lcs{}
        var topic string
        var testConStr string
        topic = "foo"
        brokerList := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
        p := ProduceMessageAsync(brokerList, topic)
        P := goracle.ConnectionParams{
                Username:    "SYS AS SYSDBA",
                Password:    "xxxxxxx",
                SID:         "192.168.0.163:21521/dg1",
//                MinSessions: 1,
//              MaxSessions: 1,
//              PoolIncrement: 1,
  //              StandaloneConnection: true,
                WaitTimeout:          10 * time.Second,
                MaxLifeTime:          5 * time.Minute,
                SessionTimeout:       30 * time.Second,
                ConnClass:            "POOLED",
      //          EnableEvents:         true,
        }
        if strings.HasSuffix(strings.ToUpper(P.Username), " AS SYSDBA") {
                P.IsSysDBA, P.Username = true, P.Username[:len(P.Username)-10]
        }
        // P.ConnClass = goracle.NoConnectionPoolingConnectionClass
        testConStr = P.StringWithPassword()
        // testConStr = strings.Replace(testConStr, "POOLED", goracle.NoConnectionPoolingConnectionClass, 1)
        fmt.Println(testConStr)
        db, err := sql.Open("godror", testConStr)
        if err != nil {
                fmt.Println(err,testConStr)
        }

        db.Exec("BEGIN dbms_logmnr.start_logmnr(startscn=>2911151,endscn=>2930588 ,options=>DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+DBMS_LOGMNR.CONTINUOUS_MINE+DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;")

        var q2 string
        q2 = `SELECT scn, sql_redo from v$logmnr_contents a, dba_tables b where a.table_name=b.table_name and a.seg_owner = b.owner and b.owner='TX' `
        rows, err := db.Query(q2)
        if err != nil {
                        fmt.Println("Error running query 2")
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
        db.Close()
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

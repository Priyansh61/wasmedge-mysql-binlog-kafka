use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::providers::mariadb::gtid::gtid_list::GtidList;
use mysql_cdc::providers::mysql::gtid::gtid_set::GtidSet;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;

use std::collections::BTreeMap;
use std::{ thread, time::Duration };
use rskafka::{
    client::{ ClientBuilder, partition::{ Compression, UnknownTopicHandling } },
    record::Record,
};
use chrono::{ TimeZone, Utc };
use rskafka::client::Client;
use rskafka::client::partition::{ OffsetAt, PartitionClient };

struct KafkaProducer {
    client: Client,
    topic: Option<String>,
}

impl KafkaProducer {
    async fn connect(url: String) -> Self {
        KafkaProducer {
            client: ClientBuilder::new(vec![url]).build().await.expect("Couldn't connect to kafka"),
            topic: None,
        }
    }

    // async fn create_topic(&mut self, topic_name: &str) {
    //     let topics = self.client.list_topics().await.unwrap();

    //     for topic in topics {
    //         if topic.name.eq(&topic_name.to_string()) {
    //             self.topic = Some(topic_name.to_string());
    //             println!("Topic already exist in Kafka");
    //             return;
    //         }
    //     }

    //     let controller_client = self.client
    //         .controller_client()
    //         .expect("Couldn't create controller client kafka");
    //     controller_client
    //         .create_topic(
    //             topic_name,
    //             1, // partitions
    //             1, // replication factor
    //             5_000 // timeout (ms)
    //         ).await
    //         .unwrap();
    //     self.topic = Some(topic_name.to_string());
    // }

    async fn create_topic(&mut self, database_name: &str, table_name: &str) {
        let topic_name = format!("{}.{}", database_name, table_name);
        let topics = self.client.list_topics().await.unwrap();

        for topic in topics {
            if topic.name.eq(&topic_name.to_string()) {
                self.topic = Some(topic_name.to_string());
                println!("{}", topic_name);
                println!("Topic already exist in Kafka");
                return
            }
        }

        let controller_client = self.client.controller_client().expect("Couldn't create controller client kafka");
        controller_client.create_topic(
            &topic_name,
            1,      // partitions
            1,      // replication factor
            5_000,  // timeout (ms)
        ).await.unwrap();
        self.topic = Some(topic_name.to_string());
    }

    fn create_record(&self, headers: String, value: String) -> Record {
        Record {
            key: None,
            value: Some(value.into_bytes()),
            headers: BTreeMap::from([("mysql_binlog_headers".to_owned(), headers.into_bytes())]),
            timestamp: Utc.timestamp_millis(42),
        }
    }

    async fn get_partition_client(&self, partition: i32) -> Option<PartitionClient> {
        if self.topic.is_none() {
            ();
        }

        let topic = self.topic.as_ref().unwrap();
        Some(
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry).await
                .expect("Couldn't fetch controller client")
        )
    }
}

fn get_tableName_from_sql_statement(sql_statement: &str) -> String {
    let keywords = vec![
        "from",
        "FROM",
        "join",
        "JOIN",
        "update",
        "UPDATE",
        "into",
        "INTO",
        "table",
        "TABLE",
        "delete",
        "DELETE"
    ];
    let mut table_name = String::new();
    for keyword in keywords {
        if sql_statement.contains(keyword) {
            let sql_statement_split = sql_statement.split(keyword).collect::<Vec<&str>>();
            let sql_statement_split = sql_statement_split[1].split(" ").collect::<Vec<&str>>();
            table_name = sql_statement_split[1].to_string();
            break;
        }
    }
    table_name
}

fn process_binlog_event_get_tablename(event: &BinlogEvent) -> Option<String> {
    match event {
        BinlogEvent::QueryEvent(query_event) => {
            // Find the table_name from the sql_statement in the query_event
            let table_name = get_tableName_from_sql_statement(&query_event.sql_statement);
            println!("table_name: {}", table_name);
            Some(table_name)
        }
        BinlogEvent::RowsQueryEvent(rows_query_event) => {
            // Find the table_name from the sql_statement in the rows_query_event
            let table_name = get_tableName_from_sql_statement(&rows_query_event.query);
            println!("table_name: {}", table_name);
            Some(table_name)
        }
        BinlogEvent::TableMapEvent(query_event) => {
            let table_name = &query_event.table_name;
            Some(table_name.to_string())
        }
        // BinlogEvent::WriteRowsEvent(query_event) => {
        //     println!("WriteRowsEvent");
        //     let table_name = &query_event.table_id;
        //     Some(table_name)
        // }
        // BinlogEvent::UpdateRowsEvent(query_event) => {
        //     println!("UpdateRowsEvent");
        //     let table_name = &query_event.table_id;
        //     Some(table_name)
        // }
        // BinlogEvent::DeleteRowsEvent(query_event) => {
        //     println!("DeleteRowsEvent");
        //     let table_name = &query_event.table_id;
        //     Some(table_name)
        // }
        _ => {
            println!("Event not related to specific table modification");
            None
        }
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), mysql_cdc::errors::Error> {
    println!("Starting mysql binlog kafka");
    std::env::set_var("SLEEP_TIME", "1000");
    std::env::set_var("SQL_USERNAME", "root");
    std::env::set_var("SQL_PASSWORD", "password");
    std::env::set_var("SQL_PORT", "3306");
    std::env::set_var("SQL_HOSTNAME", "localhost");
    std::env::set_var("SQL_DATABASE", "mysql");
    std::env::set_var("KAFKA_URL", "localhost:9092");
    let table_names_env = std::env::var("TABLE_NAMES").unwrap_or_else(|_| String::from("payment"));
    let table_names = table_names_env.split(",").collect::<Vec<&str>>();

    let sleep_time: u64 = std::env::var("SLEEP_TIME").unwrap().parse().unwrap();

    thread::sleep(Duration::from_millis(sleep_time));
    println!("Thread started");

    // // Start replication from MariaDB GTID
    // let _options = BinlogOptions::from_mariadb_gtid(GtidList::parse("0-1-270")?);
    //
    // // Start replication from MySQL GTID
    // let gtid_set =
    //     "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
    // let _options = BinlogOptions::from_mysql_gtid(GtidSet::parse(gtid_set)?);
    //
    // // Start replication from the position
    // let _options = BinlogOptions::from_position(String::from("mysql-bin.000008"), 195);
    //
    // Start replication from last master position.
    // Useful when you are only interested in new changes.
    let options = BinlogOptions::from_end();

    // Start replication from first event of first available master binlog.
    // Note that binlog files by default have expiration time and deleted.
    // let options = BinlogOptions::from_start();

    let username = std::env::var("SQL_USERNAME").unwrap();
    let password = std::env::var("SQL_PASSWORD").unwrap();
    let mysql_port = std::env::var("SQL_PORT").unwrap();
    let mysql_hostname = std::env::var("SQL_HOSTNAME").unwrap();
    let mysql_database = std::env::var("SQL_DATABASE").unwrap();
    let options = ReplicaOptions {
        username,
        password,
        port: mysql_port.parse::<u16>().unwrap(),
        hostname: mysql_hostname,
        database: Some(mysql_database),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);
    println!("Connected to mysql database");

    let kafka_url = std::env::var("KAFKA_URL").unwrap();
    let mut kafka_producer = KafkaProducer::connect(kafka_url).await;
    println!("Connected to kafka server");
    // kafka_producer.create_topic("mysql_binlog_events").await;
    // let partitionClient = kafka_producer.get_partition_client(0).await.unwrap();
    // let mut partition_offset = partitionClient.get_offset(OffsetAt::Latest).await.unwrap();

    for result in client.replicate()? {
        let (header, event) = result?;

        let table_name = process_binlog_event_get_tablename(&event);
        println!("table_name: {:?}", table_name);
        println!("table_names: {:?}", table_names);

        if ( table_name != None && table_names.contains(&table_name.clone().unwrap().as_str()) ) {
            let json_event = serde_json
                ::to_string(&event)
                .expect("Couldn't convert sql event to json");
            let json_header = serde_json
                ::to_string(&header)
                .expect("Couldn't convert sql header to json");
            let table_name_string = table_name.clone().unwrap();

            println!("Header: {}", json_header);
            println!("Event: {}", json_event);
            // let kafka_record = kafka_producer.create_record(json_header,json_event);
            let database_name = "MySQL";
            kafka_producer.create_topic(database_name,&table_name_string).await;
            let topic_name = format!("{}.{}",database_name,&table_name_string);
            let partition_client = kafka_producer.get_partition_client(0)
                .await
                .unwrap_or_else(|| panic!("Couldn't fetch partition client for topic {}",topic_name));

            let mut partition_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();

            let kafka_record = kafka_producer.create_record(json_header, json_event);
            // kafka_producer.create_topic(database_name.as_str(),&table_name.unwrap().as_str()).await;

            // let topic_name = format!("{}.{}",database_name,&table_name.unwrap().as_str());
            // let partitionClient = kafka_producer.get_partition_client(0)
            //     .await
            //     .unwrap_or_else(|| panic!("Couldn't fetch partition client for topic {}",topic_name));

            partition_client.produce(vec![kafka_record], Compression::default()).await.unwrap();

            // Consumer
            let (records, high_watermark) = partition_client
                .fetch_records(
                    partition_offset, // offset
                    1..100_000, // min..max bytes
                    1_000 // max wait time
                ).await
                .unwrap();

            partition_offset = high_watermark;

            for record in records {
                let record_clone = record.clone();
                let timestamp = record_clone.record.timestamp;
                let value = record_clone.record.value.unwrap();
                let header = record_clone.record.headers
                    .get("mysql_binlog_headers")
                    .unwrap()
                    .clone();

                println!(
                    "============================================== Event from Apache kafka =========================================================================="
                );
                println!();
                println!("Value: {}", String::from_utf8(value).unwrap());
                println!("Timestamp: {}", timestamp);
                println!("Headers: {}", String::from_utf8(header).unwrap());
                println!();
                println!();
            }

            // After you processed the event, you need to update replication position
            client.commit(&header, &event);
        }
    }
Ok(())
}

# wasmedge-mysql-binlog-kafka
Sends binlog json events to apache kafka

## Objective :

- It only processes binlogs for certain tables from the MySQL database. The table names can be passed in as an environment variable or be hardcoded in a Vec in the code.
- It sends binlogs from different tables into different topics in the Kafka queue. The queue naming convention is $databaseName_$tableName.

## Thought Process & Implementation :

### How do we only process binlogs from certain tables only ?

- After carefully going through the documentation of mysql_cdc I figured out that we can take out the information from certain Binlog events.

- In general, we have the following Binlog events 
  - Write Rows Event
  - Update Rows Event
  - Delete Rows Event
  - Query Event
  - TableMap Event
  - XID Event
  - Format Description
  - HeartBeat Event
  - And Many more
- We dont need all of them, neither all of them contain specific information about the table.
  - we will be using the following events only currently to filter the binlogs
    - Query Event
    - RowQuery Event
    - Write Row Event
    - Delete Row Event
    - TableMap Event
    - UpdateRow Event
  - This will enable us to get information about the table
- We process all the binlogs through the given funtion 

``` rust
fn process_binlog_event_get_tablename(event: &BinlogEvent) -> Option<String> {
    match event {
        BinlogEvent::QueryEvent(query_event) => {
            // Find the table_name from the sql_statement in the query_event
            let table_name = get_tableName_from_sql_statement(&query_event.sql_statement);
            println!("Event::QueryEvent and table_name: {}", table_name);
            Some(table_name)
        }
        BinlogEvent::RowsQueryEvent(rows_query_event) => {
            // Find the table_name from the sql_statement in the rows_query_event
            let table_name = get_tableName_from_sql_statement(&rows_query_event.query);
            println!("Event::RowsQueryEvent and table_name: {}", table_name);
            Some(table_name)
        }
        BinlogEvent::TableMapEvent(query_event) => {
            let table_name = &query_event.table_name;
            println!("Event::TableMapEvent and table_name: {}", table_name);
            Some(table_name.to_string())
        }
        BinlogEvent::WriteRowsEvent(query_event) => {
            println!("WriteRowsEvent");
            let table_name = &query_event.table_name;
            Some(table_name.to_string())
        }
        BinlogEvent::UpdateRowsEvent(query_event) => {
            println!("UpdateRowsEvent");
            let table_name = &query_event.table_name;
            Some(table_name.to_string())
        }
        BinlogEvent::DeleteRowsEvent(query_event) => {
            println!("DeleteRowsEvent");
            let table_name = &query_event.table_name;
            Some(table_name.to_string())
        }
        _ => {
            println!("Event not related to specific table modification");
            None
        }
    }
}
  
```

``` rust
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
  ```
- This is how we take out the name from the Binlog Event. But wait, to filter events such as WriteRow Event, UpdateRow Event do not have table_name defined, they just have table_id. This can be achieved by using the TableMap event used in their crates to get the table name, I have modified the files to incorporate the table_name in themselves.
- We have stored the tablenames to filter in the env as shown below

``` rust
let table_names_env = std::env::var("TABLE_NAMES").unwrap_or_else(|_| String::from("payment"));
    let table_names = table_names_env.split(",").collect::<Vec<&str>>();
```


- Now when processing the event,header we check whether we need to proceed the given binlog event or not


``` rust
for result in client.replicate()? {
        let (header, event) = result?;

        let table_name = process_binlog_event_get_tablename(&event);


        if ( table_name != None && table_names.contains(&table_name.clone().unwrap().as_str()) ) {
            let json_event = serde_json
                ::to_string(&event)
                .expect("Couldn't convert sql event to json");
            let json_header = serde_json
                ::to_string(&header)
                .expect("Couldn't convert sql header to json");
            let table_name_string = table_name.clone().unwrap();
```

## How do we store the various binlog events in different kafka topics ?

- This can be done easily by using the given format, we will be creating different topics for {database_name}.{table_name} and if the name is found, we store it else we create a topic and store the log in the queue.

``` rust
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
```

- Now with the topic created, let's take a look at the flow

  - We get the partition_client.
  - We need to get the offset to read from the partion_client.
  - We read the record from the given offset in the db.table topic in the kafka.

``` rust
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


            let database_name = mysql_database;
            kafka_producer.create_topic(database_name,&table_name_string).await;
            let topic_name = format!("{}.{}",database_name,&table_name_string);
            let partition_client = kafka_producer.get_partition_client(0)
                .await
                .unwrap_or_else(|| panic!("Couldn't fetch partition client for topic {}",topic_name));

            let mut partition_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();

            let kafka_record = kafka_producer.create_record(json_header, json_event);


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
```

## Let's Test our Code 

We will be using the same insert.wasm and changing the table_name for which we need to filter the logs.
### Checking for the Payment table :
  
The logs in the workflows
  
![image](https://github.com/Priyansh61/wasmedge-mysql-binlog-kafka/assets/96513964/de7ddfac-13dd-4ad7-8b7a-a6f45efa8015)
  
### Checking for the Customer Table:
  
![image](https://github.com/Priyansh61/wasmedge-mysql-binlog-kafka/assets/96513964/cdc3767b-a198-4489-a06c-0296c4b82712)

As we can see from the above screenshot, that no logs are generated when we check for the customer, since we have not invoked any change related to the customer table.
  
  
## Current Drawbacks of the approach :
  
- Currently we are only able to get the binlogs of the certain events such as
  - Write Rows Event
  - Update Rows Event
  - Delete Rows Event
  - Query Event
- So whenever we change a table there is not just a single binlog event which happens, there are various events that happens in between.
- We have int_var event, rand event, xid event which happens when a transaction is finally commited.
- But we are not able to keep a track of these in current scenario, we are just taking event which directly relate to the event, not the ones which are indirectly generated due to the changes.
  
## Possible Solution :
I have come up with a proposed solution, we will be starting the log track on a generation of query event, then using the header in the binlog we will check for the next binlog. This will keep on happening untill we reach the commit transaction (xid event). This way we will be able to keep track of all the changes related to a table not just the Query Event.

## NOTE FOR MENTORS :
I really enjoyed working on this and had so much fun last few days. Your reviews on the above approach would be really helpful I would like to know whether we can do this, or if there's a better way to achieve the same thing.

Additionaly, please let me know about the drawback I have mentioned regarding the approach. And if my proposed solution is the right way to get it resolveed 😄

THANKS
  
  





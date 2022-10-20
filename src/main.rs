use futures::{
    channel::mpsc::{channel, Receiver},
    SinkExt, StreamExt,
};
use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use notify::event::{AccessKind, ModifyKind};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{path::Path, time::Duration};
use std::{
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom},
};

// use yaml_rust::{YamlLoader, YamlEmitter};

/// Async, futures channel based event watching
fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("Argument 1 needs to be a path");
    println!("watching {}", path);

    futures::executor::block_on(async {
        if let Err(e) = async_watch(path).await {
            println!("error: {:?}", e)
        }
    });
}

fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<Event>>)> {
    let (mut tx, rx) = channel(1);

    // Automatically select the best implementation for your platform.
    // You can also access each implementation directly e.g. INotifyWatcher.
    let watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                tx.send(res).await.unwrap();
            })
        },
        Config::default(),
    )?;

    Ok((watcher, rx))
}

async fn async_watch<P: AsRef<Path>>(path: P) -> notify::Result<()> {
    let (mut watcher, mut rx) = async_watcher()?;

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    let path = path.as_ref();
    watcher.watch(path, RecursiveMode::NonRecursive)?;

    let mut buffer = String::new();
    let file = File::open(&path).unwrap();
    let f_size = file.metadata().expect("msg").len();
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(f_size)).expect("seek fail");

    while let Some(res) = rx.next().await {
        match res {
            Ok(event) => {
                match event.kind {
                    EventKind::Modify(ModifyKind::Data(_)) => {
                        reader.read_line(&mut buffer).expect("read fail");
                        println!("{:?}", buffer);
                        buffer.clear();
                    }
                    EventKind::Access(AccessKind::Close(_)) => {
                        watcher.unwatch(path).expect("unwatch fail");
                        while !path.exists() {}
                        watcher
                            .watch(path, RecursiveMode::NonRecursive)
                            .expect("rewatch fail");
                        reader = BufReader::new(File::open(&path).unwrap());
                    }
                    _ => {
                        // ignore
                        println!("changed: {:?}", event);
                    }
                }
            }
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    Ok(())
}

fn produce_message<'a, 'b>(
    data: &'a [u8],
    topic: &'b str,
    brokers: Vec<String>,
) -> Result<(), KafkaError> {
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer = Producer::from_hosts(brokers)
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::One)
        // ~ build the producer with the above settings
        .create()?;

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.
    producer.send(&Record {
        topic,
        partition: -1,
        key: (),
        value: data,
    })?;

    // ~ we can achieve exactly the same as above in a shorter way with
    // the following call
    producer.send(&Record::from_value(topic, data))?;

    Ok(())
}

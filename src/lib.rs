use actors::base::Actor;
use aggregates::redis::{Redis, RedisInsert, RedisQuery};
use bastion::{
    prelude::{Distributor, Message, SendError},
    run,
};
use log::{error, info};

pub mod actors;
pub mod aggregates;

pub fn init_redis(urls: Vec<String>) -> Actor<Redis> {
    let __redis_aggr = Redis {
        urls,
        ..Default::default()
    };

    let _redis_actor = Actor::<Redis>::builder()
        .with_state_inner(__redis_aggr)
        .run()
        .unwrap();

    _redis_actor
}

pub fn insert(key: String, value: Vec<u8>) {
    match Distributor::named("redis_actor").tell_one(RedisInsert { key, value }) {
        Ok(_) => {
            info!("insert ok");
        }
        Err(e) => {
            error!("insert error: {:?}", e);
        }
    };
}

pub fn query(key: String) -> Vec<u8> {
    let reply: Result<Vec<u8>, SendError> = run!(async {
        Distributor::named("redis_actor")
            .request(RedisQuery { key })
            .await
            .expect("couldn't receive reply")
    });
    reply.unwrap()
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        init_redis(vec!["redis://127.0.0.1:30006".to_owned()]);
        sleep(Duration::from_secs(5));
        let expected = "hi".to_owned();
        insert("hello".to_owned(), expected.as_bytes().to_vec());

        let query = query("hello".to_owned());

        let res = String::from_utf8(query).unwrap();
        assert_eq!(expected, res);
    }
}

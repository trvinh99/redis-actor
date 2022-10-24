/// Module for command definition.
mod command;
/// Module for error definition.
mod error;
/// Module for event definition.
mod event;

use anyhow::Result;
use async_trait::async_trait;
use bastion::{
    prelude::{BastionContext, Distributor, MessageHandler},
    run,
    supervisor::{ActorRestartStrategy, RestartPolicy, RestartStrategy},
};
use core::fmt::Debug;
use cqrs_es::Aggregate;
use log::{info, warn};
use r2d2::ManageConnection;
use redis::{
    cluster::{ClusterClientBuilder, ClusterConnection},
    Commands, ConnectionLike, RedisError,
};
use serde::{Deserialize, Serialize};
use std::io;

use crate::actors::base::TActor;

use self::{command::RedisCommand, event::RedisEvent};

/// Redis actor to manage redis state, store urls of cluster nodes.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Redis {
    pub state: RedisState,
    pub urls: Vec<String>,
    pub redis_auth: RedisAuth,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RedisAuth {
    #[default]
    None,
    Userpass {
        username: String,
        password: String,
    },
}

/// Redis state to manage the state of the redis connection.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RedisState {
    #[default]
    Uninitialized,
    Initialized,
}

impl Redis {
    // Returns a current redis state
    fn get_state(&self) -> RedisState {
        self.state.clone()
    }

    // Returns list urls of the cluster nodes
    fn get_urls(&self) -> Vec<String> {
        self.urls.clone()
    }
}

/// Command for delete a key
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RedisDelete {
    pub key: String,
}

impl RedisDelete {
    pub fn key(&self) -> String {
        self.key.clone()
    }
}

/// Command for query value by key.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RedisQuery {
    pub key: String,
}

impl RedisQuery {
    pub fn key(&self) -> String {
        self.key.clone()
    }
}

/// Command for insert a pair of key and value.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RedisInsert {
    pub key: String,
    pub value: Vec<u8>,
    pub expire_time: Option<usize>,
}

impl RedisInsert {
    pub fn new(key: String) -> Self {
        Self {
            key,
            ..Default::default()
        }
    }
}

/// Implement Aggregate trait for Redis Aggregate
#[async_trait]
impl Aggregate for Redis {
    type Command = RedisCommand;

    type Event = RedisEvent;

    type Error = RedisError;

    type Services = ();

    fn aggregate_type() -> String {
        "redis".to_owned()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let mut events = vec![];
        match command {
            RedisCommand::ReconnectRedisServer { urls } => {
                events.push(RedisEvent::RedisServerReconnected { urls });
            }
            RedisCommand::ConnectRedisServer { urls } => {
                events.push(RedisEvent::RedisServerConnected { urls });
            }
        }
        Ok(events)
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            RedisEvent::RedisServerConnected { urls } => {
                self.state = RedisState::Initialized;
                self.urls = urls;
            }
            RedisEvent::RedisServerReconnected { urls } => {
                self.urls = urls;
            }
        }
    }
}

impl ManageConnection for Redis {
    type Connection = ClusterConnection;

    type Error = redis::RedisError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = ClusterClientBuilder::new(self.get_urls())
            .build()
            .unwrap()
            .get_connection();
        conn
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), redis::RedisError> {
        if conn.check_connection() {
            info!("Check conn: true");
            Ok(())
        } else {
            Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.is_open()
    }
}

#[async_trait]
impl TActor for Redis {
    fn with_distributor() -> Option<Distributor> {
        Some(Distributor::named("redis_actor"))
    }

    /// Actor behaviours if it failed
    fn with_restart_strategy() -> Option<RestartStrategy> {
        Some(RestartStrategy::new(
            RestartPolicy::Always,
            ActorRestartStrategy::Immediate,
        ))
    }

    async fn handler(&mut self, ctx: BastionContext) -> Result<(), ()> {
        let pool = r2d2::Pool::builder()
            .max_size(15)
            .build(self.clone())
            .unwrap();

        let mut conn = pool.get().unwrap();

        info!("Pool state: {:?}", pool.state());

        Distributor::named("redis_actor")
            .tell_one(RedisCommand::ConnectRedisServer {
                urls: self.get_urls(),
            })
            .unwrap();

        loop {
            MessageHandler::new(ctx.recv().await?)
                .on_tell(|command: RedisCommand, _| {
                    run!(async {
                        let events = self.handle(command, &()).await.unwrap();
                        for e in events {
                            Distributor::named("redis_actor").tell_one(e).unwrap();
                        }
                    });
                })
                .on_tell(|event: RedisEvent, _| {
                    self.apply(event.clone());
                    run!(async {
                        match event {
                            RedisEvent::RedisServerReconnected { urls: _ } => {
                                let pool = r2d2::Pool::builder()
                                    .max_size(15)
                                    .build(self.clone())
                                    .unwrap();

                                conn = pool.get().unwrap();
                            }
                            RedisEvent::RedisServerConnected { urls: _ } => {}
                        }
                    });
                })
                .on_question(|event: RedisQuery, sender| {
                    if let RedisState::Initialized = self.get_state() {
                        let result: Result<Vec<u8>, RedisError> = conn.get(event.key);

                        if let Ok(data) = result {
                            sender.reply(data).expect("cannot reply");
                        }
                    }
                })
                .on_tell(|event: RedisInsert, _| {
                    if let RedisState::Initialized = self.get_state() {
                        let _: Result<(), RedisError> = conn.set(event.key.clone(), event.value);
                        match event.expire_time {
                            Some(exp) => {
                                let _: Result<(), RedisError> = conn.expire(event.key, exp);
                            }
                            None => {}
                        };
                    }
                })
                .on_tell(|event: RedisDelete, _| {
                    if let RedisState::Initialized = self.get_state() {
                        let _: Result<(), RedisError> = conn.del(event.key);
                    }
                })
                .on_fallback(|unknown, _| warn!("[REDIS] Unknown message: {unknown:?}"));
        }
    }
}

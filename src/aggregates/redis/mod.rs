use anyhow::Result;
use async_trait::async_trait;
use bastion::{
    prelude::{BastionContext, Distributor, MessageHandler, AnswerSender, Message},
    run,
    supervisor::{ActorRestartStrategy, RestartPolicy, RestartStrategy},
};
use core::fmt::Debug;
use cqrs_es::Aggregate;
use log::warn;
use redis::{cluster::{ClusterClientBuilder, ClusterConnection}, Commands, FromRedisValue, RedisError, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::actors::base::TActor;

use self::{command::RedisCommand, event::RedisEvent};

mod command;
mod error;
mod event;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Redis {
    pub state: RedisState,
    pub urls: Vec<String>,
}

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

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RedisQuery {
    pub key: String,
}

impl RedisQuery {
    pub fn key(&self) -> String {
        self.key.clone()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RedisInsert {
    pub key: String,
    pub value: Vec<u8>,
}

impl RedisInsert
{
    pub fn new(key: String) -> Self {
        Self {
            key,
            ..Default::default()
        }
    }
}

/// Implement Aggregate trait for Redis Aggregate
#[async_trait]
impl Aggregate for Redis
{
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

#[async_trait]
impl TActor for Redis
{
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
        let mut conn = ClusterClientBuilder::new(self.get_urls())
            .build()
            .unwrap()
            .get_connection()
            .unwrap();

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
                            RedisEvent::RedisServerReconnected { urls } => {
                                conn = ClusterClientBuilder::new(urls)
                                    .build()
                                    .unwrap()
                                    .get_connection()
                                    .unwrap();
                            }
                            RedisEvent::RedisServerConnected { urls: _ } => {}
                        }
                    });
                })
                .on_question(|event: RedisQuery, sender| {
                    if let RedisState::Initialized = self.get_state() {
                        let result: Vec<u8> = conn.get(event.key).unwrap();
                        sender.reply(result).expect("cannot reply");                    }
                })
                .on_tell(|event: RedisInsert, _| {
                    if let RedisState::Initialized = self.get_state() {
                        let _: () = conn.set(event.key, event.value).unwrap();
                    }
                })
                .on_fallback(|unknown, _| warn!("[REDIS] Unknown message: {unknown:?}"));
        }
    }
}
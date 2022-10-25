use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

/// Events for redis actor
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RedisEvent {
  RedisServerReconnected { urls: Vec<String> },
  RedisServerConnected { urls: Vec<String> },
}

impl DomainEvent for RedisEvent {
  fn event_type(&self) -> String {
    match self {
      RedisEvent::RedisServerReconnected { urls } => {
        format!("Redis reconnect to cluster server: {:?}", urls)
      }

      RedisEvent::RedisServerConnected { urls } => {
        format!("Redis connect to cluster server: {:?}", urls)
      }
    }
  }

  fn event_version(&self) -> String {
    "1.0".to_owned()
  }
}

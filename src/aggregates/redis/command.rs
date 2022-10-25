/// Commands for redis actor
#[derive(Debug)]
pub enum RedisCommand {
  ReconnectRedisServer { urls: Vec<String> },
  ConnectRedisServer { urls: Vec<String> },
}

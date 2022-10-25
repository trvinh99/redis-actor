use std::{
  ops::{Deref, DerefMut},
  sync::{Arc, Weak},
};

use tokio::sync::RwLock;

/// Alias for inner strong state
type Inner<S> = Arc<RwLock<S>>;
/// Alias for inner weak state
type InnerWeak<S> = Weak<RwLock<S>>;

/// State wrapper to use in actor handler
#[derive(Debug)]
pub struct State<S>(Inner<S>);

impl<S> Default for State<S>
where
  S: Default,
{
  fn default() -> Self {
    Self(Arc::new(RwLock::new(S::default())))
  }
}

impl<S> Deref for State<S> {
  type Target = Inner<S>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<S> DerefMut for State<S> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<S> State<S> {
  /// Init new State wrapper
  pub fn init(inner: S) -> Self {
    Self(Arc::new(RwLock::new(inner)))
  }

  /// Downgrade to weak
  pub fn downgrade(&self) -> WeakState<S> {
    WeakState(Arc::downgrade(self))
  }

  /// Upgrade from weak
  pub fn upgrade(weak: WeakState<S>) -> Self {
    match weak.0.upgrade() {
      Some(state) => Self(state),
      None => panic!("Upgrade state failed!"),
    }
  }
}

/// Weak state being used when actor handler call it
#[derive(Debug)]
pub struct WeakState<S>(Weak<RwLock<S>>);

impl<S> Deref for WeakState<S> {
  type Target = InnerWeak<S>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<S> Clone for WeakState<S> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

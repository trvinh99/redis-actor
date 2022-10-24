/// Actor state (wrap aggregates or data structs)
pub mod state;

use std::{ops::Deref, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use bastion::{
    prelude::{BastionContext, Dispatcher, Distributor},
    resizer::OptimalSizeExploringResizer,
    supervisor::{RestartStrategy, SupervisionStrategy, SupervisorRef},
    Bastion, Callbacks,
};

use state::State;

/// TActor to be implemented for states
#[async_trait]
pub trait TActor: Default + Send + Sync + 'static {
    /// Main handler for actor
    async fn handler(&mut self, ctx: BastionContext) -> Result<(), ()>;

    // For supervisor

    /// Callbacks for the supervisor
    fn with_supervisor_callbacks() -> Option<Callbacks> {
        None
    }
    /// Supervision strategy for the supervisor
    fn with_stategy() -> Option<SupervisionStrategy> {
        None
    }
    /// Restart strategy for the supervisor
    fn with_restart_strategy() -> Option<RestartStrategy> {
        None
    }
    // For children

    /// Callbacks for the supervisor's children
    fn with_children_callbacks() -> Option<Callbacks> {
        None
    }

    /// Dispatcher for children
    fn with_dispatcher() -> Option<Dispatcher> {
        None
    }

    /// Distributor for children
    fn with_distributor() -> Option<Distributor> {
        None
    }

    /// Heartbeat interval
    fn with_heartbeat_tick() -> Option<Duration> {
        None
    }

    /// Children group name
    fn with_name() -> Option<String> {
        None
    }

    /// Number of child in children group
    fn with_redundancy() -> Option<usize> {
        None
    }

    /// Resizer (for scalability)
    fn with_resizer() -> Option<OptimalSizeExploringResizer> {
        None
    }
}

/// Core actor with state inside that implemented TActor
#[derive(Debug)]
pub struct Actor<S> {
    __supervisor: SupervisorRef,
    state: State<S>,
}

impl<S> Actor<S>
where
    S: TActor,
{
    /// Return actor builder
    pub fn builder() -> ActorBuilder<S> {
        ActorBuilder::default()
    }
}

impl<S> Deref for Actor<S> {
    type Target = State<S>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

/// Actor builder with config settings for building new actor
pub struct ActorBuilder<S> {
    supervisor: SupervisorRef,
    state: State<S>,
    // For supervisor
    supervisor_callbacks: Option<Callbacks>,
    restart_strategy: Option<RestartStrategy>,
    strategy: Option<SupervisionStrategy>,
    // For children
    children_callbacks: Option<Callbacks>,
    dispatcher: Option<Dispatcher>,
    distributor: Option<Distributor>,
    heartbeat_tick: Option<Duration>,
    name: Option<String>,
    redundancy: Option<usize>,
    resizer: Option<OptimalSizeExploringResizer>,
}

impl<S> Default for ActorBuilder<S>
where
    S: TActor,
{
    /// Default actor builder
    fn default() -> Self {
        Self {
            supervisor: Bastion::supervisor(|sp| sp).unwrap(),
            state: State::<S>::default(),
            supervisor_callbacks: Default::default(),
            restart_strategy: Default::default(),
            strategy: Default::default(),
            children_callbacks: Default::default(),
            dispatcher: Default::default(),
            distributor: Default::default(),
            heartbeat_tick: Default::default(),
            name: Default::default(),
            redundancy: Default::default(),
            resizer: Default::default(),
        }
    }
}

impl<S> ActorBuilder<S>
where
    S: TActor,
{
    /// Init actor with particular supervisor
    pub fn with_parent(mut self, sp: SupervisorRef) -> Self {
        self.supervisor = sp;
        self
    }

    /// Inner value for actor
    pub fn with_state_inner(mut self, state_inner: S) -> Self {
        self.state = State::init(state_inner);
        self
    }

    /// State for actor
    pub fn with_state(mut self, state: State<S>) -> Self {
        self.state = state;
        self
    }

    /// Run actor
    pub fn run(self) -> Result<Actor<S>> {
        // Create supervisor behaviour
        let supervisor = self
            .supervisor
            .supervisor(|mut sp| {
                if let Some(ref callbacks) = self.supervisor_callbacks {
                    sp = sp.with_callbacks(callbacks.clone());
                }
                if let Some(ref restart_strategy) = self.restart_strategy {
                    sp = sp.with_restart_strategy(restart_strategy.clone());
                }
                if let Some(ref strategy) = self.strategy {
                    sp = sp.with_strategy(strategy.clone());
                }
                sp
            })
            .unwrap();

        // Because state does not implement Copy trait and Fn cannot handle mutable value so pure state S must be wrap by State<S>
        let state = self.state;
        let weak_state = state.downgrade();

        // Create children behaviour
        supervisor
            .children(|mut children| {
                // Implements config from TActor
                if let Some(callbacks) = S::with_children_callbacks() {
                    children = children.with_callbacks(callbacks);
                }
                if let Some(dispatcher) = S::with_dispatcher() {
                    children = children.with_dispatcher(dispatcher);
                }
                if let Some(distributor) = S::with_distributor() {
                    children = children.with_distributor(distributor);
                }
                if let Some(interval) = S::with_heartbeat_tick() {
                    children = children.with_heartbeat_tick(interval);
                }
                if let Some(name) = S::with_name() {
                    children = children.with_name(name);
                }
                if let Some(redundancy) = S::with_redundancy() {
                    children = children.with_redundancy(redundancy);
                }
                if let Some(resizer) = S::with_resizer() {
                    children = children.with_resizer(resizer);
                }

                // Keeps default setting if not all of above configs are not fulfilled
                if let Some(callbacks) = self.children_callbacks {
                    children = children.with_callbacks(callbacks);
                }
                if let Some(dispatcher) = self.dispatcher {
                    children = children.with_dispatcher(dispatcher);
                }
                if let Some(distributor) = self.distributor {
                    children = children.with_distributor(distributor);
                }
                if let Some(interval) = self.heartbeat_tick {
                    children = children.with_heartbeat_tick(interval);
                }
                if let Some(name) = self.name {
                    children = children.with_name(name);
                }
                if let Some(redundancy) = self.redundancy {
                    children = children.with_redundancy(redundancy);
                }
                if let Some(resizer) = self.resizer {
                    children = children.with_resizer(resizer);
                }

                // Main handler, this one will be called if the actor is down by error or crash
                children.with_exec(move |ctx| {
                    let weak_state = weak_state.clone();
                    let state = State::upgrade(weak_state);
                    async move {
                        {
                            let mut write = state.write().await;
                            write.handler(ctx).await
                        }
                    }
                })
            })
            .unwrap();

        Ok(Actor {
            __supervisor: supervisor,
            state,
        })
    }

    // For supervisor

    /// Callbacks for the supervisor
    pub fn with_supervisor_callbacks(mut self, callbacks: Callbacks) -> Self {
        self.supervisor_callbacks = Some(callbacks);
        self
    }
    /// Supervision strategy for the supervisor
    pub fn with_stategy(mut self, strategy: SupervisionStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }
    /// Restart strategy for the supervisor
    pub fn with_restart_strategy(mut self, restart_strategy: RestartStrategy) -> Self {
        self.restart_strategy = Some(restart_strategy);
        self
    }
    // For children

    /// Callbacks for the supervisor's children
    pub fn with_children_callbacks(mut self, callbacks: Callbacks) -> Self {
        self.children_callbacks = Some(callbacks);
        self
    }

    /// Dispatcher for children
    pub fn with_dispatcher(mut self, dispatcher: Dispatcher) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    /// Distributor for children
    pub fn with_distributor(mut self, distributor: Distributor) -> Self {
        self.distributor = Some(distributor);
        self
    }

    /// Heartbeat interval
    pub fn with_heartbeat_tick(mut self, interval: Duration) -> Self {
        self.heartbeat_tick = Some(interval);
        self
    }

    /// Children group name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Number of child in children group
    pub fn with_redundancy(mut self, redundancy: usize) -> Self {
        self.redundancy = Some(redundancy);
        self
    }

    /// Resizer (for scalability)
    pub fn with_resizer(mut self, resizer: OptimalSizeExploringResizer) -> Self {
        self.resizer = Some(resizer);
        self
    }
}

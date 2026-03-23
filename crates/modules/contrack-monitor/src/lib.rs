use chrono::{DateTime, Utc};
use pulsar_core::event::{Event, Host, Payload};
use pulsar_core::pdk::{ModuleContext, ModuleError, NoConfig, SimplePulsarModule};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

const MODULE_NAME: &str = "contrack-monitor";

#[derive(Debug)]
pub struct TransactionKey {
    source: Host,
    destination: Host,
}

impl TransactionKey {
    fn from(source: &Host, destination: &Host) -> Self {
        TransactionKey {
            source: source.clone(),
            destination: destination.clone(),
        }
    }
}

impl Display for TransactionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}:{} -> {}:{}",
            self.source.ip, self.source.port, self.destination.ip, self.destination.port
        ))
    }
}

impl PartialEq<Self> for TransactionKey {
    fn eq(&self, other: &Self) -> bool {
        self.source.ip.eq(&other.source.ip) &&
        self.source.port.eq(&other.source.port) &&
        self.destination.ip.eq(&other.destination.ip) &&
        self.destination.port.eq(&other.destination.port)
    }
}
impl Eq for TransactionKey {}

impl Hash for TransactionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.ip.hash(state);
        self.source.port.hash(state);

        self.destination.ip.hash(state);
        self.destination.port.hash(state);
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct TransactionStats {
    sent: usize,
    received: usize,
    start: SystemTime,
    // todo : add pid/image/comm who interacted with connection
}

impl TransactionStats {
    fn new() -> Self {
        TransactionStats {
            sent: 0,
            received: 0,
            start: SystemTime::now(),
        }
    }
}

impl Display for TransactionStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "sent={}, received={}, started_on={}",
            self.sent,
            self.received,
            pretty_timestamp(self.start)
        ))
    }
}

pub enum TransactionState {
    NEW,
    CLOSED,
}

impl Display for TransactionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::NEW => f.write_fmt(format_args!("\x1b[32mNEW_TRANSACTION\x1b[0m")),
            TransactionState::CLOSED => {
                f.write_fmt(format_args!("\x1b[31mCLOSED_TRANSACTION\x1b[0m"))
            }
        }
    }
}

pub struct ContrackModule;

pub struct ContrackModuleState {
    contrack: Arc<Mutex<HashMap<TransactionKey, TransactionStats>>>,
}

impl SimplePulsarModule for ContrackModule {
    type Config = NoConfig;
    type State = ContrackModuleState;

    const MODULE_NAME: &'static str = MODULE_NAME;
    const DEFAULT_ENABLED: bool = true;

    async fn init_state(
        &self,
        _config: &Self::Config,
        _ctx: &ModuleContext,
    ) -> Result<Self::State, ModuleError> {
        Ok(Self::State {
            contrack: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn on_event(
        event: &Event,
        _config: &Self::Config,
        state: &mut Self::State,
        _ctx: &ModuleContext,
    ) -> Result<(), ModuleError> {
        match event.payload() {
            Payload::Accept {
                source,
                destination,
            } => {
                let key = TransactionKey::from(source, destination);
                let mut lock = state.contrack.lock().await;

                if !lock.contains_key(&key) {
                    log::info!("{} {}", generate_prefix(event, TransactionState::NEW), &key);
                    lock.insert(key, TransactionStats::new());
                }

                drop(lock);
            }
            Payload::Receive {
                source,
                destination,
                len,
                ..
            } => {
                let key = TransactionKey::from(source, destination);
                let mut lock = state.contrack.lock().await;

                lock
                    .entry(key)
                    .or_insert(TransactionStats::new())
                    .received += len;

                drop(lock);
            }
            Payload::Send {
                source,
                destination,
                len,
                ..
            } => {
                let key = TransactionKey::from(source, destination);
                let mut lock = state.contrack.lock().await;

                lock
                    .entry(key)
                    .or_insert(TransactionStats::new())
                    .sent += len;

                drop(lock);
            }
            Payload::Close {
                source,
                destination,
            } => {
                let key = TransactionKey::from(source, destination);
                let mut lock = state.contrack.lock().await;

                match lock.get_mut(&key) {
                    Some(s) => {
                        log::info!("{} {} - {}",
                            generate_prefix(event, TransactionState::CLOSED), &key, s
                        );
                        lock.remove(&key);
                    }
                    None => (),
                }
                drop(lock);
            }
            _ => (),
        }

        // TODO : add a cleanup every x amount of time to clear old connections

        // TODO : implement special requests to query the module about active connections

        Ok(())
    }
}

fn pretty_timestamp(timestamp: SystemTime) -> String {
    DateTime::<Utc>::from(timestamp)
        .format("%Y-%m-%dT%TZ")
        .to_string()
}

fn generate_prefix(event: &Event, transaction_state: TransactionState) -> String {
    let header = event.header();
    let pid = header.pid;
    let comm = &header.comm;
    let image = &header.image;

    format!("{transaction_state} {pid}:{comm} {image} >")
}

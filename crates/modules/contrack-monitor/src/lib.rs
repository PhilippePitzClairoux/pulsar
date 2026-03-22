use chrono::{DateTime, Utc};
use pulsar_core::event::{Event, Host, Payload};
use pulsar_core::pdk::{ModuleContext, ModuleError, NoConfig, SimplePulsarModule};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

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
        self.source.ip.eq(&other.source.ip) && self.source.port.eq(&other.source.port) //&&
        // self.destination.ip.eq(&other.destination.ip) &&
        // self.destination.port.eq(&other.destination.port)
    }
}
impl Eq for TransactionKey {}

impl Hash for TransactionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.ip.hash(state);
        self.source.port.hash(state);

        // self.destination.ip.hash(state);
        // self.destination.port.hash(state);
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
    contrack: HashMap<TransactionKey, TransactionStats>,
}

impl SimplePulsarModule for ContrackModule {
    type Config = NoConfig;
    type State = ContrackModuleState;

    const MODULE_NAME: &'static str = "proxy-module";
    const DEFAULT_ENABLED: bool = true;

    async fn init_state(
        &self,
        _config: &Self::Config,
        _ctx: &ModuleContext,
    ) -> Result<Self::State, ModuleError> {
        Ok(Self::State {
            contrack: HashMap::new(),
        })
    }

    async fn on_event(
        event: &Event,
        _config: &Self::Config,
        state: &mut Self::State,
        _ctx: &ModuleContext,
    ) -> Result<(), ModuleError> {
        let pid = event.header().pid;

        match event.payload() {
            Payload::Accept {
                source,
                destination,
            } => {
                let key = TransactionKey::from(source, destination);
                log::info!("{} {}", generate_prefix(event, TransactionState::NEW), &key);
                state
                    .contrack
                    .get_mut(&key)
                    .get_or_insert(&mut TransactionStats::new());
            }
            Payload::Receive {
                source,
                destination,
                len,
                ..
            } => {
                let key = TransactionKey::from(source, destination);
                state
                    .contrack
                    .get_mut(&key)
                    .get_or_insert(&mut TransactionStats::new())
                    .received += len;
            }
            Payload::Send {
                source,
                destination,
                len,
                ..
            } => {
                let key = TransactionKey::from(source, destination);
                state
                    .contrack
                    .get_mut(&key)
                    .get_or_insert(&mut TransactionStats::new())
                    .sent += len;
            }
            Payload::Close {
                source,
                destination,
            } => {
                log::info!(
                    "{} {} {}",
                    generate_prefix(event, TransactionState::CLOSED),
                    source,
                    destination
                );

                let key = TransactionKey::from(source, destination);
                match state.contrack.get_mut(&key) {
                    Some(s) => {
                        log::info!(
                            "{} {} - {}",
                            generate_prefix(event, TransactionState::CLOSED),
                            &key,
                            s
                        );
                        state.contrack.remove(&key);
                    }
                    None => (),
                }
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

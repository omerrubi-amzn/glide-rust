// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Client configuration.
//!
//! Mirrors the Python `glide_shared.config` module. [`GlideClientConfiguration`]
//! (standalone) and [`GlideClusterClientConfiguration`] (cluster) are builder
//! structs that lower into a `glide_core::client::ConnectionRequest`.

use glide_core::client::{
    AuthenticationInfo, ConnectionRequest, ConnectionRetryStrategy, IamAuthenticationConfig,
    NodeAddress as CoreNodeAddress, PeriodicCheck, ReadFrom as CoreReadFrom, TlsMode,
};
use glide_core::iam::ServiceType as CoreServiceType;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub use glide_core::client::NodeDiscoveryMode;

/// The kind of a Pub/Sub channel subscription.
///
/// Mirrors Python's `PubSubChannelModes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PubSubChannelMode {
    /// Exact channel name (`SUBSCRIBE`).
    Exact,
    /// Glob pattern (`PSUBSCRIBE`).
    Pattern,
    /// Shard channel (`SSUBSCRIBE`, cluster only).
    Sharded,
}

impl PubSubChannelMode {
    fn to_core(self) -> redis::PubSubSubscriptionKind {
        match self {
            PubSubChannelMode::Exact => redis::PubSubSubscriptionKind::Exact,
            PubSubChannelMode::Pattern => redis::PubSubSubscriptionKind::Pattern,
            PubSubChannelMode::Sharded => redis::PubSubSubscriptionKind::Sharded,
        }
    }
}

/// Pub/Sub subscriptions to establish automatically when the client connects.
///
/// Messages received on these subscriptions are delivered via
/// [`crate::GlideClient::get_pubsub_message`] /
/// [`crate::GlideClient::try_get_pubsub_message`] (and the cluster equivalents).
///
/// Mirrors Python's `*ClientConfiguration.pubsub_subscriptions`.
#[derive(Debug, Clone, Default)]
pub struct PubSubSubscriptions {
    channels: HashMap<PubSubChannelMode, HashSet<Vec<u8>>>,
}

impl PubSubSubscriptions {
    /// Create an empty subscription set.
    pub fn new() -> Self {
        PubSubSubscriptions::default()
    }

    /// Subscribe to `channel` under the given `mode`.
    pub fn subscribe(mut self, mode: PubSubChannelMode, channel: impl Into<Vec<u8>>) -> Self {
        self.channels
            .entry(mode)
            .or_default()
            .insert(channel.into());
        self
    }

    /// Whether any subscriptions are configured.
    pub fn is_empty(&self) -> bool {
        self.channels.values().all(|s| s.is_empty())
    }

    pub(crate) fn to_core(&self) -> redis::PubSubSubscriptionInfo {
        let mut info: redis::PubSubSubscriptionInfo = HashMap::new();
        for (mode, set) in &self.channels {
            info.insert(mode.to_core(), set.clone());
        }
        info
    }
}

/// The RESP protocol version used to communicate with the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 — the legacy protocol.
    RESP2,
    /// RESP3 — the default; enables richer typed replies and client-side push.
    #[default]
    RESP3,
}

impl From<ProtocolVersion> for redis::ProtocolVersion {
    fn from(v: ProtocolVersion) -> Self {
        match v {
            ProtocolVersion::RESP2 => redis::ProtocolVersion::RESP2,
            ProtocolVersion::RESP3 => redis::ProtocolVersion::RESP3,
        }
    }
}

/// Strategy for selecting which node to read from.
///
/// Mirrors Python `ReadFrom`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReadFrom {
    /// Always read from the primary.
    #[default]
    Primary,
    /// Prefer replicas, falling back to the primary if none are available.
    PreferReplica,
    /// Read from a replica in the given availability zone when possible.
    AZAffinity(String),
    /// Read from a replica in the given AZ, else the primary in that AZ, else any.
    AZAffinityReplicasAndPrimary(String),
    /// Spread reads across all nodes.
    AllNodes,
}

impl From<ReadFrom> for CoreReadFrom {
    fn from(v: ReadFrom) -> Self {
        match v {
            ReadFrom::Primary => CoreReadFrom::Primary,
            ReadFrom::PreferReplica => CoreReadFrom::PreferReplica,
            ReadFrom::AZAffinity(az) => CoreReadFrom::AZAffinity(az),
            ReadFrom::AZAffinityReplicasAndPrimary(az) => {
                CoreReadFrom::AZAffinityReplicasAndPrimary(az)
            }
            ReadFrom::AllNodes => CoreReadFrom::AllNodes,
        }
    }
}

/// A server address (host + port).
///
/// Mirrors Python `NodeAddress`. Defaults to `localhost:6379`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAddress {
    /// Hostname or IP address.
    pub host: String,
    /// TCP port.
    pub port: u16,
}

impl NodeAddress {
    /// Create a new address.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        NodeAddress {
            host: host.into(),
            port,
        }
    }
}

impl Default for NodeAddress {
    fn default() -> Self {
        NodeAddress::new("localhost", 6379)
    }
}

impl From<NodeAddress> for CoreNodeAddress {
    fn from(a: NodeAddress) -> Self {
        CoreNodeAddress {
            host: a.host,
            port: a.port,
        }
    }
}

/// Username/password credentials.
///
/// Mirrors Python `ServerCredentials`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ServerCredentials {
    /// Optional username (ACL). If omitted, the default user is used. Required
    /// when [`Self::iam_config`] is set.
    pub username: Option<String>,
    /// Password for traditional authentication. Ignored when IAM is configured
    /// and available (IAM acts as the password source); may still be set as a
    /// fallback.
    pub password: Option<String>,
    /// AWS IAM authentication configuration. When set, IAM takes precedence over
    /// [`Self::password`].
    pub iam_config: Option<IamAuthConfig>,
}

impl ServerCredentials {
    /// Password-only credentials (default user).
    pub fn password(password: impl Into<String>) -> Self {
        ServerCredentials {
            username: None,
            password: Some(password.into()),
            iam_config: None,
        }
    }

    /// Username + password credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        ServerCredentials {
            username: Some(username.into()),
            password: Some(password.into()),
            iam_config: None,
        }
    }

    /// AWS IAM credentials for ElastiCache/MemoryDB. `username` is the IAM user
    /// and is required; the token is signed and refreshed automatically by the
    /// core. Mirrors Python's IAM `ServerCredentials`.
    pub fn iam(username: impl Into<String>, iam_config: IamAuthConfig) -> Self {
        ServerCredentials {
            username: Some(username.into()),
            password: None,
            iam_config: Some(iam_config),
        }
    }

    /// Set a fallback password (used when IAM is unavailable). Builder form.
    #[must_use]
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub(crate) fn to_core(&self) -> AuthenticationInfo {
        AuthenticationInfo {
            username: self.username.clone(),
            password: self.password.clone(),
            iam_config: self.iam_config.as_ref().map(IamAuthConfig::to_core),
        }
    }
}

impl std::fmt::Debug for ServerCredentials {
    /// Redacts the password so it never leaks through `{:?}` (including via the
    /// containing configuration's derived `Debug`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerCredentials")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("iam_config", &self.iam_config)
            .finish()
    }
}

/// Client certificate + private key (both PEM) for **mutual TLS**.
///
/// Fields are private so a half-set identity (cert without key or vice versa)
/// is unrepresentable. Build with [`Self::new`] or the configs'
/// `client_identity(cert, key)` builder methods.
#[derive(Clone)]
pub struct ClientIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl ClientIdentity {
    /// Create a client identity from certificate and private-key PEM bytes.
    pub fn new(cert_pem: impl Into<Vec<u8>>, key_pem: impl Into<Vec<u8>>) -> Self {
        ClientIdentity {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
        }
    }

    /// The client certificate (PEM bytes).
    pub fn cert_pem(&self) -> &[u8] {
        &self.cert_pem
    }

    pub(crate) fn key_pem(&self) -> &[u8] {
        &self.key_pem
    }
}

impl std::fmt::Debug for ClientIdentity {
    /// Redacts the private key so it never leaks through `{:?}` (including via
    /// the containing configuration's derived `Debug`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientIdentity")
            .field("cert_pem", &format_args!("[{} bytes]", self.cert_pem.len()))
            .field("key_pem", &"<redacted>")
            .finish()
    }
}

/// AWS service backing IAM authentication.
///
/// Mirrors Python's IAM `ServiceType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceType {
    /// Amazon ElastiCache.
    ElastiCache,
    /// Amazon MemoryDB.
    MemoryDB,
}

impl From<ServiceType> for CoreServiceType {
    fn from(s: ServiceType) -> Self {
        match s {
            ServiceType::ElastiCache => CoreServiceType::ElastiCache,
            ServiceType::MemoryDB => CoreServiceType::MemoryDB,
        }
    }
}

/// AWS IAM authentication configuration for ElastiCache/MemoryDB.
///
/// The core resolves AWS credentials, signs a SigV4 auth token, and refreshes it
/// automatically (default every 14 minutes). Mirrors Python's IAM config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamAuthConfig {
    /// AWS ElastiCache or MemoryDB cluster name.
    pub cluster_name: String,
    /// AWS region of the cluster (e.g. `us-east-1`).
    pub region: String,
    /// Which AWS service backs the cluster.
    pub service_type: ServiceType,
    /// Token refresh interval in seconds (1s–12h). `None` uses the core default
    /// (14 minutes).
    pub refresh_interval_seconds: Option<u32>,
}

impl IamAuthConfig {
    /// Create an IAM config for the given cluster, region, and service, using the
    /// default refresh interval.
    pub fn new(
        cluster_name: impl Into<String>,
        region: impl Into<String>,
        service_type: ServiceType,
    ) -> Self {
        IamAuthConfig {
            cluster_name: cluster_name.into(),
            region: region.into(),
            service_type,
            refresh_interval_seconds: None,
        }
    }

    /// Override the token refresh interval (seconds). Builder form.
    #[must_use]
    pub fn with_refresh_interval_seconds(mut self, seconds: u32) -> Self {
        self.refresh_interval_seconds = Some(seconds);
        self
    }

    fn to_core(&self) -> IamAuthenticationConfig {
        IamAuthenticationConfig {
            cluster_name: self.cluster_name.clone(),
            region: self.region.clone(),
            service_type: self.service_type.into(),
            refresh_interval_seconds: self.refresh_interval_seconds,
        }
    }
}

/// Reconnection backoff strategy.
///
/// Mirrors Python `BackoffStrategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffStrategy {
    /// Number of retry attempts before giving up on a reconnection burst.
    pub num_of_retries: u32,
    /// The exponent base used for the exponential backoff.
    pub factor: u32,
    /// The multiplier that will be applied to the waiting time between retries.
    pub exponent_base: u32,
    /// Optional jitter percentage applied to the computed delay.
    pub jitter_percent: Option<u32>,
}

impl From<BackoffStrategy> for ConnectionRetryStrategy {
    fn from(b: BackoffStrategy) -> Self {
        ConnectionRetryStrategy {
            exponent_base: b.exponent_base,
            factor: b.factor,
            number_of_retries: b.num_of_retries,
            jitter_percent: b.jitter_percent,
        }
    }
}

/// Periodic cluster topology check configuration (cluster only).
///
/// Mirrors Python `PeriodicChecksStatus` / `PeriodicChecksManualInterval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PeriodicChecks {
    /// Enabled with the default interval.
    #[default]
    Enabled,
    /// Disabled entirely.
    Disabled,
    /// Enabled with a manual interval (seconds).
    ManualInterval(u64),
}

impl From<PeriodicChecks> for PeriodicCheck {
    fn from(p: PeriodicChecks) -> Self {
        match p {
            PeriodicChecks::Enabled => PeriodicCheck::Enabled,
            PeriodicChecks::Disabled => PeriodicCheck::Disabled,
            PeriodicChecks::ManualInterval(secs) => {
                PeriodicCheck::ManualInterval(Duration::from_secs(secs))
            }
        }
    }
}

/// TLS mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsConfig {
    /// No TLS (plaintext).
    #[default]
    NoTls,
    /// TLS with full certificate verification.
    SecureTls,
    /// TLS without certificate verification (testing only).
    InsecureTls,
}

impl From<TlsConfig> for TlsMode {
    fn from(t: TlsConfig) -> Self {
        match t {
            TlsConfig::NoTls => TlsMode::NoTls,
            TlsConfig::SecureTls => TlsMode::SecureTls,
            TlsConfig::InsecureTls => TlsMode::InsecureTls,
        }
    }
}

/// Configuration for a **standalone** (non-cluster) GLIDE client.
///
/// Mirrors Python `GlideClientConfiguration`. Build with [`Self::new`] then chain
/// the `with_*` setters.
#[derive(Debug, Clone)]
pub struct GlideClientConfiguration {
    /// Seed node addresses.
    pub addresses: Vec<NodeAddress>,
    /// TLS mode.
    pub tls: TlsConfig,
    /// Optional credentials.
    pub credentials: Option<ServerCredentials>,
    /// Read strategy.
    pub read_from: ReadFrom,
    /// Overall request timeout.
    pub request_timeout: Option<Duration>,
    /// Connection establishment timeout.
    pub connection_timeout: Option<Duration>,
    /// Reconnection backoff strategy.
    pub reconnect_strategy: Option<BackoffStrategy>,
    /// Logical database index to SELECT on connect.
    pub database_id: i64,
    /// Client name reported to the server (`CLIENT SETNAME`).
    pub client_name: Option<String>,
    /// RESP protocol version.
    pub protocol: ProtocolVersion,
    /// Maximum number of concurrent in-flight requests.
    pub inflight_requests_limit: Option<u32>,
    /// Defer connecting until the first command is issued.
    pub lazy_connect: bool,
    /// Pub/Sub subscriptions to establish on connect.
    pub pubsub_subscriptions: Option<PubSubSubscriptions>,
    /// Force creation of the Pub/Sub push channel even when no subscriptions are
    /// configured, so runtime `subscribe`/`unsubscribe` can receive messages.
    /// See [`crate::commands::pubsub::PubSubCommands`].
    pub force_pubsub_channel: bool,
    /// Custom CA certificate(s) (PEM bytes) to trust when verifying the server
    /// under [`TlsConfig::SecureTls`]. Lowered into
    /// `ConnectionRequest::root_certs`. Empty = use the system trust store.
    pub root_certs: Vec<Vec<u8>>,
    /// Client certificate + private key for **mutual TLS**. Set via
    /// [`Self::client_identity`]. Only meaningful together with
    /// [`TlsConfig::SecureTls`].
    pub client_identity: Option<ClientIdentity>,
}

impl GlideClientConfiguration {
    /// Create a standalone configuration for the given addresses.
    pub fn new(addresses: Vec<NodeAddress>) -> Self {
        GlideClientConfiguration {
            addresses,
            tls: TlsConfig::NoTls,
            credentials: None,
            read_from: ReadFrom::Primary,
            request_timeout: None,
            connection_timeout: None,
            reconnect_strategy: None,
            database_id: 0,
            client_name: None,
            protocol: ProtocolVersion::default(),
            inflight_requests_limit: None,
            lazy_connect: false,
            pubsub_subscriptions: None,
            force_pubsub_channel: false,
            root_certs: Vec::new(),
            client_identity: None,
        }
    }

    /// Build a configuration from a Redis connection URL, using the exact URL
    /// semantics of redis-rs (`redis://` and `rediss://`, with
    /// `[user][:password@]host[:port][/db]`):
    ///
    /// ```
    /// use glide::GlideClientConfiguration;
    /// let cfg = GlideClientConfiguration::from_url("redis://user:pass@localhost:6379/2").unwrap();
    /// assert_eq!(cfg.database_id, 2);
    /// ```
    ///
    /// `rediss://` enables TLS with full verification;
    /// `rediss://…/#insecure` disables certificate verification, as in
    /// redis-rs. Unix-socket URLs are not supported by glide-core and return
    /// a configuration error.
    pub fn from_url(url: &str) -> crate::error::Result<Self> {
        Self::from_connection_info(url)
    }

    /// Build a configuration from anything implementing redis-rs's
    /// [`redis::IntoConnectionInfo`] (a URL string, or a prebuilt
    /// [`redis::ConnectionInfo`]).
    pub fn from_connection_info<T: redis::IntoConnectionInfo>(
        info: T,
    ) -> crate::error::Result<Self> {
        let info = info
            .into_connection_info()
            .map_err(|e| crate::error::GlideError::Configuration(e.to_string()))?;
        let (address, tls) = split_connection_addr(info.addr)?;
        let mut cfg = Self::new(vec![address]).tls(tls);
        cfg.database_id = info.redis.db;
        cfg.protocol = from_redis_protocol(info.redis.protocol);
        cfg.client_name = info.redis.client_name;
        cfg.credentials = credentials_from_info(info.redis.username, info.redis.password);
        Ok(cfg)
    }

    /// Set a client certificate + private key (both PEM) for **mutual TLS**.
    /// The server must be configured to require/verify client certificates.
    /// Only meaningful together with [`TlsConfig::SecureTls`].
    #[must_use]
    pub fn client_identity(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_identity = Some(ClientIdentity::new(cert_pem, key_pem));
        self
    }

    /// Configure for a single `host:port`.
    pub fn with_address(host: impl Into<String>, port: u16) -> Self {
        GlideClientConfiguration::new(vec![NodeAddress::new(host, port)])
    }

    /// Set TLS mode.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }
    /// Trust a custom CA certificate (PEM bytes) when verifying the server with
    /// [`TlsConfig::SecureTls`]. Enables secure TLS against a server presenting a
    /// certificate signed by a private/self-signed CA. May be called more than
    /// once to trust multiple CAs.
    pub fn tls_ca_cert(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.root_certs.push(pem.into());
        self
    }
    /// Set credentials.
    pub fn credentials(mut self, creds: ServerCredentials) -> Self {
        self.credentials = Some(creds);
        self
    }
    /// Set read strategy.
    pub fn read_from(mut self, read_from: ReadFrom) -> Self {
        self.read_from = read_from;
        self
    }
    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }
    /// Set the connection-establishment timeout.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }
    /// Set the maximum number of concurrent in-flight requests.
    pub fn inflight_requests_limit(mut self, limit: u32) -> Self {
        self.inflight_requests_limit = Some(limit);
        self
    }
    /// Set database id.
    pub fn database_id(mut self, db: i64) -> Self {
        self.database_id = db;
        self
    }
    /// Set client name.
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }
    /// Set protocol version.
    pub fn protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.protocol = protocol;
        self
    }
    /// Enable lazy connection.
    pub fn lazy_connect(mut self, lazy: bool) -> Self {
        self.lazy_connect = lazy;
        self
    }
    /// Set reconnection strategy.
    pub fn reconnect_strategy(mut self, strategy: BackoffStrategy) -> Self {
        self.reconnect_strategy = Some(strategy);
        self
    }
    /// Set Pub/Sub subscriptions to establish on connect.
    pub fn subscriptions(mut self, subscriptions: PubSubSubscriptions) -> Self {
        self.pubsub_subscriptions = Some(subscriptions);
        self
    }

    /// Enable the Pub/Sub push channel without configuring any connect-time
    /// subscriptions, so the client can use runtime `subscribe`/`unsubscribe`
    /// and receive messages via `get_pubsub_message`. Connect-time
    /// `subscriptions` enable this implicitly.
    ///
    /// Note: runtime subscriptions are session-scoped — unlike connect-time
    /// subscriptions they are not automatically restored after a reconnect.
    #[must_use]
    pub fn enable_pubsub(mut self) -> Self {
        self.force_pubsub_channel = true;
        self
    }

    pub(crate) fn to_request(&self) -> ConnectionRequest {
        let mut req = base_request(
            &self.addresses,
            self.tls,
            self.credentials.as_ref(),
            &self.read_from,
            self.request_timeout,
            self.connection_timeout,
            self.reconnect_strategy,
            self.client_name.as_deref(),
            self.protocol,
            self.inflight_requests_limit,
            self.lazy_connect,
            &self.root_certs,
        );
        req.cluster_mode_enabled = false;
        req.database_id = self.database_id;
        if let Some(identity) = &self.client_identity {
            req.client_cert = identity.cert_pem().to_vec();
            req.client_key = identity.key_pem().to_vec();
        }
        if let Some(subs) = &self.pubsub_subscriptions {
            req.pubsub_subscriptions = Some(subs.to_core());
        }
        req
    }
}

/// Configuration for a **cluster** GLIDE client.
///
/// Mirrors Python `GlideClusterClientConfiguration`.
#[derive(Debug, Clone)]
pub struct GlideClusterClientConfiguration {
    /// Seed node addresses.
    pub addresses: Vec<NodeAddress>,
    /// TLS mode.
    pub tls: TlsConfig,
    /// Optional credentials.
    pub credentials: Option<ServerCredentials>,
    /// Read strategy.
    pub read_from: ReadFrom,
    /// Overall request timeout.
    pub request_timeout: Option<Duration>,
    /// Connection establishment timeout.
    pub connection_timeout: Option<Duration>,
    /// Reconnection backoff strategy.
    pub reconnect_strategy: Option<BackoffStrategy>,
    /// Client name reported to the server.
    pub client_name: Option<String>,
    /// RESP protocol version.
    pub protocol: ProtocolVersion,
    /// Periodic topology checks.
    pub periodic_checks: PeriodicChecks,
    /// Maximum number of concurrent in-flight requests.
    pub inflight_requests_limit: Option<u32>,
    /// Defer connecting until the first command is issued.
    pub lazy_connect: bool,
    /// Pub/Sub subscriptions to establish on connect.
    pub pubsub_subscriptions: Option<PubSubSubscriptions>,
    /// Force creation of the Pub/Sub push channel even when no subscriptions are
    /// configured, so runtime `subscribe`/`unsubscribe` can receive messages.
    /// See [`crate::commands::pubsub::PubSubCommands`].
    pub force_pubsub_channel: bool,
    /// Custom CA certificate(s) (PEM bytes) to trust when verifying the server
    /// under [`TlsConfig::SecureTls`]. Lowered into
    /// `ConnectionRequest::root_certs`. Empty = use the system trust store.
    pub root_certs: Vec<Vec<u8>>,
    /// Client certificate + private key for **mutual TLS**. Set via
    /// [`Self::client_identity`]. Only meaningful together with
    /// [`TlsConfig::SecureTls`].
    pub client_identity: Option<ClientIdentity>,
}

impl GlideClusterClientConfiguration {
    /// Build a cluster configuration from one or more Redis connection URLs
    /// (seed nodes), using the exact URL semantics of redis-rs
    /// (`ClusterClient::new(initial_nodes)` accepts the same URLs):
    ///
    /// ```
    /// use glide::GlideClusterClientConfiguration;
    /// let cfg = GlideClusterClientConfiguration::from_urls([
    ///     "redis://n1:7000",
    ///     "redis://n2:7001",
    /// ]).unwrap();
    /// assert_eq!(cfg.addresses.len(), 2);
    /// ```
    ///
    /// Credentials / client-name / database / TLS mode must be identical
    /// across all URLs — conflicting settings are rejected with a
    /// configuration error, as in redis-rs's `ClusterClient`. A URL selecting
    /// a non-zero database is rejected — clusters only support database 0.
    /// The RESP `protocol` is taken from the **first** URL and not
    /// cross-validated (matching the fork, which overwrites per-node protocol
    /// from builder params without validating it).
    pub fn from_urls<T: redis::IntoConnectionInfo>(
        urls: impl IntoIterator<Item = T>,
    ) -> crate::error::Result<Self> {
        let mut addresses = Vec::new();
        let mut first: Option<(TlsConfig, redis::RedisConnectionInfo)> = None;
        for url in urls {
            let info = url
                .into_connection_info()
                .map_err(|e| crate::error::GlideError::Configuration(e.to_string()))?;
            let (address, tls) = split_connection_addr(info.addr)?;
            addresses.push(address);
            // Reject conflicting per-URL settings, matching the fork's
            // `ClusterClient` validation — silently ignoring the settings of
            // URLs 2..N would misconfigure the client.
            match &first {
                None => first = Some((tls, info.redis)),
                Some((first_tls, first_redis)) => {
                    if info.redis.password != first_redis.password {
                        return Err(crate::error::GlideError::Configuration(
                            "Cannot use different password among initial nodes.".into(),
                        ));
                    }
                    if info.redis.username != first_redis.username {
                        return Err(crate::error::GlideError::Configuration(
                            "Cannot use different username among initial nodes.".into(),
                        ));
                    }
                    if info.redis.client_name != first_redis.client_name {
                        return Err(crate::error::GlideError::Configuration(
                            "Cannot use different client_name among initial nodes.".into(),
                        ));
                    }
                    if info.redis.db != first_redis.db {
                        return Err(crate::error::GlideError::Configuration(
                            "Cannot use different database among initial nodes.".into(),
                        ));
                    }
                    if tls != *first_tls {
                        return Err(crate::error::GlideError::Configuration(
                            "Cannot use different TLS modes among initial nodes.".into(),
                        ));
                    }
                }
            }
        }
        let Some((tls, redis_info)) = first else {
            return Err(crate::error::GlideError::Configuration(
                "at least one node URL is required".into(),
            ));
        };
        if redis_info.db != 0 {
            return Err(crate::error::GlideError::Configuration(
                "cluster deployments only support database 0".into(),
            ));
        }
        let mut cfg = Self::new(addresses).tls(tls);
        cfg.protocol = from_redis_protocol(redis_info.protocol);
        cfg.client_name = redis_info.client_name;
        cfg.credentials = credentials_from_info(redis_info.username, redis_info.password);
        Ok(cfg)
    }

    /// Set a client certificate + private key (both PEM) for **mutual TLS**.
    /// The server must be configured to require/verify client certificates.
    /// Only meaningful together with [`TlsConfig::SecureTls`].
    #[must_use]
    pub fn client_identity(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_identity = Some(ClientIdentity::new(cert_pem, key_pem));
        self
    }

    /// Create a cluster configuration for the given addresses.
    pub fn new(addresses: Vec<NodeAddress>) -> Self {
        GlideClusterClientConfiguration {
            addresses,
            tls: TlsConfig::NoTls,
            credentials: None,
            read_from: ReadFrom::Primary,
            request_timeout: None,
            connection_timeout: None,
            reconnect_strategy: None,
            client_name: None,
            protocol: ProtocolVersion::default(),
            periodic_checks: PeriodicChecks::Enabled,
            inflight_requests_limit: None,
            lazy_connect: false,
            pubsub_subscriptions: None,
            force_pubsub_channel: false,
            root_certs: Vec::new(),
            client_identity: None,
        }
    }

    /// Configure for a single `host:port` seed.
    pub fn with_address(host: impl Into<String>, port: u16) -> Self {
        GlideClusterClientConfiguration::new(vec![NodeAddress::new(host, port)])
    }

    /// Set TLS mode.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }
    /// Trust a custom CA certificate (PEM bytes) when verifying the server with
    /// [`TlsConfig::SecureTls`]. May be called more than once to trust multiple CAs.
    pub fn tls_ca_cert(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.root_certs.push(pem.into());
        self
    }
    /// Set credentials.
    pub fn credentials(mut self, creds: ServerCredentials) -> Self {
        self.credentials = Some(creds);
        self
    }
    /// Set read strategy.
    pub fn read_from(mut self, read_from: ReadFrom) -> Self {
        self.read_from = read_from;
        self
    }
    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }
    /// Set the connection-establishment timeout.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }
    /// Set the maximum number of concurrent in-flight requests.
    pub fn inflight_requests_limit(mut self, limit: u32) -> Self {
        self.inflight_requests_limit = Some(limit);
        self
    }
    /// Set reconnection strategy.
    pub fn reconnect_strategy(mut self, strategy: BackoffStrategy) -> Self {
        self.reconnect_strategy = Some(strategy);
        self
    }
    /// Set client name.
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }
    /// Set protocol version.
    pub fn protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.protocol = protocol;
        self
    }
    /// Set periodic checks.
    pub fn periodic_checks(mut self, checks: PeriodicChecks) -> Self {
        self.periodic_checks = checks;
        self
    }
    /// Enable lazy connection.
    pub fn lazy_connect(mut self, lazy: bool) -> Self {
        self.lazy_connect = lazy;
        self
    }
    /// Set Pub/Sub subscriptions to establish on connect.
    pub fn subscriptions(mut self, subscriptions: PubSubSubscriptions) -> Self {
        self.pubsub_subscriptions = Some(subscriptions);
        self
    }

    /// Enable the Pub/Sub push channel without configuring any connect-time
    /// subscriptions, so the client can use runtime `subscribe`/`unsubscribe`
    /// and receive messages via `get_pubsub_message`. Connect-time
    /// `subscriptions` enable this implicitly.
    ///
    /// Note: runtime subscriptions are session-scoped — unlike connect-time
    /// subscriptions they are not automatically restored after a reconnect.
    #[must_use]
    pub fn enable_pubsub(mut self) -> Self {
        self.force_pubsub_channel = true;
        self
    }

    pub(crate) fn to_request(&self) -> ConnectionRequest {
        let mut req = base_request(
            &self.addresses,
            self.tls,
            self.credentials.as_ref(),
            &self.read_from,
            self.request_timeout,
            self.connection_timeout,
            self.reconnect_strategy,
            self.client_name.as_deref(),
            self.protocol,
            self.inflight_requests_limit,
            self.lazy_connect,
            &self.root_certs,
        );
        req.cluster_mode_enabled = true;
        req.periodic_checks = Some(self.periodic_checks.into());
        if let Some(identity) = &self.client_identity {
            req.client_cert = identity.cert_pem().to_vec();
            req.client_key = identity.key_pem().to_vec();
        }
        if let Some(subs) = &self.pubsub_subscriptions {
            req.pubsub_subscriptions = Some(subs.to_core());
        }
        req
    }
}

/// Map a redis-rs [`redis::ConnectionAddr`] to our address + TLS mode.
fn split_connection_addr(
    addr: redis::ConnectionAddr,
) -> crate::error::Result<(NodeAddress, TlsConfig)> {
    match addr {
        redis::ConnectionAddr::Tcp(host, port) => {
            Ok((NodeAddress::new(host, port), TlsConfig::NoTls))
        }
        redis::ConnectionAddr::TcpTls {
            host,
            port,
            insecure,
            tls_params,
        } => {
            // The fork's `TlsConnParams` fields (root cert store, client
            // identity) are `pub(crate)` — we cannot read them to map onto
            // `root_certs` / `client_identity`. Silently dropping them would
            // yield a mysteriously misconfigured connection (wrong trust
            // roots, mTLS not attempted), so fail loudly instead.
            if tls_params.is_some() {
                return Err(crate::error::GlideError::Configuration(
                    "ConnectionInfo carries TLS certificate parameters (TlsCertificates) that \
                     cannot be mapped; configure them via the config's `root_certs` field and \
                     `client_identity(cert, key)` instead"
                        .into(),
                ));
            }
            let tls = if insecure {
                TlsConfig::InsecureTls
            } else {
                TlsConfig::SecureTls
            };
            Ok((NodeAddress::new(host, port), tls))
        }
        redis::ConnectionAddr::Unix(_) => Err(crate::error::GlideError::Configuration(
            "unix-socket connections are not supported by glide-core".into(),
        )),
    }
}

/// Map redis-rs's protocol enum to ours.
fn from_redis_protocol(p: redis::ProtocolVersion) -> ProtocolVersion {
    match p {
        redis::ProtocolVersion::RESP2 => ProtocolVersion::RESP2,
        redis::ProtocolVersion::RESP3 => ProtocolVersion::RESP3,
    }
}

/// Build [`ServerCredentials`] from URL-provided username/password (either may
/// be absent; `redis://:pass@host` yields password-only credentials).
fn credentials_from_info(
    username: Option<String>,
    password: Option<String>,
) -> Option<ServerCredentials> {
    match (username, password) {
        (None, None) => None,
        (username, password) => Some(ServerCredentials {
            username,
            password,
            iam_config: None,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn base_request(
    addresses: &[NodeAddress],
    tls: TlsConfig,
    credentials: Option<&ServerCredentials>,
    read_from: &ReadFrom,
    request_timeout: Option<Duration>,
    connection_timeout: Option<Duration>,
    reconnect_strategy: Option<BackoffStrategy>,
    client_name: Option<&str>,
    protocol: ProtocolVersion,
    inflight_requests_limit: Option<u32>,
    lazy_connect: bool,
    root_certs: &[Vec<u8>],
) -> ConnectionRequest {
    let mut req = ConnectionRequest {
        addresses: addresses.iter().cloned().map(Into::into).collect(),
        tls_mode: Some(tls.into()),
        read_from: Some(read_from.clone().into()),
        protocol: Some(protocol.into()),
        client_name: client_name.map(|s| s.to_string()),
        // Identify this client library to the server (CLIENT INFO / lib-name),
        // mirroring the other GLIDE wrappers (GlidePy, GlideJava, ...).
        lib_name: Some("GlideRust".to_string()),
        lazy_connect,
        inflight_requests_limit,
        // Disable Nagle's algorithm. We build `ConnectionRequest` directly, so we
        // do NOT inherit glide-core's protobuf-path default of `tcp_nodelay = true`
        // (the bare struct `Default` is `false`). Leaving Nagle on interacts with
        // delayed-ACK to add multi-ms tail latency under high concurrency, so we
        // explicitly enable TCP_NODELAY to match the intended core default.
        tcp_nodelay: true,
        // All other fields (periodic_checks, database_id, pubsub_subscriptions,
        // cluster_mode_enabled, tls certs, otel, IAM, ...) are intentionally left
        // at their core defaults here and set by the caller's `to_request()`.
        // If glide-core adds a field this default absorbs it silently — revisit
        // when bumping the glide-core dependency.
        ..ConnectionRequest::default()
    };

    if !root_certs.is_empty() {
        req.root_certs = root_certs.to_vec();
    }

    if let Some(creds) = credentials {
        req.authentication_info = Some(creds.to_core());
    }

    if let Some(t) = request_timeout {
        req.request_timeout = Some(duration_as_millis_u32(t));
    }
    if let Some(t) = connection_timeout {
        req.connection_timeout = Some(duration_as_millis_u32(t));
    }
    if let Some(strategy) = reconnect_strategy {
        req.connection_retry_strategy = Some(strategy.into());
    }
    req
}

/// Convert a [`Duration`] to whole milliseconds as `u32`, saturating at
/// `u32::MAX` rather than silently truncating (a `Duration` above ~49.7 days
/// would overflow `u32` ms). The core treats the timeout as a `u32` millisecond
/// count, so saturation is the safe, lossless-within-range behavior.
fn duration_as_millis_u32(d: Duration) -> u32 {
    u32::try_from(d.as_millis()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    //! Pure-logic configuration tests mirroring Python `tests/test_config.py`.
    //!
    //! Every test asserts that the ergonomic builder structs lower into the
    //! correct `glide_core::client::ConnectionRequest` via `to_request()`, plus
    //! the standalone `From` conversions for each config enum.
    use super::*;
    use glide_core::client::{PeriodicCheck, ReadFrom as CoreReadFrom, TlsMode};

    // ---- addresses -------------------------------------------------------

    #[test]
    fn node_address_default_is_localhost_6379() {
        let a = NodeAddress::default();
        assert_eq!(a.host, "localhost");
        assert_eq!(a.port, 6379);
    }

    #[test]
    fn tcp_nodelay_enabled_by_default() {
        // Guard against the Nagle regression: we build ConnectionRequest directly,
        // so we must explicitly enable TCP_NODELAY (bare struct default is false).
        assert!(
            GlideClientConfiguration::with_address("h", 1)
                .to_request()
                .tcp_nodelay
        );
        assert!(
            GlideClusterClientConfiguration::with_address("h", 1)
                .to_request()
                .tcp_nodelay
        );
    }

    #[test]
    fn lib_name_is_glide_rust() {
        assert_eq!(
            GlideClientConfiguration::with_address("h", 1)
                .to_request()
                .lib_name
                .as_deref(),
            Some("GlideRust")
        );
        assert_eq!(
            GlideClusterClientConfiguration::with_address("h", 1)
                .to_request()
                .lib_name
                .as_deref(),
            Some("GlideRust")
        );
    }

    #[test]
    fn with_address_single_host_port() {
        let req = GlideClientConfiguration::with_address("example.com", 6380).to_request();
        assert_eq!(req.addresses.len(), 1);
        assert_eq!(req.addresses[0].host, "example.com");
        assert_eq!(req.addresses[0].port, 6380);
    }

    #[test]
    fn multiple_addresses_preserved_in_order() {
        let req = GlideClientConfiguration::new(vec![
            NodeAddress::new("a", 1),
            NodeAddress::new("b", 2),
            NodeAddress::new("c", 3),
        ])
        .to_request();
        assert_eq!(req.addresses.len(), 3);
        assert_eq!(req.addresses[0].host, "a");
        assert_eq!(req.addresses[0].port, 1);
        assert_eq!(req.addresses[1].host, "b");
        assert_eq!(req.addresses[2].host, "c");
        assert_eq!(req.addresses[2].port, 3);
    }

    #[test]
    fn cluster_with_address_single_seed() {
        let req = GlideClusterClientConfiguration::with_address("seed", 7000).to_request();
        assert_eq!(req.addresses.len(), 1);
        assert_eq!(req.addresses[0].host, "seed");
        assert_eq!(req.addresses[0].port, 7000);
    }

    // ---- defaults --------------------------------------------------------

    #[test]
    fn standalone_request_defaults() {
        let req = GlideClientConfiguration::with_address("example.com", 6380).to_request();
        assert!(!req.cluster_mode_enabled);
        assert_eq!(req.tls_mode, Some(TlsMode::NoTls));
        assert_eq!(req.read_from, Some(CoreReadFrom::Primary));
        assert_eq!(req.protocol, Some(redis::ProtocolVersion::RESP3));
        assert_eq!(req.database_id, 0);
        assert!(req.authentication_info.is_none());
        assert!(req.periodic_checks.is_none());
        assert!(req.request_timeout.is_none());
        assert!(req.connection_timeout.is_none());
        assert!(req.connection_retry_strategy.is_none());
        assert!(req.client_name.is_none());
        assert!(!req.lazy_connect);
        assert!(req.inflight_requests_limit.is_none());
    }

    #[test]
    fn cluster_request_defaults() {
        let req = GlideClusterClientConfiguration::with_address("seed", 7000).to_request();
        assert!(req.cluster_mode_enabled);
        assert_eq!(req.tls_mode, Some(TlsMode::NoTls));
        assert_eq!(req.read_from, Some(CoreReadFrom::Primary));
        assert_eq!(req.protocol, Some(redis::ProtocolVersion::RESP3));
        // Default periodic checks are Enabled.
        assert!(matches!(req.periodic_checks, Some(PeriodicCheck::Enabled)));
    }

    // ---- TLS modes -------------------------------------------------------

    #[test]
    fn tls_no_tls() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .tls(TlsConfig::NoTls)
            .to_request();
        assert_eq!(req.tls_mode, Some(TlsMode::NoTls));
    }

    #[test]
    fn tls_secure() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .tls(TlsConfig::SecureTls)
            .to_request();
        assert_eq!(req.tls_mode, Some(TlsMode::SecureTls));
    }

    #[test]
    fn tls_insecure() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .tls(TlsConfig::InsecureTls)
            .to_request();
        assert_eq!(req.tls_mode, Some(TlsMode::InsecureTls));
    }

    #[test]
    fn tls_mode_from_conversion() {
        assert_eq!(TlsMode::from(TlsConfig::NoTls), TlsMode::NoTls);
        assert_eq!(TlsMode::from(TlsConfig::SecureTls), TlsMode::SecureTls);
        assert_eq!(TlsMode::from(TlsConfig::InsecureTls), TlsMode::InsecureTls);
    }

    #[test]
    fn tls_applies_to_cluster() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .tls(TlsConfig::SecureTls)
            .to_request();
        assert_eq!(req.tls_mode, Some(TlsMode::SecureTls));
    }

    // ---- protocol --------------------------------------------------------

    #[test]
    fn protocol_resp2() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .protocol(ProtocolVersion::RESP2)
            .to_request();
        assert_eq!(req.protocol, Some(redis::ProtocolVersion::RESP2));
    }

    #[test]
    fn protocol_resp3() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .protocol(ProtocolVersion::RESP3)
            .to_request();
        assert_eq!(req.protocol, Some(redis::ProtocolVersion::RESP3));
    }

    #[test]
    fn protocol_default_is_resp3() {
        assert_eq!(ProtocolVersion::default(), ProtocolVersion::RESP3);
    }

    #[test]
    fn protocol_from_conversion() {
        assert_eq!(
            redis::ProtocolVersion::from(ProtocolVersion::RESP2),
            redis::ProtocolVersion::RESP2
        );
        assert_eq!(
            redis::ProtocolVersion::from(ProtocolVersion::RESP3),
            redis::ProtocolVersion::RESP3
        );
    }

    // ---- read_from -------------------------------------------------------

    #[test]
    fn read_from_primary() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .read_from(ReadFrom::Primary)
            .to_request();
        assert_eq!(req.read_from, Some(CoreReadFrom::Primary));
    }

    #[test]
    fn read_from_prefer_replica() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .read_from(ReadFrom::PreferReplica)
            .to_request();
        assert_eq!(req.read_from, Some(CoreReadFrom::PreferReplica));
    }

    #[test]
    fn read_from_all_nodes() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .read_from(ReadFrom::AllNodes)
            .to_request();
        assert_eq!(req.read_from, Some(CoreReadFrom::AllNodes));
    }

    #[test]
    fn read_from_az_affinity_carries_az() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .read_from(ReadFrom::AZAffinity("us-east-1a".into()))
            .to_request();
        assert_eq!(
            req.read_from,
            Some(CoreReadFrom::AZAffinity("us-east-1a".into()))
        );
    }

    #[test]
    fn read_from_az_affinity_replicas_and_primary_carries_az() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .read_from(ReadFrom::AZAffinityReplicasAndPrimary("us-west-2b".into()))
            .to_request();
        assert_eq!(
            req.read_from,
            Some(CoreReadFrom::AZAffinityReplicasAndPrimary(
                "us-west-2b".into()
            ))
        );
    }

    #[test]
    fn read_from_from_conversions() {
        assert_eq!(CoreReadFrom::from(ReadFrom::Primary), CoreReadFrom::Primary);
        assert_eq!(
            CoreReadFrom::from(ReadFrom::PreferReplica),
            CoreReadFrom::PreferReplica
        );
        assert_eq!(
            CoreReadFrom::from(ReadFrom::AllNodes),
            CoreReadFrom::AllNodes
        );
        assert_eq!(
            CoreReadFrom::from(ReadFrom::AZAffinity("z".into())),
            CoreReadFrom::AZAffinity("z".into())
        );
        assert_eq!(
            CoreReadFrom::from(ReadFrom::AZAffinityReplicasAndPrimary("z".into())),
            CoreReadFrom::AZAffinityReplicasAndPrimary("z".into())
        );
    }

    #[test]
    fn read_from_default_is_primary() {
        assert_eq!(ReadFrom::default(), ReadFrom::Primary);
    }

    // ---- credentials -----------------------------------------------------

    #[test]
    fn credentials_password_only() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::password("secret"))
            .to_request();
        let auth = req.authentication_info.expect("auth set");
        assert!(auth.username.is_none());
        assert_eq!(auth.password.as_deref(), Some("secret"));
    }

    #[test]
    fn credentials_username_and_password() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::new("alice", "hunter2"))
            .to_request();
        let auth = req.authentication_info.expect("auth set");
        assert_eq!(auth.username.as_deref(), Some("alice"));
        assert_eq!(auth.password.as_deref(), Some("hunter2"));
    }

    #[test]
    fn credentials_apply_to_cluster() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::new("u", "p"))
            .to_request();
        let auth = req.authentication_info.expect("auth set");
        assert_eq!(auth.username.as_deref(), Some("u"));
        assert_eq!(auth.password.as_deref(), Some("p"));
    }

    // ---- IAM authentication ---------------------------------------------

    #[test]
    fn iam_credentials_elasticache_lower_correctly() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::iam(
                "iam-user",
                IamAuthConfig::new("my-cluster", "us-east-1", ServiceType::ElastiCache),
            ))
            .to_request();
        let auth = req.authentication_info.expect("auth set");
        assert_eq!(auth.username.as_deref(), Some("iam-user"));
        // IAM-only: no static password.
        assert!(auth.password.is_none());
        let iam = auth.iam_config.expect("iam config set");
        assert_eq!(iam.cluster_name, "my-cluster");
        assert_eq!(iam.region, "us-east-1");
        assert_eq!(iam.service_type, CoreServiceType::ElastiCache);
        assert_eq!(iam.refresh_interval_seconds, None);
    }

    #[test]
    fn iam_credentials_memorydb_with_refresh_interval() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::iam(
                "u",
                IamAuthConfig::new("c", "eu-west-1", ServiceType::MemoryDB)
                    .with_refresh_interval_seconds(300),
            ))
            .to_request();
        let iam = req
            .authentication_info
            .expect("auth set")
            .iam_config
            .expect("iam set");
        assert_eq!(iam.service_type, CoreServiceType::MemoryDB);
        assert_eq!(iam.region, "eu-west-1");
        assert_eq!(iam.refresh_interval_seconds, Some(300));
    }

    #[test]
    fn iam_with_fallback_password_keeps_both() {
        // IAM takes precedence at auth time, but a fallback password may still be
        // provided and must be lowered alongside the IAM config.
        let creds = ServerCredentials::iam(
            "u",
            IamAuthConfig::new("c", "us-west-2", ServiceType::ElastiCache),
        )
        .with_password("fallback");
        let req = GlideClientConfiguration::with_address("h", 1)
            .credentials(creds)
            .to_request();
        let auth = req.authentication_info.expect("auth set");
        assert_eq!(auth.password.as_deref(), Some("fallback"));
        assert!(auth.iam_config.is_some());
    }

    #[test]
    fn iam_credentials_apply_to_cluster() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::iam(
                "u",
                IamAuthConfig::new("c", "ap-south-1", ServiceType::MemoryDB),
            ))
            .to_request();
        let iam = req
            .authentication_info
            .expect("auth set")
            .iam_config
            .expect("iam set");
        assert_eq!(iam.region, "ap-south-1");
        assert_eq!(iam.service_type, CoreServiceType::MemoryDB);
    }

    #[test]
    fn non_iam_credentials_have_no_iam_config() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .credentials(ServerCredentials::password("p"))
            .to_request();
        assert!(
            req.authentication_info
                .expect("auth set")
                .iam_config
                .is_none()
        );
    }

    // ---- runtime pub/sub opt-in ------------------------------------------

    #[test]
    fn enable_pubsub_sets_flag_standalone() {
        let cfg = GlideClientConfiguration::with_address("h", 1);
        assert!(!cfg.force_pubsub_channel);
        assert!(cfg.enable_pubsub().force_pubsub_channel);
    }

    #[test]
    fn enable_pubsub_sets_flag_cluster() {
        let cfg = GlideClusterClientConfiguration::with_address("h", 1);
        assert!(!cfg.force_pubsub_channel);
        assert!(cfg.enable_pubsub().force_pubsub_channel);
    }

    #[test]
    fn credentials_debug_redacts_password() {
        let creds = ServerCredentials::new("alice", "super-secret");
        let shown = format!("{creds:?}");
        assert!(!shown.contains("super-secret"), "password leaked: {shown}");
        assert!(shown.contains("<redacted>"));
        assert!(shown.contains("alice"));
        // And transitively through the configuration's Debug.
        let cfg = GlideClientConfiguration::with_address("h", 1).credentials(creds);
        assert!(!format!("{cfg:?}").contains("super-secret"));
    }

    // ---- timeouts --------------------------------------------------------

    #[test]
    fn request_timeout_millis_conversion() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .request_timeout(Duration::from_millis(750))
            .to_request();
        assert_eq!(req.request_timeout, Some(750));
    }

    #[test]
    fn request_timeout_from_seconds_converts_to_millis() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .request_timeout(Duration::from_secs(2))
            .to_request();
        assert_eq!(req.request_timeout, Some(2000));
    }

    #[test]
    fn connection_timeout_millis_conversion() {
        let mut cfg = GlideClientConfiguration::with_address("h", 1);
        cfg.connection_timeout = Some(Duration::from_millis(250));
        let req = cfg.to_request();
        assert_eq!(req.connection_timeout, Some(250));
    }

    // ---- database_id -----------------------------------------------------

    #[test]
    fn database_id_standalone() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .database_id(7)
            .to_request();
        assert_eq!(req.database_id, 7);
    }

    #[test]
    fn database_id_defaults_to_zero() {
        let req = GlideClientConfiguration::with_address("h", 1).to_request();
        assert_eq!(req.database_id, 0);
    }

    #[test]
    fn cluster_never_sets_database_id() {
        // Cluster config has no database_id setter; request keeps the default 0.
        let req = GlideClusterClientConfiguration::with_address("h", 1).to_request();
        assert_eq!(req.database_id, 0);
    }

    // ---- client_name -----------------------------------------------------

    #[test]
    fn client_name_standalone() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .client_name("app-1")
            .to_request();
        assert_eq!(req.client_name.as_deref(), Some("app-1"));
    }

    #[test]
    fn client_name_cluster() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .client_name("cluster-app")
            .to_request();
        assert_eq!(req.client_name.as_deref(), Some("cluster-app"));
    }

    // ---- lazy_connect ----------------------------------------------------

    #[test]
    fn lazy_connect_standalone() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .lazy_connect(true)
            .to_request();
        assert!(req.lazy_connect);
    }

    #[test]
    fn lazy_connect_cluster() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .lazy_connect(true)
            .to_request();
        assert!(req.lazy_connect);
    }

    // ---- inflight_requests_limit ----------------------------------------

    #[test]
    fn inflight_requests_limit_standalone() {
        let mut cfg = GlideClientConfiguration::with_address("h", 1);
        cfg.inflight_requests_limit = Some(1000);
        let req = cfg.to_request();
        assert_eq!(req.inflight_requests_limit, Some(1000));
    }

    #[test]
    fn inflight_requests_limit_cluster() {
        let mut cfg = GlideClusterClientConfiguration::with_address("h", 1);
        cfg.inflight_requests_limit = Some(42);
        let req = cfg.to_request();
        assert_eq!(req.inflight_requests_limit, Some(42));
    }

    // ---- cluster_mode_enabled -------------------------------------------

    #[test]
    fn cluster_mode_enabled_only_for_cluster() {
        let standalone = GlideClientConfiguration::with_address("h", 1).to_request();
        let cluster = GlideClusterClientConfiguration::with_address("h", 1).to_request();
        assert!(!standalone.cluster_mode_enabled);
        assert!(cluster.cluster_mode_enabled);
    }

    // ---- backoff / retry strategy ---------------------------------------

    #[test]
    fn backoff_strategy_field_mapping() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .reconnect_strategy(BackoffStrategy {
                num_of_retries: 5,
                factor: 2,
                exponent_base: 3,
                jitter_percent: Some(10),
            })
            .to_request();
        let s = req.connection_retry_strategy.expect("retry set");
        assert_eq!(s.number_of_retries, 5);
        assert_eq!(s.factor, 2);
        assert_eq!(s.exponent_base, 3);
        assert_eq!(s.jitter_percent, Some(10));
    }

    #[test]
    fn backoff_strategy_without_jitter() {
        let req = GlideClientConfiguration::with_address("h", 1)
            .reconnect_strategy(BackoffStrategy {
                num_of_retries: 1,
                factor: 100,
                exponent_base: 2,
                jitter_percent: None,
            })
            .to_request();
        let s = req.connection_retry_strategy.expect("retry set");
        assert_eq!(s.number_of_retries, 1);
        assert_eq!(s.factor, 100);
        assert_eq!(s.exponent_base, 2);
        assert!(s.jitter_percent.is_none());
    }

    #[test]
    fn backoff_strategy_from_conversion() {
        let s: ConnectionRetryStrategy = BackoffStrategy {
            num_of_retries: 9,
            factor: 8,
            exponent_base: 7,
            jitter_percent: Some(6),
        }
        .into();
        assert_eq!(s.number_of_retries, 9);
        assert_eq!(s.factor, 8);
        assert_eq!(s.exponent_base, 7);
        assert_eq!(s.jitter_percent, Some(6));
    }

    // ---- periodic checks (cluster only) ---------------------------------

    #[test]
    fn periodic_checks_enabled() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .periodic_checks(PeriodicChecks::Enabled)
            .to_request();
        assert!(matches!(req.periodic_checks, Some(PeriodicCheck::Enabled)));
    }

    #[test]
    fn periodic_checks_disabled() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .periodic_checks(PeriodicChecks::Disabled)
            .to_request();
        assert!(matches!(req.periodic_checks, Some(PeriodicCheck::Disabled)));
    }

    #[test]
    fn periodic_checks_manual_interval() {
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .periodic_checks(PeriodicChecks::ManualInterval(30))
            .to_request();
        match req.periodic_checks {
            Some(PeriodicCheck::ManualInterval(d)) => assert_eq!(d, Duration::from_secs(30)),
            other => panic!("unexpected periodic checks: {other:?}"),
        }
    }

    #[test]
    fn periodic_checks_from_conversions() {
        assert!(matches!(
            PeriodicCheck::from(PeriodicChecks::Enabled),
            PeriodicCheck::Enabled
        ));
        assert!(matches!(
            PeriodicCheck::from(PeriodicChecks::Disabled),
            PeriodicCheck::Disabled
        ));
        match PeriodicCheck::from(PeriodicChecks::ManualInterval(5)) {
            PeriodicCheck::ManualInterval(d) => assert_eq!(d, Duration::from_secs(5)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn periodic_checks_default_is_enabled() {
        assert_eq!(PeriodicChecks::default(), PeriodicChecks::Enabled);
    }

    // ---- pub/sub subscriptions ------------------------------------------

    #[test]
    fn pubsub_subscriptions_default_none() {
        let req = GlideClientConfiguration::with_address("h", 1).to_request();
        assert!(req.pubsub_subscriptions.is_none());
    }

    #[test]
    fn pubsub_subscriptions_lowered_to_core() {
        let subs = PubSubSubscriptions::new()
            .subscribe(PubSubChannelMode::Exact, "ch1")
            .subscribe(PubSubChannelMode::Exact, "ch2")
            .subscribe(PubSubChannelMode::Pattern, "news.*");
        let req = GlideClientConfiguration::with_address("h", 1)
            .subscriptions(subs)
            .to_request();
        let info = req.pubsub_subscriptions.expect("subscriptions set");
        let exact = info
            .get(&redis::PubSubSubscriptionKind::Exact)
            .expect("exact set");
        assert_eq!(exact.len(), 2);
        assert!(exact.contains(&b"ch1".to_vec()));
        assert!(exact.contains(&b"ch2".to_vec()));
        let pattern = info
            .get(&redis::PubSubSubscriptionKind::Pattern)
            .expect("pattern set");
        assert!(pattern.contains(&b"news.*".to_vec()));
    }

    #[test]
    fn pubsub_subscriptions_cluster_sharded() {
        let subs = PubSubSubscriptions::new().subscribe(PubSubChannelMode::Sharded, "shard-ch");
        let req = GlideClusterClientConfiguration::with_address("h", 1)
            .subscriptions(subs)
            .to_request();
        let info = req.pubsub_subscriptions.expect("subscriptions set");
        assert!(
            info.get(&redis::PubSubSubscriptionKind::Sharded)
                .unwrap()
                .contains(&b"shard-ch".to_vec())
        );
    }

    // ---- full end-to-end lowering ---------------------------------------

    #[test]
    fn standalone_request_full() {
        let cfg =
            GlideClientConfiguration::new(vec![NodeAddress::new("a", 1), NodeAddress::new("b", 2)])
                .tls(TlsConfig::SecureTls)
                .credentials(ServerCredentials::new("user", "pass"))
                .read_from(ReadFrom::PreferReplica)
                .protocol(ProtocolVersion::RESP2)
                .database_id(3)
                .client_name("myclient")
                .request_timeout(Duration::from_millis(500))
                .lazy_connect(true);
        let req = cfg.to_request();
        assert_eq!(req.addresses.len(), 2);
        assert_eq!(req.tls_mode, Some(TlsMode::SecureTls));
        assert_eq!(req.read_from, Some(CoreReadFrom::PreferReplica));
        assert_eq!(req.protocol, Some(redis::ProtocolVersion::RESP2));
        assert_eq!(req.database_id, 3);
        assert_eq!(req.client_name.as_deref(), Some("myclient"));
        assert_eq!(req.request_timeout, Some(500));
        assert!(req.lazy_connect);
        assert!(!req.cluster_mode_enabled);
        let auth = req.authentication_info.expect("auth set");
        assert_eq!(auth.username.as_deref(), Some("user"));
        assert_eq!(auth.password.as_deref(), Some("pass"));
    }

    #[test]
    fn cluster_request_full() {
        let cfg = GlideClusterClientConfiguration::with_address("seed", 7000)
            .tls(TlsConfig::InsecureTls)
            .credentials(ServerCredentials::password("p"))
            .read_from(ReadFrom::AZAffinity("use1-az1".into()))
            .protocol(ProtocolVersion::RESP2)
            .client_name("c")
            .periodic_checks(PeriodicChecks::ManualInterval(12))
            .request_timeout(Duration::from_millis(100))
            .lazy_connect(true);
        let req = cfg.to_request();
        assert!(req.cluster_mode_enabled);
        assert_eq!(req.tls_mode, Some(TlsMode::InsecureTls));
        assert_eq!(
            req.read_from,
            Some(CoreReadFrom::AZAffinity("use1-az1".into()))
        );
        assert_eq!(req.protocol, Some(redis::ProtocolVersion::RESP2));
        assert_eq!(req.client_name.as_deref(), Some("c"));
        assert_eq!(req.request_timeout, Some(100));
        assert!(req.lazy_connect);
        match req.periodic_checks {
            Some(PeriodicCheck::ManualInterval(d)) => assert_eq!(d, Duration::from_secs(12)),
            other => panic!("unexpected periodic checks: {other:?}"),
        }
        let auth = req.authentication_info.expect("auth set");
        assert!(auth.username.is_none());
        assert_eq!(auth.password.as_deref(), Some("p"));
    }

    // ---- redis-rs URL parity (`from_url` / `from_urls`) ----

    #[test]
    fn from_url_basic() {
        let cfg = GlideClientConfiguration::from_url("redis://localhost:6380").unwrap();
        assert_eq!(cfg.addresses.len(), 1);
        assert_eq!(cfg.addresses[0].host, "localhost");
        assert_eq!(cfg.addresses[0].port, 6380);
        assert_eq!(cfg.tls, TlsConfig::NoTls);
        assert_eq!(cfg.database_id, 0);
        assert!(cfg.credentials.is_none());
    }

    #[test]
    fn from_url_default_port_and_db() {
        let cfg = GlideClientConfiguration::from_url("redis://example.com/3").unwrap();
        assert_eq!(cfg.addresses[0].port, 6379);
        assert_eq!(cfg.database_id, 3);
    }

    #[test]
    fn from_url_credentials() {
        let cfg = GlideClientConfiguration::from_url("redis://user:secret@h:1234").unwrap();
        let creds = cfg.credentials.expect("credentials parsed");
        assert_eq!(creds.username.as_deref(), Some("user"));
        assert_eq!(creds.password.as_deref(), Some("secret"));

        // Password-only (empty username) form.
        let cfg = GlideClientConfiguration::from_url("redis://:secret@h:1234").unwrap();
        let creds = cfg.credentials.expect("credentials parsed");
        assert!(creds.username.is_none());
        assert_eq!(creds.password.as_deref(), Some("secret"));
    }

    #[test]
    fn from_url_tls_schemes() {
        let cfg = GlideClientConfiguration::from_url("rediss://secure-host:6379").unwrap();
        assert_eq!(cfg.tls, TlsConfig::SecureTls);

        let cfg =
            GlideClientConfiguration::from_url("rediss://secure-host:6379/#insecure").unwrap();
        assert_eq!(cfg.tls, TlsConfig::InsecureTls);
    }

    #[test]
    fn from_url_invalid_rejected() {
        assert!(GlideClientConfiguration::from_url("not a url").is_err());
        assert!(GlideClientConfiguration::from_url("http://host").is_err());
        assert!(GlideClientConfiguration::from_url("redis+unix:///tmp/redis.sock").is_err());
    }

    #[test]
    fn from_urls_cluster_multiple_seeds() {
        let cfg =
            GlideClusterClientConfiguration::from_urls(["redis://n1:7000", "redis://n2:7001"])
                .unwrap();
        assert_eq!(cfg.addresses.len(), 2);
        assert_eq!(cfg.addresses[1].host, "n2");
        assert_eq!(cfg.addresses[1].port, 7001);
    }

    #[test]
    fn from_urls_cluster_rejects_db_and_empty() {
        assert!(GlideClusterClientConfiguration::from_urls(["redis://n1:7000/5"]).is_err());
        assert!(GlideClusterClientConfiguration::from_urls(Vec::<&str>::new()).is_err());
        // A non-zero db on any URL (not just the first) is rejected.
        assert!(
            GlideClusterClientConfiguration::from_urls(["redis://n1:7000", "redis://n2:7001/5"])
                .is_err()
        );
    }

    #[test]
    fn from_urls_cluster_rejects_conflicting_settings() {
        // Matches the fork's `ClusterClient` validation: settings must be
        // identical across all initial-node URLs.
        assert!(
            GlideClusterClientConfiguration::from_urls([
                "redis://:pw1@n1:7000",
                "redis://:pw2@n2:7001",
            ])
            .is_err(),
            "different passwords must be rejected"
        );
        assert!(
            GlideClusterClientConfiguration::from_urls([
                "redis://u1:pw@n1:7000",
                "redis://u2:pw@n2:7001",
            ])
            .is_err(),
            "different usernames must be rejected"
        );
        assert!(
            GlideClusterClientConfiguration::from_urls(["redis://n1:7000", "rediss://n2:7001"])
                .is_err(),
            "mixed TLS modes must be rejected"
        );
        // Identical settings on all URLs remain accepted.
        assert!(
            GlideClusterClientConfiguration::from_urls([
                "redis://u:pw@n1:7000",
                "redis://u:pw@n2:7001",
            ])
            .is_ok()
        );
    }

    // ---- mutual TLS lowering ----

    #[test]
    fn client_identity_lowered_into_request() {
        let cert = b"-----BEGIN CERTIFICATE-----".to_vec();
        let key = b"-----BEGIN PRIVATE KEY-----".to_vec();
        let req = GlideClientConfiguration::with_address("h", 6379)
            .tls(TlsConfig::SecureTls)
            .client_identity(cert.clone(), key.clone())
            .to_request();
        assert_eq!(req.client_cert, cert);
        assert_eq!(req.client_key, key);

        let req = GlideClusterClientConfiguration::with_address("h", 7000)
            .tls(TlsConfig::SecureTls)
            .client_identity(cert.clone(), key.clone())
            .to_request();
        assert_eq!(req.client_cert, cert);
        assert_eq!(req.client_key, key);
    }

    #[test]
    fn no_client_identity_leaves_request_empty() {
        let req = GlideClientConfiguration::with_address("h", 6379).to_request();
        assert!(req.client_cert.is_empty());
        assert!(req.client_key.is_empty());
    }

    #[test]
    fn client_identity_debug_redacts_private_key() {
        let key_material = "SUPER-SECRET-KEY-MATERIAL";
        let cfg = GlideClientConfiguration::with_address("h", 6379)
            .client_identity(b"cert-bytes".to_vec(), key_material.as_bytes().to_vec());
        // Both the identity's own Debug and the config's derived Debug must
        // redact the private key.
        let identity_dbg = format!("{:?}", cfg.client_identity.as_ref().unwrap());
        let config_dbg = format!("{cfg:?}");
        for rendered in [&identity_dbg, &config_dbg] {
            assert!(
                rendered.contains("<redacted>"),
                "missing redaction: {rendered}"
            );
            assert!(
                !rendered.contains(key_material)
                    && !rendered.contains(&format!("{:?}", key_material.as_bytes())),
                "private key leaked through Debug: {rendered}"
            );
        }
    }
}

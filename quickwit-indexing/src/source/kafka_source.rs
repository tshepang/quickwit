// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::KafkaSourceParams;
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{
    BaseConsumer, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde_json::json;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio::time;
use tracing::{debug, info, warn};

use crate::actors::Indexer;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, SourceExecutionContext, TypedSourceFactory};

/// We try to emit chewable batches for the indexer.
/// One batch = one message to the indexer actor.
///
/// If batches are too large:
/// - we might not be able to observe the state of the indexer for 5 seconds.
/// - we will be needlessly occupying resident memory in the mailbox.
/// - we will not have a precise control of the timeout before commit.
///
/// 5MB seems like a good one size fits all value.
const TARGET_BATCH_NUM_BYTES: u64 = 5_000_000;

/// Factory for instantiating a `KafkaSource`.
pub struct KafkaSourceFactory;

#[async_trait]
impl TypedSourceFactory for KafkaSourceFactory {
    type Source = KafkaSource;
    type Params = KafkaSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: KafkaSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        KafkaSource::try_new(ctx, params, checkpoint).await
    }
}

enum RebalanceEvent {
    Starting,
    Assignments(HashMap<i32, PartitionId>),
}
struct RdKafkaContext {
    ctx: Arc<SourceExecutionContext>,
    rebalance_events: mpsc::Sender<RebalanceEvent>,
}

impl ClientContext for RdKafkaContext {}

impl ConsumerContext for RdKafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
        let source_id = self.ctx.config.source_id.clone();

        let mut assignments = HashMap::new();

        // TODO: need to also handle Rebalance::Revoke
        if let &Rebalance::Assign(tpl) = rebalance {
            // The rebalance API is not defined to be async so this attempts a work around
            // by explicitly blocking the thread handling the rebalance callback until the
            // database action is complete
            let handle = Handle::current();
            let _guard = handle.enter();
            let result = futures::executor::block_on(async {
                // This is intended to invalidate the in progress split. Since partitions may be
                // moving away, any in progress state will be invalid and a new split would need
                // to be started from the most recently committed offsets.
                // TODO: this can throw an error
                let _result = self.rebalance_events.send(RebalanceEvent::Starting).await;

                self.ctx.metastore.index_metadata(&self.ctx.index_id).await
            });

            let index_metadata = match result {
                Ok(index_metadata) => index_metadata,
                Err(_err) => {
                    // TODO: a panic here may not be the right thing
                    panic!(
                        "No Index metadata found for {}: this should never happen.",
                        source_id
                    );
                }
            };

            let source_checkpoint = index_metadata.checkpoint.source_checkpoint(&source_id);

            let checkpoint = match source_checkpoint {
                Some(checkpoint) => checkpoint.clone(),
                None => {
                    warn!("Source checkpoint doesn't exist for {}", source_id);
                    SourceCheckpoint::default()
                }
            };

            // elements() will give you the list of partitions being assigned
            let partitions = tpl.elements();
            for entry in partitions.iter() {
                // Need to use find_partition to get a mutable reference.
                let mut partition = tpl
                    .find_partition(entry.topic(), entry.partition())
                    .expect("Consumer rebalance unknown partition");

                let partition_id = PartitionId::from(entry.partition() as i64);

                assignments.insert(entry.partition(), partition_id.clone());

                // Offsets must be tracked per partition
                let offset = match checkpoint.position_for_partition(&partition_id) {
                    Some(position) => match position {
                        Position::Offset(offset_string) => {
                            let numeric_offset =
                                offset_string.parse::<i64>().expect("Invalid stored offset");

                            if numeric_offset < 0 {
                                Offset::Beginning
                            } else {
                                Offset::Offset(numeric_offset + 1)
                            }
                        }
                        Position::Beginning => Offset::Beginning,
                    },
                    None => Offset::Beginning,
                };

                debug!(
                    "Setting offsets for {} {}, {}, {:?}",
                    source_id,
                    entry.topic(),
                    entry.partition(),
                    offset
                );

                // set_offset will move the in memory offset on the client
                // If the offset is invalid at this point the standard consumer group offset reset
                // mechanism will apply to handle it.
                partition
                    .set_offset(offset)
                    .expect("Failure setting offset");
            }
        }

        let handle = Handle::current();
        let _guard = handle.enter();
        let _result = futures::executor::block_on(
            self.rebalance_events
                .send(RebalanceEvent::Assignments(assignments)),
        );
        // TODO: handle the sending error.
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

#[derive(Debug)]
pub enum NumActivePartitions {
    Initializing,
    Some(usize),
}

impl NumActivePartitions {
    fn dec(&mut self) {
        match self {
            Self::Initializing => panic!("We should not decrement the number of active partitions while initializing the source. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."),
            Self::Some(ref mut counter) => *counter -= 1,
        }
    }

    fn is_zero(&self) -> bool {
        matches!(self, Self::Some(0))
    }

    fn json(&self) -> serde_json::Value {
        match self {
            Self::Initializing => json!(null),
            Self::Some(ref counter) => json!(counter),
        }
    }
}

impl Default for NumActivePartitions {
    fn default() -> Self {
        Self::Initializing
    }
}

type RdKafkaConsumer = StreamConsumer<RdKafkaContext>;

#[derive(Default)]
pub struct KafkaSourceState {
    /// Partitions IDs assigned to the source.
    pub assigned_partition_ids: HashMap<i32, PartitionId>,
    /// Offset for each partition of the last message received.
    pub current_positions: HashMap<i32, Position>,
    /// Number of active partitions, i.e., that have not reached EOF.
    pub num_active_partitions: NumActivePartitions,
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    pub num_invalid_messages: u64,
}

/// A `KafkaSource` consumes a topic and forwards its messages to an `Indexer`.
pub struct KafkaSource {
    ctx: Arc<SourceExecutionContext>,
    topic: String,
    consumer: Arc<RdKafkaConsumer>,
    state: KafkaSourceState,
    rebalance_events: mpsc::Receiver<RebalanceEvent>,
    backfill_mode_enabled: bool,
}

impl fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KafkaSource {{ source_id: {}, topic: {} }}",
            self.ctx.config.source_id, self.topic
        )
    }
}

impl KafkaSource {
    /// Instantiates a new `KafkaSource`.
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: KafkaSourceParams,
        _checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let topic = params.topic.clone();
        let backfill_mode_enabled = params.enable_backfill_mode;

        let (rebalance_sender, rebalance_receiver) = mpsc::channel(32);

        let consumer = create_consumer(ctx.clone(), params, rebalance_sender)?;

        info!(
            topic = %topic,
            "Starting Kafka source."
        );
        consumer
            .subscribe(&[topic.as_str()])
            .context("Failed to resume from checkpoint.")?;

        let state = KafkaSourceState {
            ..Default::default()
        };
        Ok(KafkaSource {
            ctx: ctx.clone(),
            topic,
            consumer,
            state,
            rebalance_events: rebalance_receiver,
            backfill_mode_enabled,
        })
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn emit_batches(
        &mut self,
        indexer_mailbox: &Mailbox<Indexer>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let mut batch_num_bytes = 0;
        let mut docs = Vec::new();
        let mut checkpoint_delta = SourceCheckpointDelta::default();

        let deadline = time::sleep(quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                message = self.rebalance_events.recv() => {
                    match message {
                        Some(event) => {
                            match event {
                                RebalanceEvent::Starting => {
                                    // TODO: on a rebalance we need to stop processing and invalidate
                                    // the in progress split.
                                },
                                RebalanceEvent::Assignments(assignments) => {
                                    debug!("Received partition assignments {:?}", assignments);
                                    self.state.num_active_partitions = NumActivePartitions::Some(assignments.len());
                                    self.state.assigned_partition_ids = assignments;
                                }
                            }
                        },
                        None => {}
                    }
                },
                message_res = self.consumer.recv() => {
                    let message = match message_res {
                        Ok(message) => message,
                        Err(KafkaError::PartitionEOF(partition_id)) => {
                            self.state.num_active_partitions.dec();
                            info!(
                                topic = %self.topic,
                                partition_id = ?partition_id,
                                num_active_partitions = ?self.state.num_active_partitions,
                                "Reached end of partition."
                            );
                            continue;
                        }
                        // FIXME: This is assuming that Kafka errors are not recoverable, it may not be the
                        // case.
                        Err(err) => return Err(ActorExitStatus::from(anyhow::anyhow!(err))),
                    };
                    if let Some(doc) = parse_message_payload(&message) {
                        docs.push(doc);
                    } else {
                        self.state.num_invalid_messages += 1;
                    }
                    batch_num_bytes += message.payload_len() as u64;
                    self.state.num_bytes_processed += message.payload_len() as u64;
                    self.state.num_messages_processed += 1;

                    let partition_id = self
                        .state
                        .assigned_partition_ids
                        .get(&message.partition())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Received message from unassigned partition `{}`. Assigned partitions: \
                                 `{{{}}}`.",
                                message.partition(),
                                self.state.assigned_partition_ids.keys().join(", "),
                            )
                        })?
                        .clone();
                    let current_position = Position::from(message.offset());
                    let previous_position = self
                        .state
                        .current_positions
                        .insert(message.partition(), current_position.clone())
                        .unwrap_or_else(|| previous_position_for_offset(message.offset()));
                    checkpoint_delta
                        .record_partition_delta(partition_id, previous_position, current_position)
                        .context("Failed to record partition delta.")?;

                    if batch_num_bytes >= TARGET_BATCH_NUM_BYTES {
                        break;
                    }
                    ctx.record_progress();
                }
                _ = &mut deadline => {
                    break;
                }
            }
        }
        if !checkpoint_delta.is_empty() {
            let batch = RawDocBatch {
                docs,
                checkpoint_delta,
            };
            ctx.send_message(indexer_mailbox, batch).await?;
        }
        if self.backfill_mode_enabled && self.state.num_active_partitions.is_zero() {
            info!(topic = %self.topic, "Reached end of topic.");
            ctx.send_exit_with_success(indexer_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        debug!("batch complete, waiting for next iteration");
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("KafkaSource{{source_id={}}}", self.ctx.config.source_id)
    }

    fn observable_state(&self) -> serde_json::Value {
        let assigned_partition_ids: Vec<&i32> =
            self.state.assigned_partition_ids.keys().sorted().collect();
        let current_positions: Vec<(&i32, i64)> = self
            .state
            .current_positions
            .iter()
            .filter_map(|(partition_id, position)| match position {
                Position::Offset(offset_str) => offset_str
                    .parse::<i64>()
                    .ok()
                    .map(|offset| (partition_id, offset)),
                Position::Beginning => None,
            })
            .sorted()
            .collect();
        json!({
            "topic": self.topic,
            "assigned_partition_ids": assigned_partition_ids,
            "current_positions": current_positions,
            "num_active_partitions": self.state.num_active_partitions.json(),
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
        })
    }
}

/// Returns the preceding `Position` for the offset.
fn previous_position_for_offset(offset: i64) -> Position {
    if offset == 0 {
        Position::Beginning
    } else {
        Position::from(offset - 1)
    }
}

/// Checks whether we can establish a connection to the Kafka broker.
pub(super) async fn check_connectivity(params: KafkaSourceParams) -> anyhow::Result<()> {
    let mut client_config = parse_client_params(params.client_params)?;

    let consumer: BaseConsumer<DefaultConsumerContext> = client_config
        .set("group.id", "quickwit-connectivity-check".to_string())
        .set_log_level(RDKafkaLogLevel::Error)
        .create()?;

    let topic = params.topic.clone();
    let timeout = Timeout::After(Duration::from_secs(5));
    let cluster_metadata = spawn_blocking(move || {
        consumer
            .fetch_metadata(Some(&topic), timeout)
            .with_context(|| format!("Failed to fetch metadata for topic `{}`.", topic))
    })
    .await??;

    if cluster_metadata.topics().is_empty() {
        bail!("Topic `{}` does not exist.", params.topic);
    }

    let topic_metadata = &cluster_metadata.topics()[0];
    assert_eq!(topic_metadata.name(), params.topic); // Belt and suspenders.

    if topic_metadata.partitions().is_empty() {
        bail!("Topic `{}` has no partitions.", params.topic);
    }

    Ok(())
}

/// Creates a new `KafkaSourceConsumer`.
fn create_consumer(
    ctx: Arc<SourceExecutionContext>,
    params: KafkaSourceParams,
    rebalance_sender: mpsc::Sender<RebalanceEvent>,
) -> anyhow::Result<Arc<RdKafkaConsumer>> {
    let mut client_config = parse_client_params(params.client_params)?;

    // Group ID is limited to 255 characters.
    let mut group_id = format!("quickwit-{}", ctx.config.source_id);
    group_id.truncate(255);
    debug!("Initializing consumer for group_id {}", group_id);

    let log_level = parse_client_log_level(params.client_log_level)?;
    let consumer: RdKafkaConsumer = client_config
        .set("group.id", group_id)
        .set_log_level(log_level)
        .create_with_context(RdKafkaContext {
            ctx,
            rebalance_events: rebalance_sender,
        })
        .context("Failed to create Kafka consumer.")?;

    Ok(Arc::new(consumer))
}

fn parse_client_log_level(client_log_level: Option<String>) -> anyhow::Result<RDKafkaLogLevel> {
    let log_level = match client_log_level
        .map(|log_level| log_level.to_lowercase())
        .as_deref()
    {
        Some("debug") => RDKafkaLogLevel::Debug,
        Some("info") | None => RDKafkaLogLevel::Info,
        Some("warn") | Some("warning") => RDKafkaLogLevel::Warning,
        Some("error") => RDKafkaLogLevel::Error,
        Some(level) => bail!(
            "Failed to parse Kafka client log level. Value `{}` is not supported.",
            level
        ),
    };
    Ok(log_level)
}

fn parse_client_params(client_params: serde_json::Value) -> anyhow::Result<ClientConfig> {
    let params = if let serde_json::Value::Object(params) = client_params {
        params
    } else {
        bail!("Failed to parse Kafka client parameters. `client_params` must be a JSON object.");
    };
    let mut client_config = ClientConfig::new();
    for (key, value_json) in params {
        let value = match value_json {
            serde_json::Value::Bool(value_bool) => value_bool.to_string(),
            serde_json::Value::Number(value_number) => value_number.to_string(),
            serde_json::Value::String(value_string) => value_string,
            serde_json::Value::Null => continue,
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => bail!(
                "Failed to parse Kafka client parameters. `client_params.{}` must be a boolean, \
                 number, or string.",
                key
            ),
        };
        client_config.set(key, value);
    }
    // We manage offsets ourselves: we always want this value to be `false`.
    client_config.set("enable.auto.commit", "false");
    Ok(client_config)
}

/// Converts the raw bytes of the message payload to a `String` skipping corrupted or empty
/// messages.
fn parse_message_payload(message: &BorrowedMessage) -> Option<String> {
    match message.payload_view::<str>() {
        Some(Ok(payload)) if !payload.is_empty() => {
            let doc = payload.to_string();
            debug!(
                topic = ?message.topic(),
                partition_id = ?message.partition(),
                offset = ?message.offset(),
                timestamp = ?message.timestamp(),
                num_bytes = ?message.payload_len(),
                "Message received.",
            );
            return Some(doc);
        }
        Some(Ok(_)) => debug!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            "Document is empty."
        ),
        Some(Err(error)) => warn!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            error = ?error,
            "Failed to deserialize message payload."
        ),
        None => debug!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            "Message payload is empty."
        ),
    }
    None
}

#[cfg(all(test, feature = "kafka-broker-tests"))]
mod kafka_broker_tests {
    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::uri::Uri;
    use quickwit_config::{DocMapping, SourceConfig, SourceParams};
    use quickwit_metastore::checkpoint::{
        IndexCheckpoint, IndexCheckpointDelta, SourceCheckpointDelta,
    };
    use quickwit_metastore::{FileBackedMetastore, IndexMetadata, Metastore, MetastoreResult};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::message::ToBytes;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    use super::*;
    use crate::source::{quickwit_supported_sources, source_factory, SourceActor};

    fn create_admin_client(
        bootstrap_servers: &str,
    ) -> anyhow::Result<AdminClient<DefaultClientContext>> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .create()?;
        Ok(admin_client)
    }

    async fn create_topic(
        admin_client: &AdminClient<DefaultClientContext>,
        topic: &str,
        num_partitions: i32,
    ) -> anyhow::Result<()> {
        admin_client
            .create_topics(
                &[NewTopic::new(
                    topic,
                    num_partitions,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
            )
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|(topic, err_code)| {
                anyhow::anyhow!(
                    "Failed to create topic `{}`. Error code: `{}`",
                    topic,
                    err_code
                )
            })?;
        Ok(())
    }

    async fn populate_topic<K, M, J, Q>(
        bootstrap_servers: &str,
        topic: &str,
        num_messages: i32,
        key_fn: &K,
        message_fn: &M,
        partition: Option<i32>,
        timestamp: Option<i64>,
    ) -> anyhow::Result<HashMap<(i32, i64), i32>>
    where
        K: Fn(i32) -> J,
        M: Fn(i32) -> Q,
        J: ToBytes,
        Q: ToBytes,
    {
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("statistics.interval.ms", "500")
            .set("api.version.request", "true")
            .set("debug", "all")
            .set("message.timeout.ms", "30000")
            .create()?;
        let tasks = (0..num_messages).map(|id| async move {
            producer
                .send(
                    FutureRecord {
                        topic,
                        partition,
                        timestamp,
                        key: Some(&key_fn(id)),
                        payload: Some(&message_fn(id)),
                        headers: None,
                    },
                    Duration::from_secs(1),
                )
                .await
                .map(|(partition, offset)| (id, partition, offset))
                .map_err(|(err, _)| err)
        });
        let message_map = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .fold(HashMap::new(), |mut acc, (id, partition, offset)| {
                acc.insert((partition, offset), id);
                acc
            });
        Ok(message_map)
    }

    fn key_fn(id: i32) -> String {
        format!("Key {}", id)
    }

    fn merge_doc_batches(batches: Vec<RawDocBatch>) -> anyhow::Result<RawDocBatch> {
        let mut merged_batch = RawDocBatch::default();
        for batch in batches {
            merged_batch.docs.extend(batch.docs);
            merged_batch
                .checkpoint_delta
                .extend(batch.checkpoint_delta)?;
        }
        merged_batch.docs.sort();
        Ok(merged_batch)
    }

    async fn create_test_index(metastore: Arc<FileBackedMetastore>) -> MetastoreResult<()> {
        metastore
            .create_index(IndexMetadata {
                index_id: "test-index".to_string(),
                index_uri: Uri::new("s3://localhost/test-index".to_string()),
                checkpoint: IndexCheckpoint::default(),
                doc_mapping: DocMapping {
                    field_mappings: vec![],
                    tag_fields: Default::default(),
                    store_source: false,
                    mode: Default::default(),
                    dynamic_mapping: None,
                    partition_key: "".to_string(),
                },
                indexing_settings: Default::default(),
                search_settings: Default::default(),
                sources: Default::default(),
                create_timestamp: 0,
                update_timestamp: 0,
            })
            .await
    }

    #[tokio::test]
    async fn test_kafka_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::new();

        let bootstrap_servers = "localhost:9092".to_string();
        let topic = append_random_suffix("test-kafka-source-topic");

        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 3).await?;

        let source_config = SourceConfig {
            source_id: "test-kafka-source".to_string(),
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: topic.clone(),
                client_log_level: None,
                client_params: json!({
                    "bootstrap.servers": bootstrap_servers,
                    "enable.partition.eof": true,
                }),
                enable_backfill_mode: true,
            }),
        };

        let source_loader = quickwit_supported_sources();
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint = SourceCheckpoint::default();
            let metastore = Arc::new(source_factory::test_helpers::metastore_for_test().await);

            create_test_index(metastore.clone()).await?;

            let source = source_loader
                .load_source(
                    Arc::new(SourceExecutionContext {
                        metastore,
                        config: source_config.clone(),
                        index_id: "test-index".to_string(),
                    }),
                    checkpoint,
                )
                .await?;
            let actor = SourceActor {
                source,
                indexer_mailbox: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn();
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|msg_any| msg_any.downcast::<RawDocBatch>().ok())
                .map(|boxed_msg| *boxed_msg)
                .collect();

            assert!(messages.is_empty());

            let expected_current_positions: Vec<(i32, i64)> = vec![];
            let expected_state = json!({
                "topic":  topic,
                "assigned_partition_ids": vec![0u64, 1u64, 2u64],
                "current_positions":  expected_current_positions,
                "num_active_partitions": 0u64,
                "num_bytes_processed": 0u64,
                "num_messages_processed": 0u64,
                "num_invalid_messages": 0u64,
            });
            assert_eq!(exit_state, expected_state);
        }
        for partition_id in 0..3 {
            populate_topic(
                &bootstrap_servers,
                &topic,
                3,
                &key_fn,
                &|message_id| {
                    if message_id == 1 {
                        "".to_string()
                    } else {
                        format!("Message #{:0>3}", partition_id * 100 + message_id)
                    }
                },
                Some(partition_id),
                None,
            )
            .await?;
        }
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint = SourceCheckpoint::default();
            let metastore = Arc::new(source_factory::test_helpers::metastore_for_test().await);

            create_test_index(metastore.clone()).await?;

            let source = source_loader
                .load_source(
                    Arc::new(SourceExecutionContext {
                        metastore,
                        config: source_config.clone(),
                        index_id: "test-index".to_string(),
                    }),
                    checkpoint,
                )
                .await?;
            let actor = SourceActor {
                source,
                indexer_mailbox: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn();
            let (exit_status, state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .collect();
            assert!(messages.len() >= 1);

            let batch = merge_doc_batches(messages)?;
            let expected_docs = vec![
                "Message #000",
                "Message #002",
                "Message #100",
                "Message #102",
                "Message #200",
                "Message #202",
            ];
            assert_eq!(batch.docs, expected_docs);

            let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
            for partition in 0u64..3u64 {
                expected_checkpoint_delta.record_partition_delta(
                    PartitionId::from(partition),
                    Position::Beginning,
                    Position::from(2u64),
                )?;
            }
            assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);

            let expected_state = json!({
                "topic":  topic,
                "assigned_partition_ids": vec![0u64, 1u64, 2u64],
                "current_positions":  vec![(0u32, 2u64), (1u32, 2u64), (2u32, 2u64)],
                "num_active_partitions": 0usize,
                "num_bytes_processed": 72u64,
                "num_messages_processed": 9u64,
                "num_invalid_messages": 3u64,
            });
            assert_eq!(state, expected_state);
        }
        {
            let (sink, inbox) = create_test_mailbox();
            let mut source_delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from(0u64),
                Position::from(0u64),
                Position::from(0u64),
            );
            source_delta
                .record_partition_delta(
                    PartitionId::from(1u64),
                    Position::from(0u64),
                    Position::from(2u64),
                )
                .unwrap();

            let index_delta = IndexCheckpointDelta {
                source_id: source_config.source_id.clone(),
                source_delta,
            };
            let metastore = Arc::new(source_factory::test_helpers::metastore_for_test().await);
            create_test_index(metastore.clone()).await?;

            metastore
                .publish_splits(
                    "test-index",
                    &[&source_config.source_id.clone()],
                    &[],
                    Some(index_delta),
                )
                .await
                .unwrap();

            let source = source_loader
                .load_source(
                    Arc::new(SourceExecutionContext {
                        metastore,
                        config: source_config.clone(),
                        index_id: "test-index".to_string(),
                    }),
                    SourceCheckpoint::default(),
                )
                .await?;
            let actor = SourceActor {
                source,
                indexer_mailbox: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn();
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages: Vec<RawDocBatch> = inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_message| box_message.downcast::<RawDocBatch>())
                .map(|box_raw_batch| *box_raw_batch)
                .collect();
            assert!(messages.len() >= 1);

            let batch = merge_doc_batches(messages)?;
            let expected_docs = vec!["Message #002", "Message #200", "Message #202"];
            assert_eq!(batch.docs, expected_docs);

            let mut expected_checkpoint_delta = SourceCheckpointDelta::default();
            expected_checkpoint_delta.record_partition_delta(
                PartitionId::from(0u64),
                Position::from(0u64),
                Position::from(2u64),
            )?;
            expected_checkpoint_delta.record_partition_delta(
                PartitionId::from(2u64),
                Position::Beginning,
                Position::from(2u64),
            )?;
            assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta,);

            let expected_exit_state = json!({
                "topic":  topic,
                "assigned_partition_ids": vec![0u64, 1u64, 2u64],
                "current_positions":  vec![(0u64, 2u64), (2u64, 2u64)],
                "num_active_partitions": 0usize,
                "num_bytes_processed": 36u64,
                "num_messages_processed": 5u64,
                "num_invalid_messages": 2u64,
            });
            assert_eq!(exit_state, expected_exit_state);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_kafka_connectivity() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let bootstrap_servers = "localhost:9092".to_string();
        let topic = append_random_suffix("test-kafka-connectivity-topic");

        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 1).await?;

        // Check valid connectivity
        let result = check_connectivity(KafkaSourceParams {
            topic: topic.clone(),
            client_log_level: None,
            client_params: json!({ "bootstrap.servers": bootstrap_servers }),
            enable_backfill_mode: true,
        })
        .await?;

        assert_eq!(result, ());

        // TODO: these tests should be checking the specific errors.
        // Non existent topic should throw an error.
        let result = check_connectivity(KafkaSourceParams {
            topic: "non-existent-topic".to_string(),
            client_log_level: None,
            client_params: json!({ "bootstrap.servers": bootstrap_servers }),
            enable_backfill_mode: true,
        })
        .await;

        assert!(result.is_err());

        // Invalid brokers should throw an error
        let result = check_connectivity(KafkaSourceParams {
            topic: topic.clone(),
            client_log_level: None,
            client_params: json!({
                "bootstrap.servers": "192.0.2.10:9092"
            }),
            enable_backfill_mode: true,
        })
        .await;

        assert!(result.is_err());

        Ok(())
    }
}

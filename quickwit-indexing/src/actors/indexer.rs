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

use std::collections::hash_map::Entry;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use async_trait::async_trait;
use fail::fail_point;
use fnv::FnvHashMap;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::IndexingSettings;
use quickwit_control_plane::MetastoreService;
use quickwit_doc_mapper::{DocMapper, DocParsingError, SortBy, QUICKWIT_TOKENIZER_MANAGER};
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_proto::metastore_api::PublishSplitsRequest;
use tantivy::schema::{Field, Schema, Value};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::{Document, IndexBuilder, IndexSettings, IndexSortByField};
use tokio::runtime::Handle;
use tracing::{info, warn};
use ulid::Ulid;

use crate::actors::Packager;
use crate::models::{IndexedSplit, IndexedSplitBatch, IndexingDirectory, RawDocBatch};

#[derive(Debug)]
struct CommitTimeout {
    workbench_id: Ulid,
}

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct IndexerCounters {
    /// Overall number of documents received, partitioned
    /// into 3 categories:
    /// - number docs that did not parse correctly.
    /// - number docs missing a timestamp (if the index has no timestamp,
    /// then this counter is 0)
    /// - number of valid docs.
    pub num_parse_errors: u64,
    pub num_missing_fields: u64,
    pub num_valid_docs: u64,

    /// Number of splits that were emitted by the indexer.
    pub num_splits_emitted: u64,

    /// Number of split batches that were emitted by the indexer.
    pub num_split_batches_emitted: u64,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub overall_num_bytes: u64,

    /// Number of (valid) documents in the current workbench.
    /// This value is used to trigger commit and for observation.
    pub num_docs_in_workbench: u64,
}

impl IndexerCounters {
    /// Returns the overall number of docs that went through the indexer (valid or not).
    pub fn num_processed_docs(&self) -> u64 {
        self.num_valid_docs + self.num_parse_errors + self.num_missing_fields
    }

    /// Returns the overall number of docs that were sent to the indexer but were invalid.
    /// (For instance, because they were missing a required field or because their because
    /// their format was invalid)
    pub fn num_invalid_docs(&self) -> u64 {
        self.num_parse_errors + self.num_missing_fields
    }
}

struct IndexerState {
    index_id: String,
    source_id: String,
    doc_mapper: Arc<dyn DocMapper>,
    indexing_directory: IndexingDirectory,
    indexing_settings: IndexingSettings,
    timestamp_field_opt: Option<Field>,
    schema: Schema,
    index_settings: IndexSettings,
}

enum PrepareDocumentOutcome {
    ParsingError,
    MissingField,
    Document {
        document: Document,
        timestamp_opt: Option<i64>,
        partition: u64,
    },
}

impl IndexerState {
    fn create_indexed_split(&self, ctx: &ActorContext<Indexer>) -> anyhow::Result<IndexedSplit> {
        let index_builder = IndexBuilder::new()
            .settings(self.index_settings.clone())
            .schema(self.schema.clone())
            .tokenizers(QUICKWIT_TOKENIZER_MANAGER.clone());
        let indexed_split = IndexedSplit::new_in_dir(
            self.index_id.clone(),
            self.indexing_directory.scratch_directory.clone(),
            self.indexing_settings.resources.clone(),
            index_builder,
            ctx.progress().clone(),
            ctx.kill_switch().clone(),
        )?;
        info!(split_id = %indexed_split.split_id, "new-split");
        Ok(indexed_split)
    }

    fn get_or_create_indexed_split<'a>(
        &self,
        partition: u64,
        splits: &'a mut FnvHashMap<u64, IndexedSplit>,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexedSplit> {
        match splits.entry(partition) {
            Entry::Occupied(indexed_split) => Ok(indexed_split.into_mut()),
            Entry::Vacant(vacant_entry) => {
                let indexed_split = self.create_indexed_split(ctx)?;
                Ok(vacant_entry.insert(indexed_split))
            }
        }
    }

    fn create_workbench(&self) -> anyhow::Result<IndexingWorkbench> {
        let workbench = IndexingWorkbench {
            checkpoint_delta: IndexCheckpointDelta {
                source_id: self.source_id.clone(),
                source_delta: SourceCheckpointDelta::default(),
            },
            indexed_splits: FnvHashMap::with_capacity_and_hasher(250, Default::default()),
            workbench_id: Ulid::new(),
            date_of_birth: Instant::now(),
        };
        Ok(workbench)
    }

    /// Returns the current_indexed_split. If this is the first message, then
    /// the indexed_split does not exist yet.
    ///
    /// This function will then create it, and can hence return an Error.
    async fn get_or_create_workbench<'a, 'b>(
        &self,
        indexing_workbench_opt: &'a mut Option<IndexingWorkbench>,
        ctx: &'b ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexingWorkbench> {
        if indexing_workbench_opt.is_none() {
            let indexing_workbench = self.create_workbench()?;
            let commit_timeout_message = CommitTimeout {
                workbench_id: indexing_workbench.workbench_id,
            };
            ctx.schedule_self_msg(
                self.indexing_settings.commit_timeout(),
                commit_timeout_message,
            )
            .await;
            *indexing_workbench_opt = Some(indexing_workbench);
        }
        let current_indexing_workbench: &'a mut IndexingWorkbench = indexing_workbench_opt.as_mut().context(
            "No index writer available. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."
        )?;
        Ok(current_indexing_workbench)
    }

    fn prepare_document(&self, doc_json: String) -> PrepareDocumentOutcome {
        // Parse the document
        let doc_parsing_result = self.doc_mapper.doc_from_json(doc_json);
        let (partition, document) = match doc_parsing_result {
            Ok(doc) => doc,
            Err(doc_parsing_error) => {
                warn!(err=?doc_parsing_error);
                return match doc_parsing_error {
                    DocParsingError::RequiredFastField(_) => PrepareDocumentOutcome::MissingField,
                    _ => PrepareDocumentOutcome::ParsingError,
                };
            }
        };
        // Extract timestamp if necessary
        let timestamp_field = if let Some(timestamp_field) = self.timestamp_field_opt {
            timestamp_field
        } else {
            // No need to check the timestamp, there are no timestamp.
            return PrepareDocumentOutcome::Document {
                document,
                timestamp_opt: None,
                partition,
            };
        };
        let timestamp_opt = document
            .get_first(timestamp_field)
            .and_then(|value| match value {
                Value::Date(date_time) => Some(date_time.into_timestamp_secs()),
                value => value.as_i64(),
            });
        assert!(
            timestamp_opt.is_some(),
            "We should always have a timestamp here as doc parsing returns a `RequiredFastField` \
             error on a missing timestamp."
        );
        PrepareDocumentOutcome::Document {
            document,
            timestamp_opt,
            partition,
        }
    }

    async fn process_batch(
        &self,
        batch: RawDocBatch,
        indexing_workbench_opt: &mut Option<IndexingWorkbench>,
        counters: &mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> Result<(), ActorExitStatus> {
        let IndexingWorkbench {
            checkpoint_delta,
            indexed_splits,
            ..
        } = self
            .get_or_create_workbench(indexing_workbench_opt, ctx)
            .await?;
        checkpoint_delta
            .source_delta
            .extend(batch.checkpoint_delta)
            .context("Batch delta does not follow indexer checkpoint")?;
        for doc_json in batch.docs {
            let doc_json_num_bytes = doc_json.len() as u64;
            counters.overall_num_bytes += doc_json_num_bytes;
            let prepared_doc = {
                let _protect_zone = ctx.protect_zone();
                self.prepare_document(doc_json)
            };
            match prepared_doc {
                PrepareDocumentOutcome::ParsingError => {
                    counters.num_parse_errors += 1;
                }
                PrepareDocumentOutcome::MissingField => {
                    counters.num_missing_fields += 1;
                }
                PrepareDocumentOutcome::Document {
                    document,
                    timestamp_opt,
                    partition,
                } => {
                    let indexed_split =
                        self.get_or_create_indexed_split(partition, indexed_splits, ctx)?;
                    indexed_split.docs_size_in_bytes += doc_json_num_bytes;
                    counters.num_docs_in_workbench += 1;
                    counters.num_valid_docs += 1;
                    indexed_split.num_docs += 1;
                    if let Some(timestamp) = timestamp_opt {
                        record_timestamp(timestamp, &mut indexed_split.time_range);
                    }
                    let _protect_guard = ctx.protect_zone();
                    indexed_split
                        .index_writer
                        .add_document(document)
                        .context("Failed to add document.")?;
                }
            }
            ctx.record_progress();
        }
        Ok(())
    }
}

/// A workbench hosts the set of `IndexedSplit` that will are being built.
struct IndexingWorkbench {
    checkpoint_delta: IndexCheckpointDelta,
    indexed_splits: FnvHashMap<u64, IndexedSplit>,
    workbench_id: Ulid,
    // TODO create this Instant on the source side to be more accurate.
    // Right now this instant is used to compute time-to-search, but this
    // does not include the amount of time a document could have been
    // staying in the indexer queue or in the push api queue.
    date_of_birth: Instant,
}

pub struct Indexer {
    indexer_state: IndexerState,
    packager_mailbox: Mailbox<Packager>,
    indexing_workbench_opt: Option<IndexingWorkbench>,
    metastore_service: MetastoreService,
    counters: IndexerCounters,
}

#[async_trait]
impl Actor for Indexer {
    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn name(&self) -> String {
        "Indexer".to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        match exit_status {
            ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed
            | ActorExitStatus::Failure(_)
            | ActorExitStatus::Panicked => return Ok(()),
            ActorExitStatus::Quit | ActorExitStatus::Success => {
                self.send_to_packager(CommitTrigger::NoMoreDocs, ctx)
                    .await?;
            }
        }
        Ok(())
    }
}

fn record_timestamp(timestamp: i64, time_range: &mut Option<RangeInclusive<i64>>) {
    let new_timestamp_range = match time_range.as_ref() {
        Some(range) => {
            RangeInclusive::new(timestamp.min(*range.start()), timestamp.max(*range.end()))
        }
        None => RangeInclusive::new(timestamp, timestamp),
    };
    *time_range = Some(new_timestamp_range);
}

#[async_trait]
impl Handler<CommitTimeout> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        commit_timeout: CommitTimeout,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(indexing_workbench) = self.indexing_workbench_opt.as_ref() {
            // This is a timeout for a different split.
            // We can ignore it.
            if indexing_workbench.workbench_id != commit_timeout.workbench_id {
                return Ok(());
            }
        }
        self.send_to_packager(CommitTrigger::Timeout, ctx).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<RawDocBatch> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.process_batch(batch, ctx).await
    }
}

#[derive(Debug, Clone, Copy)]
enum CommitTrigger {
    Timeout,
    NoMoreDocs,
    NumDocsLimit,
}

impl Indexer {
    pub fn new(
        index_id: String,
        doc_mapper: Arc<dyn DocMapper>,
        source_id: String,
        metastore_service: MetastoreService,
        indexing_directory: IndexingDirectory,
        indexing_settings: IndexingSettings,
        packager_mailbox: Mailbox<Packager>,
    ) -> Self {
        let schema = doc_mapper.schema();
        let timestamp_field_opt = doc_mapper.timestamp_field(&schema);
        let sort_by_field_opt = match indexing_settings.sort_by() {
            SortBy::DocId => None,
            SortBy::FastField { field_name, order } => Some(IndexSortByField {
                field: field_name,
                order: order.into(),
            }),
        };
        let schema = doc_mapper.schema();
        let index_settings = IndexSettings {
            sort_by_field: sort_by_field_opt,
            docstore_blocksize: indexing_settings.docstore_blocksize,
            docstore_compression: Compressor::Zstd(ZstdCompressor {
                compression_level: Some(indexing_settings.docstore_compression_level),
            }),
        };
        Self {
            indexer_state: IndexerState {
                index_id,
                source_id,
                doc_mapper,
                indexing_directory,
                indexing_settings,
                timestamp_field_opt,
                schema,
                index_settings,
            },
            packager_mailbox,
            indexing_workbench_opt: None,
            metastore_service,
            counters: IndexerCounters::default(),
        }
    }

    async fn process_batch(
        &mut self,
        batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("indexer:batch:before");
        self.indexer_state
            .process_batch(
                batch,
                &mut self.indexing_workbench_opt,
                &mut self.counters,
                ctx,
            )
            .await?;
        if self.counters.num_docs_in_workbench
            >= self.indexer_state.indexing_settings.split_num_docs_target as u64
        {
            self.send_to_packager(CommitTrigger::NumDocsLimit, ctx)
                .await?;
        }
        fail_point!("indexer:batch:after");
        Ok(())
    }

    /// Extract the indexed split and send it to the Packager.
    async fn send_to_packager(
        &mut self,
        commit_trigger: CommitTrigger,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let IndexingWorkbench {
            checkpoint_delta,
            indexed_splits,
            date_of_birth,
            ..
        } = if let Some(indexing_workbench) = self.indexing_workbench_opt.take() {
            indexing_workbench
        } else {
            return Ok(());
        };

        let splits: Vec<IndexedSplit> = indexed_splits.into_values().collect();

        // Avoid producing empty split, but still update the checkpoint to avoid
        // reprocessing the same faulty documents.
        if splits.is_empty() {
            let index_checkpoint_delta_serialized_json =
                Some(serde_json::to_string(&checkpoint_delta)?);
            let publish_request = PublishSplitsRequest {
                index_id: self.indexer_state.index_id.clone(),
                split_ids: Vec::new(),
                replaced_split_ids: Vec::new(),
                index_checkpoint_delta_serialized_json,
            };
            self.metastore_service
                .publish_splits(publish_request)
                .await
                .with_context(|| {
                    format!(
                        "Failed to update the checkpoint for {}, {} after a split containing only \
                         errors.",
                        &self.indexer_state.index_id, &self.indexer_state.source_id
                    )
                })?;
            return Ok(());
        }

        let num_splits = splits.len() as u64;
        let split_ids = splits.iter().map(|split| &split.split_id).join(",");
        info!(commit_trigger=?commit_trigger, split_ids=%split_ids, num_docs=self.counters.num_docs_in_workbench, "send-to-packager");
        ctx.send_message(
            &self.packager_mailbox,
            IndexedSplitBatch {
                splits,
                checkpoint_delta: Some(checkpoint_delta),
                date_of_birth,
            },
        )
        .await?;
        self.counters.num_docs_in_workbench = 0;
        self.counters.num_splits_emitted += num_splits;
        self.counters.num_split_batches_emitted += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_control_plane::MetastoreService;
    use quickwit_doc_mapper::{DefaultDocMapper, SortOrder};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_metastore::MockMetastore;

    use super::*;
    use crate::actors::indexer::{record_timestamp, IndexerCounters};
    use crate::models::{IndexingDirectory, RawDocBatch};

    #[test]
    fn test_record_timestamp() {
        let mut time_range = None;
        record_timestamp(1628664679, &mut time_range);
        assert_eq!(time_range, Some(1628664679..=1628664679));
        record_timestamp(1628664112, &mut time_range);
        assert_eq!(time_range, Some(1628664112..=1628664679));
        record_timestamp(1628665112, &mut time_range);
        assert_eq!(time_range, Some(1628664112..=1628665112))
    }

    #[tokio::test]
    async fn test_indexer_simple() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper = Arc::new(quickwit_doc_mapper::default_doc_mapper_for_tests());
        let indexing_directory = IndexingDirectory::for_test().await?;
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 3;
        indexing_settings.sort_field = Some("timestamp".to_string());
        indexing_settings.sort_order = Some(SortOrder::Desc);
        indexing_settings.timestamp_field = Some("timestamp".to_string());
        let (mailbox, inbox) = create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_publish_splits()
            .returning(move |_, splits, _, _| {
                assert!(splits.is_empty());
                Ok(())
            });
        let universe = Universe::new();
        let metastore_service = MetastoreService::from_metastore(Arc::new(metastore));
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            "source-id".to_string(),
            metastore_service,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                        r#"{"body": "happy", "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string(), // missing timestamp
                        r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#.to_string(), // ok
                        r#"{"body": "happy2", "timestamp": 1628837062, "response_date": "2021-12-19T16:40:57+00:00", "response_time": 13, "response_payload": "YWJj"}"#.to_string(), // ok
                        "{".to_string(),                    // invalid json
                    ],
                checkpoint_delta: SourceCheckpointDelta::from(0..4),
            })
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 1,
                num_missing_fields: 1,
                num_valid_docs: 2,
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
                num_docs_in_workbench: 2, //< we have not reached the commit limit yet.
                overall_num_bytes: 387
            }
        );
        indexer_mailbox
            .send_message(
                RawDocBatch {
                    docs: vec![r#"{"body": "happy3", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string()],
                    checkpoint_delta: SourceCheckpointDelta::from(4..5),
                }
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 1,
                num_missing_fields: 1,
                num_valid_docs: 3,
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0, //< the num docs in split counter has been reset.
                overall_num_bytes: 525
            }
        );
        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let batch = output_messages[0]
            .downcast_ref::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(batch.splits[0].num_docs, 3);
        let sort_by_field = batch.splits[0].index.settings().sort_by_field.as_ref();
        assert!(sort_by_field.is_some());
        assert_eq!(sort_by_field.unwrap().field, "timestamp");
        assert!(sort_by_field.unwrap().order.is_desc());
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_timeout() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper = Arc::new(quickwit_doc_mapper::default_doc_mapper_for_tests());
        let indexing_directory = IndexingDirectory::for_test().await?;
        let indexing_settings = IndexingSettings::for_test();
        let (mailbox, inbox) = create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_publish_splits()
            .returning(move |_, splits, _, _| {
                assert!(splits.is_empty());
                Ok(())
            });
        let universe = Universe::new();
        let metastore_service = MetastoreService::from_metastore(Arc::new(metastore));
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            "source-id".to_string(),
            metastore_service,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(
                RawDocBatch {
                    docs: vec![r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string()],
                    checkpoint_delta: SourceCheckpointDelta::from(0..1),
                }
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 1,
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
                num_docs_in_workbench: 1,
                overall_num_bytes: 137
            }
        );
        universe.simulate_time_shift(Duration::from_secs(61)).await;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 1,
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0,
                overall_num_bytes: 137
            }
        );
        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let indexed_split_batch = output_messages[0]
            .downcast_ref::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(indexed_split_batch.splits[0].num_docs, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_eof() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper = Arc::new(quickwit_doc_mapper::default_doc_mapper_for_tests());
        let indexing_directory = IndexingDirectory::for_test().await?;
        let indexing_settings = IndexingSettings::for_test();
        let (mailbox, inbox) = create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_publish_splits()
            .returning(move |_, splits, _, _| {
                assert!(splits.is_empty());
                Ok(())
            });
        let universe = Universe::new();
        let metastore_service = MetastoreService::from_metastore(Arc::new(metastore));
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            "source-id".to_string(),
            metastore_service,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(
                RawDocBatch {
                    docs: vec![r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string()],
                    checkpoint_delta: SourceCheckpointDelta::from(0..1),
                }
            )
            .await?;
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(exit_status.is_success());
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 1,
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0,
                overall_num_bytes: 137
            }
        );
        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(
            output_messages[0]
                .downcast_ref::<IndexedSplitBatch>()
                .unwrap()
                .splits[0]
                .num_docs,
            1
        );
        Ok(())
    }

    const DOCMAPPER_WITH_PARTITION_JSON: &str = r#"
        {
            "tag_fields": ["tenant"],
            "partition_key": "tenant",
            "field_mappings": [
                { "name": "tenant", "type": "text", "tokenizer": "raw", "indexed": true },
                { "name": "body", "type": "text" }
            ]
        }"#;

    #[tokio::test]
    async fn test_indexer_partitioning() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper: Arc<dyn DocMapper> = Arc::new(
            serde_json::from_str::<DefaultDocMapper>(DOCMAPPER_WITH_PARTITION_JSON).unwrap(),
        );
        let indexing_directory = IndexingDirectory::for_test().await?;
        let indexing_settings = IndexingSettings::for_test();
        let (mailbox, inbox) = create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_publish_splits()
            .returning(move |_, splits, _, _| {
                assert!(splits.is_empty());
                Ok(())
            });
        let metastore_service = MetastoreService::from_metastore(Arc::new(metastore));
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            "source-id".to_string(),
            metastore_service,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let universe = Universe::new();
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                    r#"{"tenant": "tenant_1", "body": "first doc for tenant 1"}"#.to_string(),
                    r#"{"tenant": "tenant_2", "body": "first doc for tenant 2"}"#.to_string(),
                    r#"{"tenant": "tenant_1", "body": "second doc for tenant 1"}"#.to_string(),
                ],
                checkpoint_delta: SourceCheckpointDelta::from(0..2),
            })
            .await?;

        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 3,
                num_docs_in_workbench: 3,
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
                overall_num_bytes: 169
            }
        );
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 3,
                num_docs_in_workbench: 0,
                num_splits_emitted: 2,
                num_split_batches_emitted: 1,
                overall_num_bytes: 169
            }
        );

        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);

        let indexed_split_batch = output_messages[0]
            .downcast_ref::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(indexed_split_batch.splits.len(), 2);

        Ok(())
    }
}

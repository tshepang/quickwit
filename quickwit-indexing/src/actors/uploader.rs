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

use std::collections::HashSet;
use std::iter::FromIterator;
use std::mem;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context};
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_control_plane::MetastoreService;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore_api::StageSplitRequest;
use quickwit_storage::SplitPayloadBuilder;
use time::OffsetDateTime;
use tokio::sync::{oneshot, Semaphore, SemaphorePermit};
use tracing::{info, info_span, warn, Instrument, Span};

use crate::actors::sequencer::Sequencer;
use crate::actors::Publisher;
use crate::models::{PackagedSplit, PackagedSplitBatch, SplitUpdate};
use crate::split_store::IndexingSplitStore;

pub const MAX_CONCURRENT_SPLIT_UPLOAD: usize = 4;

/// This semaphore ensures that at most `MAX_CONCURRENT_SPLIT_UPLOAD` uploads can happen
/// concurrently.
///
/// This permit applies to all uploader actors. In the future, we might want to have a nicer
/// granularity, and put that semaphore back into the uploader actor, but have a single uploader
/// actor for all indexing pipeline.
static CONCURRENT_UPLOAD_PERMITS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_SPLIT_UPLOAD);

pub struct Uploader {
    actor_name: &'static str,
    metastore_service: MetastoreService,
    index_storage: IndexingSplitStore,
    sequencer_mailbox: Mailbox<Sequencer<Publisher>>,
    counters: UploaderCounters,
}

impl Uploader {
    pub fn new(
        actor_name: &'static str,
        metastore_service: MetastoreService,
        index_storage: IndexingSplitStore,
        sequencer_mailbox: Mailbox<Sequencer<Publisher>>,
    ) -> Uploader {
        Uploader {
            actor_name,
            metastore_service,
            index_storage,
            sequencer_mailbox,
            counters: Default::default(),
        }
    }

    async fn acquire_semaphore(
        &self,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<SemaphorePermit<'static>> {
        let _guard = ctx.protect_zone();
        Semaphore::acquire(&CONCURRENT_UPLOAD_PERMITS)
            .await
            .context("The uploader semaphore is closed. (This should never happen.)")
    }
}

#[derive(Clone, Debug, Default)]
pub struct UploaderCounters {
    pub num_staged_splits: Arc<AtomicU64>,
    pub num_uploaded_splits: Arc<AtomicU64>,
}

#[async_trait]
impl Actor for Uploader {
    type ObservableState = UploaderCounters;

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // We do not need a large capacity here...
        // The uploader just spawns tasks that are uploading,
        // so that in a sense, the CONCURRENT_UPLOAD_PERMITS semaphore also acts as
        // a queue capacity.
        //
        // Having a large queue is costly too, because each message is a handle over
        // a split directory. We DO need agressive backpressure here.
        QueueCapacity::Bounded(0)
    }

    fn name(&self) -> String {
        self.actor_name.to_string()
    }
}

#[async_trait]
impl Handler<PackagedSplitBatch> for Uploader {
    type Reply = ();

    fn message_span(&self, msg_id: u64, batch: &PackagedSplitBatch) -> Span {
        info_span!("", msg_id=&msg_id, num_splits=%batch.split_ids().len())
    }

    async fn handle(
        &mut self,
        batch: PackagedSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("uploader:before");
        let (split_uploaded_tx, split_uploaded_rx) = oneshot::channel::<SplitUpdate>();

        // We send the future to the sequencer right away.

        // The sequencer will then resolve the future in their arrival order and ensure that the
        // publisher publishes splits in order.
        ctx.send_message(&self.sequencer_mailbox, split_uploaded_rx)
            .await?;

        // The permit will be added back manually to the semaphore the task after it is finished.
        // This is not a valid usage of protected zone here.
        //
        // Protected zone are supposed to be used when the cause for blocking is
        // outside of the responsability of the current actor.
        // For instance, when sending a message on a downstream actor with a saturated
        // mailbox.
        // This is meant to be fixed with ParallelActors.
        let permit_guard = self.acquire_semaphore(ctx).await?;
        let kill_switch = ctx.kill_switch().clone();
        let split_ids = batch.split_ids();
        if kill_switch.is_dead() {
            warn!(split_ids=?split_ids,"Kill switch was activated. Cancelling upload.");
            return Err(ActorExitStatus::Killed);
        }
        let metastore_service = self.metastore_service.clone();
        let index_storage = self.index_storage.clone();
        let counters = self.counters.clone();
        let index_id = batch.index_id();
        let span = Span::current();
        info!(split_ids=?split_ids, "start-stage-and-store-splits");
        tokio::spawn(
            async move {
                fail_point!("uploader:intask:before");
                let mut packaged_splits_and_metadatas = Vec::new();
                for split in batch.splits {
                    let upload_result = stage_and_upload_split(
                        &split,
                        &index_storage,
                        metastore_service.clone(),
                        counters.clone(),
                    )
                    .await;
                    if let Err(cause) = upload_result {
                        warn!(cause=?cause, split_id=%split.split_id, "Failed to upload split. Killing!");
                        kill_switch.kill();
                        bail!("Failed to upload split `{}`. Killing!", split.split_id);
                    }
                    packaged_splits_and_metadatas.push((split, upload_result.unwrap()));
                }
                let publisher_message = make_publish_operation(index_id, packaged_splits_and_metadatas, batch.checkpoint_delta_opt, batch.date_of_birth);
                if let Err(publisher_message) = split_uploaded_tx.send(publisher_message) {
                    bail!(
                        "Failed to send upload split `{:?}`. The publisher is probably dead.",
                        &publisher_message
                    );
                }
                info!("success-stage-and-store-splits");
                // We explicitely drop it in order to force move the permit guard into the async
                // task.
                mem::drop(permit_guard);
                Result::<(), anyhow::Error>::Ok(())
            }
            .instrument(span),
        );
        fail_point!("uploader:intask:after");
        Ok(())
    }
}

fn create_split_metadata(split: &PackagedSplit, footer_offsets: Range<u64>) -> SplitMetadata {
    SplitMetadata {
        split_id: split.split_id.clone(),
        num_docs: split.num_docs as usize,
        time_range: split.time_range.clone(),
        uncompressed_docs_size_in_bytes: split.size_in_bytes,
        create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
        tags: split.tags.clone(),
        demux_num_ops: split.demux_num_ops,
        footer_offsets,
    }
}

fn make_publish_operation(
    index_id: String,
    packaged_splits_and_metadatas: Vec<(PackagedSplit, SplitMetadata)>,
    checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    date_of_birth: Instant,
) -> SplitUpdate {
    assert!(!packaged_splits_and_metadatas.is_empty());
    let replaced_split_ids = packaged_splits_and_metadatas
        .iter()
        .flat_map(|(split, _)| split.replaced_split_ids.clone())
        .collect::<HashSet<_>>();
    SplitUpdate {
        index_id,
        new_splits: packaged_splits_and_metadatas
            .into_iter()
            .map(|split_and_meta| split_and_meta.1)
            .collect_vec(),
        replaced_split_ids: Vec::from_iter(replaced_split_ids),
        checkpoint_delta_opt,
        date_of_birth,
    }
}

async fn stage_and_upload_split(
    packaged_split: &PackagedSplit,
    split_store: &IndexingSplitStore,
    mut metastore_service: MetastoreService,
    counters: UploaderCounters,
) -> anyhow::Result<SplitMetadata> {
    let split_streamer = SplitPayloadBuilder::get_split_payload(
        &packaged_split.split_files,
        &packaged_split.hotcache_bytes,
    )?;
    let split_metadata = create_split_metadata(
        packaged_split,
        split_streamer.footer_range.start as u64..split_streamer.footer_range.end as u64,
    );
    let index_id = packaged_split.index_id.clone();
    info!(split_id = packaged_split.split_id.as_str(), "staging-split");
    let split_metadata_serialized_json =
        serde_json::to_string(&split_metadata).map_err(|error| anyhow::anyhow!(error))?;
    let stage_split_request = StageSplitRequest {
        index_id,
        split_metadata_serialized_json,
    };
    metastore_service.stage_split(stage_split_request).await?;
    counters.num_staged_splits.fetch_add(1, Ordering::SeqCst);

    info!(split_id = packaged_split.split_id.as_str(), "storing-split");
    split_store
        .store_split(
            &split_metadata,
            packaged_split.split_scratch_directory.path(),
            Box::new(split_streamer),
        )
        .await?;
    counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
    Ok(split_metadata)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;

    use quickwit_actors::{create_test_mailbox, ObservationType, Universe};
    use quickwit_control_plane::MetastoreService;
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::RamStorage;
    use tokio::sync::oneshot;

    use super::*;
    use crate::models::ScratchDirectory;

    #[tokio::test]
    async fn test_uploader_1() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox::<Sequencer<Publisher>>();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_stage_split()
            .withf(move |index_id, metadata| -> bool {
                (index_id == "test-index")
                    && metadata.split_id() == "test-split"
                    && metadata.time_range == Some(1628203589..=1628203640)
            })
            .times(1)
            .returning(|_, _| Ok(()));
        let ram_storage = RamStorage::default();
        let index_storage: IndexingSplitStore =
            IndexingSplitStore::create_with_no_local_store(Arc::new(ram_storage.clone()));
        let metastore_service = MetastoreService::from_metastore(Arc::new(mock_metastore));
        let uploader = Uploader::new("TestUploader", metastore_service, index_storage, mailbox);
        let (uploader_mailbox, uploader_handle) = universe.spawn_actor(uploader).spawn();
        let split_scratch_directory = ScratchDirectory::for_test()?;
        let checkpoint_delta_opt: Option<IndexCheckpointDelta> = Some(IndexCheckpointDelta {
            source_id: "source".to_string(),
            source_delta: SourceCheckpointDelta::from(3..15),
        });
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_id: "test-split".to_string(),
                    index_id: "test-index".to_string(),
                    time_range: Some(1_628_203_589i64..=1_628_203_640i64),
                    size_in_bytes: 1_000,
                    split_scratch_directory,
                    num_docs: 10,
                    demux_num_ops: 0,
                    tags: Default::default(),
                    replaced_split_ids: Vec::new(),
                    hotcache_bytes: vec![],
                    split_files: vec![],
                }],
                checkpoint_delta_opt,
                Instant::now(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let mut publish_futures = inbox.drain_for_test();
        assert_eq!(publish_futures.len(), 1);
        let publish_future = *publish_futures
            .pop()
            .unwrap()
            .downcast::<oneshot::Receiver<SplitUpdate>>()
            .unwrap();
        let publisher_message = publish_future.await?;
        let SplitUpdate {
            index_id,
            new_splits,
            checkpoint_delta_opt,
            replaced_split_ids,
            date_of_birth: _,
        } = publisher_message;
        assert_eq!(new_splits.len(), 1);
        assert_eq!(index_id, "test-index");
        assert_eq!(new_splits[0].split_id(), "test-split");
        let checkpoint_delta = checkpoint_delta_opt.unwrap();
        assert_eq!(checkpoint_delta.source_id, "source");
        assert_eq!(
            checkpoint_delta.source_delta,
            SourceCheckpointDelta::from(3..15)
        );
        assert!(replaced_split_ids.is_empty());
        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(&files, &[PathBuf::from("test-split.split")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_emits_replace() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox::<Sequencer<Publisher>>();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_stage_split()
            .withf(move |index_id, metadata| -> bool {
                (index_id == "test-index")
                    && vec!["test-split-1".to_owned(), "test-split-2".to_owned()]
                        .contains(&metadata.split_id().to_string())
                    && metadata.time_range == Some(1628203589..=1628203640)
            })
            .times(2)
            .returning(|_, _| Ok(()));
        let ram_storage = RamStorage::default();
        let index_storage: IndexingSplitStore =
            IndexingSplitStore::create_with_no_local_store(Arc::new(ram_storage.clone()));
        let metastore_service = MetastoreService::from_metastore(Arc::new(mock_metastore));
        let uploader = Uploader::new("TestUploader", metastore_service, index_storage, mailbox);
        let (uploader_mailbox, uploader_handle) = universe.spawn_actor(uploader).spawn();
        let split_scratch_directory_1 = ScratchDirectory::for_test()?;
        let split_scratch_directory_2 = ScratchDirectory::for_test()?;
        let packaged_split_1 = PackagedSplit {
            split_id: "test-split-1".to_string(),
            index_id: "test-index".to_string(),
            time_range: Some(1_628_203_589i64..=1_628_203_640i64),
            size_in_bytes: 1_000,
            split_scratch_directory: split_scratch_directory_1,
            num_docs: 10,
            demux_num_ops: 1,
            tags: Default::default(),
            replaced_split_ids: vec![
                "replaced-split-1".to_string(),
                "replaced-split-2".to_string(),
            ],
            split_files: vec![],
            hotcache_bytes: vec![],
        };
        let package_split_2 = PackagedSplit {
            split_id: "test-split-2".to_string(),
            index_id: "test-index".to_string(),
            time_range: Some(1_628_203_589i64..=1_628_203_640i64),
            size_in_bytes: 1_000,
            split_scratch_directory: split_scratch_directory_2,
            num_docs: 10,
            demux_num_ops: 1,
            tags: Default::default(),
            replaced_split_ids: vec![
                "replaced-split-1".to_string(),
                "replaced-split-2".to_string(),
            ],
            split_files: vec![],
            hotcache_bytes: vec![],
        };
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![packaged_split_1, package_split_2],
                None,
                Instant::now(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let publish_futures = inbox.drain_for_test();
        assert_eq!(publish_futures.len(), 1);
        let publish_future = publish_futures
            .into_iter()
            .next()
            .unwrap()
            .downcast::<oneshot::Receiver<SplitUpdate>>()
            .unwrap();
        let publisher_message = publish_future.await?;
        let SplitUpdate {
            index_id,
            new_splits,
            mut replaced_split_ids,
            checkpoint_delta_opt,
            date_of_birth: _,
        } = publisher_message;
        assert_eq!(&index_id, "test-index");
        // Sort first to avoid test failing.
        replaced_split_ids.sort();
        assert_eq!(new_splits.len(), 2);
        assert_eq!(new_splits[0].split_id(), "test-split-1");
        assert_eq!(new_splits[1].split_id(), "test-split-2");
        assert_eq!(
            &replaced_split_ids,
            &[
                "replaced-split-1".to_string(),
                "replaced-split-2".to_string()
            ]
        );
        assert!(checkpoint_delta_opt.is_none());
        assert_eq!(new_splits[0].demux_num_ops, 1);
        assert_eq!(new_splits[1].demux_num_ops, 1);

        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(
            &files,
            &[
                PathBuf::from("test-split-1.split"),
                PathBuf::from("test-split-2.split")
            ]
        );
        Ok(())
    }
}

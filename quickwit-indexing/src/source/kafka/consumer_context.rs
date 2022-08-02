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

use async_trait::async_trait;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

#[derive(Debug)]
pub(super) enum RebalanceEvent {
    Starting {
        ack_tx: oneshot::Sender<()>,
    },
    Assignment {
        assignment: Vec<i32>,
        ack_tx: oneshot::Sender<Vec<(i32, Offset)>>,
    },
}

pub(super) struct KafkaSourceConsumerContext {
    pub topic: String,
    pub rebalance_events: mpsc::Sender<RebalanceEvent>,
}

#[async_trait]
trait AsyncConsumerContext {
    async fn pre_rebalance_async(&self, rebalance: &Rebalance);

    async fn post_rebalance_async(&self, rebalance: &Rebalance);

    async fn commit_callback_async(
        &self,
        commit_res: KafkaResult<()>,
        offsets: &TopicPartitionList,
    );
}

#[async_trait]
impl AsyncConsumerContext for KafkaSourceConsumerContext {
    async fn pre_rebalance_async(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
        if let Rebalance::Assign(_) = rebalance {
            let (ack_tx, ack_rx) = oneshot::channel();
            self.rebalance_events
                .send(RebalanceEvent::Starting { ack_tx })
                .await
                .expect("Failed to send pre-rebalance event.");
            ack_rx
                .await
                .expect("Failed to receive pre-rebalance event ack.");
        }
    }

    async fn post_rebalance_async(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
        if let Rebalance::Assign(tpl) = rebalance {
            let assignment = tpl
                .elements()
                .iter()
                .map(|tple| {
                    assert_eq!(tple.topic(), self.topic);
                    tple.partition()
                })
                .collect();
            let (ack_tx, ack_rx) = oneshot::channel();
            self.rebalance_events
                .send(RebalanceEvent::Assignment { assignment, ack_tx })
                .await
                .expect("Failed to send post-rebalance event.");
            let next_offsets = ack_rx
                .await
                .expect("Failed to receive post-rebalance event ack.");

            for (partition, offset) in next_offsets {
                let mut partition = tpl.find_partition(&self.topic, partition).expect("Failed to find partition in assignment. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
                partition.set_offset(offset).expect("Failed to convert offset to librdkafka internal representation. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
            }
        }
    }

    #[allow(unused_variables)]
    async fn commit_callback_async(
        &self,
        commit_res: KafkaResult<()>,
        offsets: &TopicPartitionList,
    ) {
        info!("Committing offsets: {:?}", commit_res);
    }
}

impl ClientContext for KafkaSourceConsumerContext {}

impl ConsumerContext for KafkaSourceConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        let handle = Handle::current();
        let _guard = handle.enter();
        futures::executor::block_on(async { self.pre_rebalance_async(rebalance) });
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        let handle = Handle::current();
        let _guard = handle.enter();
        futures::executor::block_on(async { self.post_rebalance_async(rebalance) });
    }

    fn commit_callback(&self, commit_res: KafkaResult<()>, offsets: &TopicPartitionList) {
        let handle = Handle::current();
        let _guard = handle.enter();
        futures::executor::block_on(async { self.commit_callback_async(commit_res, offsets) });
    }
}

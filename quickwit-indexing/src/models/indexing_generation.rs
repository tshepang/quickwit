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

use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use super::IndexingDirectory;

#[derive(Debug, Default)]
pub struct IndexingGenerationLeader(Arc<Mutex<usize>>);

impl IndexingGenerationLeader {
    pub async fn current(&self) -> IndexingGeneration {
        let mutex = self.0.clone();
        let guard = self.0.lock().await;
        IndexingGeneration(mutex, *guard)
    }

    pub async fn inc(&self) -> IndexingGeneration {
        let mutex = self.0.clone();
        let mut guard = self.0.lock().await;
        *guard += 1;
        IndexingGeneration(mutex, *guard)
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexingGeneration(Arc<Mutex<usize>>, usize);

impl IndexingGeneration {
    pub async fn is_current(&self) -> bool {
        let guard = self.0.lock().await;
        *guard == self.1
    }

    pub async fn lock(&self) -> IndexingGenerationGuard {
        let guard = self.0.lock().await;
        let gen = *guard;
        IndexingGenerationGuard(guard, gen)
    }
}

#[derive(Debug)]
pub struct IndexingGenerationGuard<'a>(MutexGuard<'a, usize>, usize);

impl<'a> IndexingGenerationGuard<'a> {
    pub fn is_current(&self) -> bool {
        *self.0 == self.1
    }
}

#[derive(Debug)]
pub struct NewIndexingGeneration(pub IndexingGeneration);

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_indexing_generation() {
        // let indexing_generation = IndexingGenerationLeader::default();
        // let indexing_generation_0 = indexing_generation.current();
        // assert!(indexing_generation_0.is_current());

        // current_indexing_generation.inc();
        // let indexing_generation_1 = current_indexing_generation.current();
        // assert!(indexing_generation_0.is_current());
        // assert!(indexing_generation_1.is_current());
    }
}

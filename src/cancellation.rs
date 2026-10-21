// Copyright (c) 2026 ADBC Drivers Contributors
// Licensed under the Apache License, Version 2.0.

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use adbc_core::error::{Error, Result, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion::{
    execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;
use sedona_extension::{runtime::RuntimeHandle, streaming::StreamingRecordBatchReader};
use tokio_util::sync::CancellationToken;

/// A replaceable generation, shared with statements but never permanently cancelled.
/// Operations clone the token, not this slot. Completed operations release their
/// clones naturally; the handle retains no list of past operations.
#[derive(Clone, Default)]
pub(crate) struct Cancellation(Arc<Mutex<CancellationToken>>);

impl Cancellation {
    fn token(&self) -> CancellationToken {
        self.0.lock().unwrap().clone()
    }

    pub(crate) fn cancel(&self) {
        let old = std::mem::take(&mut *self.0.lock().unwrap());
        old.cancel();
    }
}

#[derive(Clone)]
pub(crate) struct Operation {
    connection: CancellationToken,
    statement: CancellationToken,
}

impl Operation {
    pub(crate) fn connection(connection: &Cancellation) -> Self {
        Self {
            connection: connection.token(),
            statement: CancellationToken::new(),
        }
    }

    pub(crate) fn new(connection: &Cancellation, statement: &Cancellation) -> Self {
        Self {
            connection: connection.token(),
            statement: statement.token(),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.connection.is_cancelled() || self.statement.is_cancelled()
    }

    pub(crate) async fn run<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.connection.cancelled() => Err(cancelled()),
            _ = self.statement.cancelled() => Err(cancelled()),
            result = future => result,
        }
    }

    pub(crate) fn reader(
        self,
        stream: SendableRecordBatchStream,
        runtime: Arc<RuntimeHandle>,
    ) -> impl RecordBatchReader + Send {
        let schema = stream.schema();
        let operation = self.clone();
        // Selecting on the tokens wakes a pending batch fetch immediately. Ending
        // this unfold drops the DataFusion stream, which cancels query execution.
        let stream = futures::stream::unfold(Some((stream, operation)), |state| async move {
            let (mut stream, operation) = state?;
            match operation.run(async { Ok(stream.next().await) }).await {
                Ok(Some(batch)) => Some((batch, Some((stream, operation)))),
                Ok(None) => None,
                Err(error) => Some((
                    Err(datafusion::error::DataFusionError::External(Box::new(
                        error,
                    ))),
                    None,
                )),
            }
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream));
        let reader = StreamingRecordBatchReader::new(stream, runtime).with_skip_empty_batches(true);
        self.wrap_reader(Box::new(reader))
    }

    pub(crate) fn wrap_reader(
        self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> impl RecordBatchReader + Send {
        CancellableReader {
            schema: reader.schema(),
            active: Some((reader, self)),
        }
    }
}

fn cancelled() -> Error {
    Error::with_message_and_status("Operation cancelled", Status::Cancelled)
}

/// Check before returning prefetched batches too, and release both the worker and
/// tokens on cancellation, EOF, or error even if the caller keeps the reader.
struct CancellableReader {
    schema: SchemaRef,
    active: Option<(Box<dyn RecordBatchReader + Send>, Operation)>,
}

impl Iterator for CancellableReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (reader, operation) = self.active.as_mut()?;
        let mut result = if operation.is_cancelled() {
            Some(Err(ArrowError::ExternalError(Box::new(cancelled()))))
        } else {
            reader.next()
        };
        if operation.is_cancelled() {
            result = Some(Err(ArrowError::ExternalError(Box::new(cancelled()))));
        }
        if !matches!(result, Some(Ok(_))) {
            self.active = None;
        }
        result
    }
}

impl RecordBatchReader for CancellableReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Stream;
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    struct PendingStream {
        started: Option<std::sync::mpsc::Sender<()>>,
        dropped: std::sync::mpsc::Sender<()>,
    }

    impl Stream for PendingStream {
        type Item = datafusion::common::Result<RecordBatch>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
            }
            Poll::Pending
        }
    }

    impl Drop for PendingStream {
        fn drop(&mut self) {
            let _ = self.dropped.send(());
        }
    }

    fn runtime() -> Arc<RuntimeHandle> {
        Arc::new(RuntimeHandle::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        ))
    }

    #[test]
    fn pending_reads_wake_and_drop_stream() {
        for cancel_connection in [false, true] {
            let connection = Cancellation::default();
            let statement = Cancellation::default();
            let operation = Operation::new(&connection, &statement);
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(arrow_schema::Schema::empty()),
                PendingStream {
                    started: Some(started_tx),
                    dropped: dropped_tx,
                },
            ));
            let mut reader = operation.reader(stream, runtime());
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                result_tx
                    .send(reader.next().unwrap().unwrap_err().to_string())
                    .unwrap();
                assert!(reader.next().is_none());
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            if cancel_connection {
                connection.cancel();
            } else {
                statement.cancel();
            }
            assert!(
                result_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .contains("cancelled")
            );
            dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            thread.join().unwrap();
        }
    }

    #[test]
    fn pending_future_is_dropped_and_reports_cancelled() {
        let connection = Cancellation::default();
        let operation = Operation::connection(&connection);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let runtime = runtime();
        let task = runtime.spawn(async move {
            let mut pending = PendingStream {
                started: Some(started_tx),
                dropped: dropped_tx,
            };
            operation
                .run(async move {
                    pending.next().await;
                    Ok(())
                })
                .await
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        connection.cancel();
        let error = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err()
        });
        assert_eq!(error.status, Status::Cancelled);
        dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(!Operation::connection(&connection).is_cancelled());
    }

    #[test]
    fn unpolled_reader_drop_releases_stream() {
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(arrow_schema::Schema::empty()),
            PendingStream {
                started: None,
                dropped: dropped_tx,
            },
        ));
        let reader = Operation::connection(&Cancellation::default()).reader(stream, runtime());
        drop(reader);
        dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}

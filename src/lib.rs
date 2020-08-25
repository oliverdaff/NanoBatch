#![warn(missing_docs)]
//! __A micro batching library.__
//!
//! Micro-batching is a technique used in processing pipelines where
//! individual tasks are grouped together into small batches. This can
//! improve throughput by reducing the number of requests made to a
//! downstream system. Nano is a micro-batching library.
//!
//! # Usage
//! The `NanoBatcher` struct is the interface for submitting batch items as
//! well as shutting down the processing of batches.
//! The _Nano_ library introduces a `BatchProcessor` trait, which must be
//! implemented for each downstream system receiving batches.
//!
//! Nano uses the `tokio` library to provide non blocking async processing.
//!
//! # Example
//! ```
//! use std::sync::{Arc, Mutex};
//! use tokio::time;
//!
//!struct TestBatchProcessor;
//!
//!#[derive(Clone, Debug, PartialEq)]
//!
//!struct TestBatchProcessorError(String);
//!
//!impl nanobatch::BatchProcessor<i32, i32, TestBatchProcessorError> for TestBatchProcessor {
//!    fn process_batch(&self, batch: Vec<i32>) -> Result<Vec<i32>, TestBatchProcessorError> {
//!        Ok(batch.iter().map(|x| x + 2).collect::<Vec<_>>())
//!    }
//!}
//! #[tokio::main]
//! async fn main() {
//!     let processor = Arc::new(Mutex::new(TestBatchProcessor));
//!     let batcher = nanobatch::NanoBatcher::new(10, 32, time::Duration::from_millis(10), processor);
//!     let job_result_one = batcher.send_item(1, None).await;
//!     let job_result_two = batcher.send_item(2, None).await;
//!     assert_eq!(job_result_one.await, Ok(Ok(3)));
//!     assert_eq!(job_result_two.await, Ok(Ok(4)));
//! }
//! ```
//!
//! # Usage
//!
//! Instances of NanoBatcher are created using the `NanoBatcher::new` which configures the microbatch details.
//! The configuration options are:
//!  *   `max_batch_size` - the largest allowed batch size, before the batch is sent to the `BatchProcessor`.
//!  *   `max_pending_messages` - the largest allowed pending messages before `send_item` waits for capacity.
//!  *   `max_time_between_batches` - the maximum time until the submitted items will be
//!  send, even if the `max_batch_size` has not been reached.
//!  
//! Backpressure can be achieved on the sender by setting `max_pending_messages` which sets the allowed number of pending
//! messages in the input channel.
//!
//! Calls to the `BatchProcessor` to process each batch are carried out in a separate thread to avoid blocking the tokio runtime.
//!
//! To shutdown and drain the input channels use `shutdown` which will block until all submitted BatchItems have been processed.

use futures::future;
use futures::prelude::*;
use std::fmt::Debug;
use std::mem::replace;
use std::panic;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time;

/// Errors returned by the Nano Batcher
/// The command to send down the channel.
#[derive(Debug, PartialEq)]
pub enum NanoBatchError<T> {
    /// The command was sent to a closed channel.
    Closed(T),
    /// The command could not be sent before it timed out.
    Timeout(T),
    /// The command could not be set because the NanoBatcher is shutdown.
    ShutDown(T),
    /// The one shot receiver was closed or dropped
    ReceiverError,
}

/// Errors returned for a single BatchItem
#[derive(Debug, PartialEq)]
pub enum BatchItemError<E> {
    /// The error returned by the BatchProcessor
    ItemError(E),
    ///The BatchProcessor task was cancelled
    Cancelled,
    /// The BatchProcessor task panicked.
    Paniced,
}

/// A Result from sending a Item
pub type NanoBatchResult<R, T> = Result<R, NanoBatchError<T>>;

/// A Single Item that forms part of a batch
/// *   the item to send to the BatchProcessor.
/// *   the channel to send the response to.
/// # Type parameters
/// *   T - The type of of the batch Item send to the BatchProcessor.
/// *   S - The type of a result for a Item send to the BatchProcessor.
/// *   E - The type of the Error returned by the BatchProcessor if the entire batch fails.
struct BatchItem<T, S, E>(T, oneshot::Sender<Result<S, BatchItemError<E>>>);

/// NanoBatcher allows single items to be submitted, the items
/// are grouped into a batch and sent to the BatchProcessor.
/// Items are sent to the BatchProcessor when the
/// number of items highwater mark is reached
/// or the time since last batch was sent reaches the limit.
/// Items are submitted using `send_item`.
/// The NanoBatcher is shut down using the `close` method which prevents
/// new BatchItems being submitted and submits all pending items.
/// # Type Parameters
/// *   T - The type of of the batch Item send to the BatchProcessor.
/// *   S - The type of a result for a Item send to the BatchProcessor.
/// *   E - The type of the Error returned by the BatchProcessor if the entire batch fails.
pub struct NanoBatcher<T, S, E> {
    // Channel BatchItems are submitted on
    command_channel: mpsc::Sender<BatchItem<T, S, E>>,
    // Notify the NanoBatcher loop to close the receiving channel
    closed_notifier: Arc<Notify>,
    // Recive the notification that the NanoBatcher has processed all jobs
    shutdown_notifier: Arc<Notify>,
    // True if the NanoBatcher is not receiving new BatchItems
    closed: bool,

    is_shutdown: tokio::sync::Mutex<bool>
}

/// The BatchProcessor is a trait that must be implemented for each
/// downstream system.
/// *   T - is the type of the request.
/// *   S - is the type of the response.
/// *   E - is the type of Error if the entire batch fails.
pub trait BatchProcessor<T, S, E> {
    /// Returns a Result for a batch.
    ///
    /// # Arguments
    ///
    /// *   `batch` - a batch of items
    fn process_batch(&self, batch: Vec<T>) -> Result<Vec<S>, E>;
}

impl<T: 'static + Send + Debug, S: 'static + Send + Debug, E: 'static + Send + Clone + Debug>
    NanoBatcher<T, S, E>
{
    /// Creates a new NanoBatcher capable of receiving new batch items.
    ///
    /// # Arguments
    /// *   `max_batch_size` - the largest allowed batch size, before the batch is sent
    /// to the `BatchProcessor`.
    /// *   `max_pending_messages` - the largest allowed pending messages before
    /// `send_item` waits for capacity.
    /// *   `max_time_between_batchs - the maximum time until the submitted items will be
    /// send, even if the `max_batch_size` has note been reached.
    /// *   `batch_processor` - A implemention of a BatchProcessor that receives batches.
    pub fn new(
        max_batch_size: usize,
        max_pending_messages: usize,
        max_time_between_batches: time::Duration,
        batch_processor: Arc<Mutex<dyn BatchProcessor<T, S, E> + Send>>,
    ) -> NanoBatcher<T, S, E> {
        //Channels for Batch Items
        let (send, mut rx) = mpsc::channel::<BatchItem<T, S, E>>(max_pending_messages);
        //Notifiction for shutdown.
        let closed_notifier = Arc::new(Notify::new());
        let shutdown_notifier = Arc::new(Notify::new());
        //function to generate next batch time
        let new_deadline = move || time::Instant::now() + max_time_between_batches;

        //cross thread items
        let task_closed = closed_notifier.clone();
        let batcher_shutdown = shutdown_notifier.clone();
        let bp = batch_processor.clone();
        tokio::spawn(async move {
            let mut buffer: Vec<BatchItem<T, S, E>> = Vec::with_capacity(max_batch_size);
            let mut delay = time::delay_until(new_deadline());

            //event loop waiting on timers, notifications and channels.
            loop {
                tokio::select! {
                    _ =  task_closed.notified().fuse() => {
                        rx.close();
                    }
                    _ = &mut delay => {
                        if !buffer.is_empty(){
                            NanoBatcher::submit_batch(&bp, &mut buffer, max_batch_size).await
                        }
                        delay.reset(new_deadline());
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(job) => {
                                buffer.push(job);
                                if buffer.len() >= max_batch_size {
                                    NanoBatcher::submit_batch(&bp,
                                        &mut buffer,
                                        max_batch_size).await;
                                    delay.reset(new_deadline());
                                 }
                            },
                            None => {
                                if !buffer.is_empty()  {
                                    NanoBatcher::submit_batch(&bp,
                                        &mut buffer,
                                        max_batch_size).await;
                                 }
                                 batcher_shutdown.notify();
                                break;
                            }
                        }
                    }
                };
            }
        });
        NanoBatcher {
            command_channel: send,
            closed_notifier,
            shutdown_notifier,
            closed: false,
            is_shutdown: tokio::sync::Mutex::new(false),
        }
    }

    /// Submits the provided batch and returns the results to the
    /// oneshot Sender channels for each BatchItem.
    /// # Arguments
    /// * `batch_processor` - A implemention o `BatchProcessor` used to
    /// process the batch.
    /// *   `buffer` - The buffer containing the `BatchItems` to be sent.
    /// *   `buffer_capacity` - The capacity of the buffer.
    async fn submit_batch(
        batch_processor: &Arc<Mutex<dyn BatchProcessor<T, S, E> + Send>>,
        buffer: &mut Vec<BatchItem<T, S, E>>,
        buffer_capacity: usize,
    ) {
        let batch = replace(buffer, Vec::with_capacity(buffer_capacity));
        let batch_processor = batch_processor.clone();
        let (requests, channels): (Vec<_>, Vec<_>) =
            batch.into_iter().map(|BatchItem(q, r)| (q, r)).unzip();
        //There is no guarantee batch_processor will not lock so spawn
        //the task in a seperate worker thread.
        let results = tokio::task::spawn_blocking(move || {
            batch_processor.lock().unwrap().process_batch(requests)
        })
        .await;
        match results {
            Ok(Ok(results)) => results
                .into_iter()
                .zip(channels)
                .for_each(|(result, sender)| {
                    let _ = sender.send(Ok(result));
                }),
            Ok(Err(e)) => channels.into_iter().for_each(|c| {
                let _ = c.send(Err(BatchItemError::ItemError(e.clone())));
            }),
            Err(e) if e.is_panic() => channels.into_iter().for_each(|c| {
                let _ = c.send(Err(BatchItemError::Paniced));
            }),
            Err(_) => channels.into_iter().for_each(|c| {
                let _ = c.send(Err(BatchItemError::Cancelled));
            }),
        }
    }

    /// Shuts down this NanoBatcher, the returned future will not complete
    /// until the previously accepted BatchItems are processed.  After calling
    /// this method futher calls to `send_item` wil return imeidiately `Err`.
    pub async fn shutdown(&mut self) {
        let mut is_shut_down = self.is_shutdown.lock().await;
        if !*is_shut_down {
            self.closed_notifier.notify();
            self.closed = true;
            self.shutdown_notifier.notified().await;
            *is_shut_down = true;
        }
    }

    /// Returns a `mpl Future<Output = NanoBatchResult<Result<S, E>, T>> + '_`
    /// the outer future relates to submission of the batch,
    /// where as the inner future relates to the response from the BatchProcessor.
    ///
    /// # Arguments
    /// *   `q` - the request.
    /// *   `timeout` - the time to wait before failing to send if `Some` else
    /// wait indefinitely.
    pub async fn send_item(
        &self,
        q: T,
        timeout: Option<time::Duration>,
    ) -> impl Future<Output = NanoBatchResult<Result<S, BatchItemError<E>>, T>> {
        let result = if self.closed {
            Err(NanoBatchError::ShutDown(q))
        } else {
            let (resp_tx, resp_rx) = oneshot::channel::<Result<S, BatchItemError<E>>>();
            let mut x = self.command_channel.clone();
            match timeout {
                Some(timeout) => {
                    x.send_timeout(BatchItem(q, resp_tx), timeout)
                        .map_err(|e| match e {
                            mpsc::error::SendTimeoutError::Timeout(BatchItem(q, _)) => {
                                NanoBatchError::Timeout(q)
                            }
                            mpsc::error::SendTimeoutError::Closed(BatchItem(q, _)) => {
                                NanoBatchError::Closed(q)
                            }
                        })
                        .await
                }
                None => {
                    x.send(BatchItem(q, resp_tx))
                        .map_err(|mpsc::error::SendError(BatchItem(q, _))| {
                            NanoBatchError::Closed(q)
                        })
                        .await
                }
            }
            .map(|_| resp_rx)
        };
        future::ready(result).and_then(|resp_rx| resp_rx.map_err(|_| NanoBatchError::ReceiverError))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::Poll;
    use futures::{pin_mut, poll};
    use tokio::time;

    struct TestBatchProcessor {
        is_erroring: bool,
        is_panic: bool,
        delay: Option<time::Duration>,
    }

    impl TestBatchProcessor {
        fn new_ok(delay: Option<time::Duration>) -> TestBatchProcessor {
            TestBatchProcessor {
                is_erroring: false,
                is_panic: false,
                delay,
            }
        }

        fn new_panic() -> TestBatchProcessor {
            TestBatchProcessor {
                is_erroring: false,
                is_panic: true,
                delay: None,
            }
        }

        fn new_err() -> TestBatchProcessor {
            TestBatchProcessor {
                is_erroring: true,
                is_panic: false,
                delay: None,
            }
        }
    }

    impl BatchProcessor<i32, i32, String> for TestBatchProcessor {
        fn process_batch(&self, batch: Vec<i32>) -> Result<Vec<i32>, String> {
            if self.is_panic {
                panic!("Batch Panic")
            } else if self.is_erroring {
                Err("Batch failed".into())
            } else {
                if let Some(delay) = self.delay {
                    std::thread::sleep(delay);
                }
                Ok(batch.iter().map(|x| x + 2).collect::<Vec<_>>())
            }
        }
    }

    #[tokio::test()]
    async fn test_batch_sent_test() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = Arc::new(NanoBatcher::new(
            2,
            32,
            time::Duration::from_secs(10),
            processor,
        ));
        let bt = batcher.clone();
        let result_one = tokio::spawn(async move { bt.send_item(1, None).flatten().await });
        let bt = batcher.clone();
        let result_two = tokio::spawn(async move { bt.send_item(2, None).flatten().await });
        assert_eq!(result_one.await.unwrap(), Ok(Ok(3)));
        assert_eq!(result_two.await.unwrap(), Ok(Ok(4)));
    }

    #[tokio::test()]
    async fn test_batch_time_test() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = NanoBatcher::new(10, 32, time::Duration::from_millis(100), processor);
        let job_result_one = batcher.send_item(1, None).await;
        let job_result_two = batcher.send_item(2, None).await;
        assert_eq!(job_result_one.await, Ok(Ok(3)));
        assert_eq!(job_result_two.await, Ok(Ok(4)));
    }

    #[tokio::test]
    async fn test_batch_not_sent_test() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_secs(10), processor);
        let job_result_one = batcher.send_item(1, None).await;
        pin_mut!(job_result_one);
        assert_eq!(poll!(job_result_one), Poll::Pending);
    }

    #[tokio::test]
    async fn test_close() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_secs(10), processor);
        batcher.shutdown().await;
        let job_result_one = batcher.send_item(1, None).await;
        assert_eq!(job_result_one.await, Err(NanoBatchError::ShutDown(1)));
    }

    #[tokio::test()]
    async fn test_timeout() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(Some(
            time::Duration::from_millis(500),
        ))));
        let batcher = NanoBatcher::new(1, 1, time::Duration::from_millis(1), processor);
        let _ = batcher.send_item(1, None).await; //Process one
        let _ = batcher.send_item(1, None).await; //Fill the channel
        let job_result_two = batcher
            .send_item(2, Some(time::Duration::from_millis(1)))
            .await;
        assert_eq!(job_result_two.await, Err(NanoBatchError::Timeout(2)))
    }

    #[tokio::test]
    async fn test_closed_timeout_error() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_secs(10), processor);
        batcher.closed_notifier.notify();
        tokio::task::yield_now().await;
        let job_result_one = batcher
            .send_item(1, Some(time::Duration::from_secs(10)))
            .await;
        assert_eq!(job_result_one.await, Err(NanoBatchError::Closed(1)));
    }

    #[tokio::test]
    async fn test_closed_no_timeout_error() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_secs(10), processor);
        batcher.closed_notifier.notify();
        tokio::task::yield_now().await;
        let job_result_one = batcher.send_item(1, None).await;
        assert_eq!(job_result_one.await, Err(NanoBatchError::Closed(1)));
    }

    #[tokio::test]
    async fn test_shutdown_drain() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_ok(None)));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_millis(500), processor);
        let job_result_one = batcher.send_item(1, None).await;
        let job_result_two = batcher.send_item(2, None).await;
        batcher.shutdown().await;
        assert_eq!(job_result_one.await, Ok(Ok(3)));
        assert_eq!(job_result_two.await, Ok(Ok(4)));
    }

    #[tokio::test]
    async fn test_batch_processor_error() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_err()));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_millis(100), processor);
        let job_result_one = batcher.send_item(1, None).await;
        let job_result_two = batcher.send_item(2, None).await;
        batcher.shutdown().await;
        assert_eq!(
            job_result_one.await,
            Ok(Err(BatchItemError::ItemError("Batch failed".into())))
        );
        assert_eq!(
            job_result_two.await,
            Ok(Err(BatchItemError::ItemError("Batch failed".into())))
        );
    }

    #[tokio::test]
    async fn test_batch_processor_panic() {
        let processor = Arc::new(Mutex::new(TestBatchProcessor::new_panic()));
        let batcher = &mut NanoBatcher::new(2, 32, time::Duration::from_millis(100), processor);
        let job_result_one = batcher.send_item(2, None).await;
        assert_eq!(job_result_one.await, Ok(Err(BatchItemError::Paniced)));
    }
}

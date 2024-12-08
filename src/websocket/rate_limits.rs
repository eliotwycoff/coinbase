use crate::common::Error;
use std::sync::Arc;
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{interval, Duration, MissedTickBehavior},
};

pub struct RateLimiter {
    token_bucket: Arc<Semaphore>,
    join_handle: JoinHandle<()>,
}

impl RateLimiter {
    pub fn new(duration: Duration, capacity: usize) -> Self {
        let token_bucket = Arc::new(Semaphore::new(capacity));

        // Refill the token bucket at the end of each interval.
        let join_handle = tokio::task::spawn({
            let token_bucket = token_bucket.clone();
            let mut interval = interval(duration);

            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            async move {
                loop {
                    interval.tick().await;

                    if token_bucket.available_permits() < capacity {
                        token_bucket.add_permits(1);
                    }
                }
            }
        });

        Self {
            token_bucket,
            join_handle,
        }
    }

    pub async fn acquire(&self) -> Result<(), Error> {
        let permit = self.token_bucket.acquire().await?;

        permit.forget();

        Ok(())
    }
}

impl Drop for RateLimiter {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

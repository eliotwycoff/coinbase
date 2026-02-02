use crate::exchange::common::Error;
use mule::BackOff;
use std::sync::Arc;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::{self, Duration, MissedTickBehavior},
};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct TokenBucket {
    semaphore: Arc<Semaphore>,
    drop_stack: Arc<Mutex<Vec<OwnedSemaphorePermit>>>,
    wait_period: Duration,
}

impl TokenBucket {
    pub fn new(capacity: usize, wait_period: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            drop_stack: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
            wait_period,
        }
    }

    pub async fn get_token(&self) -> Result<OwnedSemaphorePermit, Error> {
        Ok(self.semaphore.clone().acquire_owned().await?)
    }

    pub async fn return_token(&self, token: OwnedSemaphorePermit) -> Result<(), Error> {
        // Get a guard on the drop stack and determine if there is an active task.
        let mut drop_stack_guard = self.drop_stack.lock().await;
        let no_active_task = drop_stack_guard.len() == 0;

        // Add the semaphore permit to the drop stack, then release the guard.
        drop_stack_guard.push(token);
        drop(drop_stack_guard);

        if no_active_task {
            // Prepare variable copies for the token-dropping task.
            let drop_stack = self.drop_stack.clone();
            let wait_period = self.wait_period;

            // Spawn a tokio task to drop this token and any others that get put
            // into the stack. This task completes when the stack is empty.
            tokio::spawn(async move {
                // Set up and configure the token wait period.
                let mut wait_period = time::interval(wait_period);

                // Configure the wait period.
                wait_period.set_missed_tick_behavior(MissedTickBehavior::Skip);
                wait_period.tick().await; // first tick returns immediately

                // Loop until all semaphore permits have been dropped.
                loop {
                    // Wait before dropping a permit.
                    wait_period.tick().await;

                    // Get a guard on the drop stack and remove one token.
                    let mut drop_stack_guard = drop_stack.lock().await;
                    let token = drop_stack_guard.pop();

                    // Release the drop stack guard.
                    drop(drop_stack_guard);

                    // Break the loop if there are no more tokens to drop.
                    if token.is_none() {
                        break;
                    }
                }
            });
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct BackOffBucket {
    semaphore: Arc<Semaphore>,
    backoff: Arc<Mutex<BackOff>>,
}

impl BackOffBucket {
    pub fn new(min_wait: Duration, max_wait: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(1)),
            backoff: Arc::new(Mutex::new(BackOff::new(
                min_wait.as_millis() as u64,
                max_wait.as_millis() as u64,
            ))),
        }
    }

    pub async fn get_token(&self) -> Result<OwnedSemaphorePermit, Error> {
        Ok(self.semaphore.clone().acquire_owned().await?)
    }

    pub async fn return_token(&self, token: OwnedSemaphorePermit) {
        tokio::spawn({
            let backoff = self.backoff.clone();

            async move {
                backoff
                    .lock()
                    .await
                    .tick(|ms| debug!("Returning backoff token in {ms} ms"))
                    .await;
                drop(token);
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::Instant;

    #[tokio::test]
    async fn can_get_capacity_tokens_in_burst() -> Result<(), Box<dyn std::error::Error>> {
        let token_bucket = TokenBucket::new(1_000, Duration::from_millis(100));
        let mut tokens = Vec::with_capacity(1_000);
        let t0 = Instant::now();

        for _ in 0..1_000 {
            tokens.push(token_bucket.get_token().await?);
        }

        let elapsed = Instant::now() - t0;

        assert!(elapsed.as_millis() < 100);

        for _ in 0..1_000 {
            token_bucket.return_token(tokens.pop().unwrap()).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_get_rate_limited_tokens_after_wait_period()
    -> Result<(), Box<dyn std::error::Error>> {
        let token_bucket = TokenBucket::new(1_000, Duration::from_millis(100));
        let mut tokens = Vec::with_capacity(1_000);

        for _ in 0..1_000 {
            tokens.push(token_bucket.get_token().await?);
        }

        let t0 = Instant::now();

        // Return five tokens.
        for _ in 0..5 {
            token_bucket.return_token(tokens.pop().unwrap()).await?;
        }

        // Get five more tokens.
        for _ in 0..5 {
            tokens.push(token_bucket.get_token().await?);
        }

        let elapsed = Instant::now() - t0;

        assert!(elapsed.as_millis() > 500);

        Ok(())
    }
}

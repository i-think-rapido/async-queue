use std::sync::Arc;
use anyhow::Result;
use parking_lot::RwLock;
use rand::Rng;
use async_queue_runner::{Continue, AsyncQueue};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let num = Arc::new(RwLock::new(5));

    // initializing the runner
    let runner = AsyncQueue::new(
        5, // the amount of parellel processes

        // inject producer function
        move || {
            *num.write() += 1;
            let idx = *num.read();

            if idx > 15 {
                return Continue::Done
            }
    
            log::info!("pr: ------------< {}", idx);
    
            // the sleep is simulating the processing of the async task
            // sleeps a random amount of time
    
            Continue::Next(idx)
        },

        |input| {
            std::thread::sleep(std::time::Duration::from_millis(300 + rand::thread_rng().gen_range(33..333)));
            log::info!("pr: {input}");
            Ok(input)
        },

        // inject consumer function
        |output| { 
            log::info!("pr: .................. {:?}", output); 
        }
    );

    // here we execute the process and wait for finishing
    runner.run().await?;

    Ok(())
}
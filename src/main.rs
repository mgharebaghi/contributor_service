use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use std::error::Error;
use tokio::time::{sleep, Duration};
mod make_trx;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let centichain_db = client.database("Centichain");
    let validators_coll = centichain_db.collection::<Document>("validators");

    let mut change_stream = validators_coll.watch().await?;

    let mut is_running = false;

    loop {
        let count = validators_coll.count_documents(doc! {}).await.unwrap_or(0);

        if count > 0 && !is_running {
            tokio::time::sleep(Duration::from_secs(30)).await; // Wait for 5 seconds before starting
            println!("Starting transaction sending - Validator count: {}", count);
            is_running = true;
            tokio::spawn(make_trx::make());
        } else if count == 0 && is_running {
            println!("Stopping transaction sending - No validators");
            is_running = false;
        }

        // Wait for next change or timeout
        tokio::select! {
            change = change_stream.next() => {
                match change {
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => println!("Error in change stream: {}", e),
                    None => break,
                }
            }
            _ = sleep(Duration::from_secs(120)) => continue,
        }
    }

    Ok(())
}

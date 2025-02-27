use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};
use std::error::Error;
mod make_trx;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let centichain_db = client.database("Centichain");
    let validators_coll = centichain_db.collection::<Document>("validators");

    // Check initial validator count and start transactions if > 0
    let initial_count = validators_coll.count_documents(doc! {}).await.unwrap_or(0);
    if initial_count > 0 {
        println!("Started transaction sending");
        make_trx::make().await;
    }

    Ok(())
}

use chrono::{SubsecRound, Utc};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    change_stream::event::ChangeStreamEvent,
    options::ChangeStreamOptions,
    Client, Collection, Database,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
mod make_trx;

#[derive(Debug, Serialize, Deserialize)]
struct Contributor {
    peer_id: String,
    wallet: String,
    node_type: String,
    join_date: String,
    deactive_date: String,
}

struct MongoDBWatcher {
    centichain_db: Database,
    centiweb_db: Database,
}

impl MongoDBWatcher {
    async fn new(client: Client) -> Self {
        let centichain_db = client.database("Centichain");
        let centiweb_db = client.database("centiweb");

        MongoDBWatcher {
            centichain_db,
            centiweb_db,
        }
    }

    async fn process_validator_change(
        &self,
        change: ChangeStreamEvent<Document>,
    ) -> Result<(), Box<dyn Error>> {
        let contributors_coll: Collection<Contributor> =
            self.centiweb_db.collection("contributors");

        match change.operation_type {
            mongodb::change_stream::event::OperationType::Insert => {
                if let Some(doc) = change.full_document {
                    let wallet = doc.get_str("wallet").unwrap_or_default().to_string();
                    let peer_id = if let Some(doc_key) = &change.document_key {
                        // Try to get _id as ObjectId first, then convert to string
                        match doc_key.get("_id") {
                            Some(id) => id.to_string(),
                            None => "".to_string(),
                        }
                    } else {
                        "".to_string()
                    };

                    let new_contributor = Contributor {
                        peer_id,
                        wallet,
                        node_type: "validator".to_string(),
                        join_date: Utc::now().round_subsecs(0).to_string(),
                        deactive_date: "".to_string(),
                    };

                    contributors_coll.insert_one(new_contributor).await?;
                }
            }
            mongodb::change_stream::event::OperationType::Delete => {
                if let Some(doc_key) = change.document_key {
                    if let Some(peer_id) = doc_key.get("_id") {
                        let current_time = Utc::now().round_subsecs(0).to_string();
                        let update_result = contributors_coll
                            .update_one(
                                doc! {"peer_id": peer_id.to_string()},
                                doc! {
                                    "$set": { "deactive_date": current_time }
                                },
                            )
                            .await?;

                        if update_result.modified_count == 0 {
                            eprintln!(
                                "No validator contributor found to deactivate for peer_id: {}",
                                peer_id
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn process_relay_change(
        &self,
        change: ChangeStreamEvent<Document>,
    ) -> Result<(), Box<dyn Error>> {
        let contributors_coll: Collection<Contributor> =
            self.centiweb_db.collection("contributors");

        match change.operation_type {
            mongodb::change_stream::event::OperationType::Insert => {
                if let Some(doc) = change.full_document {
                    let wallet = doc.get_str("wallet").unwrap_or_default().to_string();
                    let peer_id = if let Some(doc_key) = &change.document_key {
                        // Try to get _id as ObjectId first, then convert to string
                        match doc_key.get("_id") {
                            Some(id) => id.to_string(),
                            None => "".to_string(),
                        }
                    } else {
                        "".to_string()
                    };

                    let new_contributor = Contributor {
                        peer_id,
                        wallet,
                        node_type: "relay".to_string(),
                        join_date: Utc::now().round_subsecs(0).to_string(),
                        deactive_date: "".to_string(),
                    };

                    contributors_coll.insert_one(new_contributor).await?;
                }
            }
            mongodb::change_stream::event::OperationType::Delete => {
                if let Some(doc_key) = change.document_key {
                    if let Some(peer_id) = doc_key.get("_id") {
                        let current_time = Utc::now().round_subsecs(0).to_string();
                        let update_result = contributors_coll
                            .update_one(
                                doc! {
                                    "peer_id": peer_id.to_string(),
                                },
                                doc! {
                                    "$set": { "deactive_date": current_time }
                                },
                            )
                            .await?;

                        if update_result.modified_count == 0 {
                            eprintln!(
                                "No relay contributor found to deactivate for peer_id: {}",
                                peer_id
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn watch_collections(&self) -> Result<(), Box<dyn Error>> {
        let validators_coll: Collection<Document> = self.centichain_db.collection("validators");
        let relays_coll: Collection<Document> = self.centiweb_db.collection("relays");

        let options = ChangeStreamOptions::builder()
            .full_document(Some(mongodb::options::FullDocumentType::UpdateLookup))
            .build();

        let mut validators_stream = validators_coll
            .watch()
            .with_options(options.clone())
            .await?;
        let mut relays_stream = relays_coll.watch().with_options(options).await?;

        loop {
            tokio::select! {
                Some(validator_change) = validators_stream.next() => {
                    if let Ok(change) = validator_change {
                        if let Err(e) = self.process_validator_change(change).await {
                            eprintln!("Error processing validator change: {}", e);
                        }
                    }
                }
                Some(relay_change) = relays_stream.next() => {
                    if let Ok(change) = relay_change {
                        if let Err(e) = self.process_relay_change(change).await {
                            eprintln!("Error processing relay change: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let watcher = MongoDBWatcher::new(client.clone()).await;

    println!("Starting MongoDB watcher...");

    let watcher_handle = tokio::spawn(async move {
        if let Err(e) = watcher.watch_collections().await {
            eprintln!("Error in watcher: {}", e);
        }
    });

    let client_clone = client.clone();
    let transaction_handle = tokio::spawn(async move {
        let centichain_db = client_clone.database("Centichain");
        let validators_coll = centichain_db.collection::<Document>("validators");
        let options = ChangeStreamOptions::builder()
            .full_document(Some(mongodb::options::FullDocumentType::UpdateLookup))
            .build();

        let mut transaction_task: Option<tokio::task::JoinHandle<()>> = None;

        // Check initial validator count and start transactions if > 0
        let initial_count = validators_coll.count_documents(doc! {}).await.unwrap_or(0);
        if initial_count > 0 {
            // Spawn in a separate task so it doesn't block
            transaction_task = Some(tokio::spawn(async {
                make_trx::make().await;
            }));
            println!("Started transaction sending");
        }

        // Watch for changes in a separate task
        let watch_task = tokio::spawn(async move {
            let mut validator_stream = validators_coll.watch().with_options(options).await.unwrap();
            
            while let Some(Ok(change)) = validator_stream.next().await {
                match change.operation_type {
                    mongodb::change_stream::event::OperationType::Delete => {
                        let count = validators_coll.count_documents(doc! {}).await.unwrap_or(0);
                        if count == 0 {
                            if let Some(handle) = transaction_task.take() {
                                handle.abort();
                                println!("Stopped transaction sending - no validators");
                            }
                        }
                    }
                    mongodb::change_stream::event::OperationType::Insert => {
                        let count = validators_coll.count_documents(doc! {}).await.unwrap_or(0);
                        if count > 0 && transaction_task.is_none() {
                            transaction_task = Some(tokio::spawn(async {
                                make_trx::make().await;
                            }));
                            println!("Started transaction sending");
                        }
                    }
                    _ => {}
                }
            }
        });

        // Wait for the watch task to complete
        if let Err(e) = watch_task.await {
            eprintln!("Error in validator watch task: {}", e);
        }
    });

    let _ = tokio::join!(watcher_handle, transaction_handle);

    Ok(())
}

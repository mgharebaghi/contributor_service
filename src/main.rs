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

#[derive(Debug, Serialize, Deserialize)]
struct Contributor {
    wallet: String,
    node_type: String,
    join_date: String,
    deactive_date: Option<String>,
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

                    let new_contributor = Contributor {
                        wallet,
                        node_type: "validator".to_string(),
                        join_date: Utc::now().round_subsecs(0).to_string(),
                        deactive_date: None,
                    };

                    contributors_coll.insert_one(new_contributor).await?;
                }
            }
            mongodb::change_stream::event::OperationType::Delete => {
                if let Some(doc_key) = change.document_key {
                    if let Ok(wallet) = doc_key.get_str("wallet") {
                        contributors_coll
                            .update_many(
                                doc! {
                                    "wallet": wallet,
                                    "node_type": "validator"
                                },
                                doc! {
                                    "$set": { "deactive_date": Utc::now().round_subsecs(0).to_string() }
                                },
                            )
                            .await?;
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

                    let new_contributor = Contributor {
                        wallet,
                        node_type: "relay".to_string(),
                        join_date: Utc::now().round_subsecs(0).to_string(),
                        deactive_date: None,
                    };

                    contributors_coll.insert_one(new_contributor).await?;
                }
            }
            mongodb::change_stream::event::OperationType::Delete => {
                if let Some(doc_key) = change.document_key {
                    if let Ok(wallet) = doc_key.get_str("wallet") {
                        contributors_coll
                            .update_many(
                                doc! {
                                    "wallet": wallet,
                                    "node_type": "relay"
                                },
                                doc! {
                                    "$set": { "deactive_date": Utc::now().round_subsecs(0).to_string() }
                                },
                            )
                            .await?;
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
    let watcher = MongoDBWatcher::new(client).await;

    println!("Starting MongoDB watcher...");
    watcher.watch_collections().await?;

    Ok(())
}

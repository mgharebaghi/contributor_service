use cc_transaction::Transaction;
use rand;

pub async fn make() {
    eprintln!("Starting transaction");
    let wallet = "5FAGvcnSCa4hvh5YbWxecDCz1weEjttgBYbLK4RcABJX1nD1".to_string();
    let private_key = "belt reason ten attitude panda music aim grass accuse calm wing ocean used arrest cushion avocado expose tourist camera fly security muffin critic patient".to_string();
    let recipient = "5Fo2ocSQ7hZsi7GkFhdtmTmkaewsiDX3u1TmMaJvNHUPKL5f".to_string();

    let wallet2 = "5Fo2ocSQ7hZsi7GkFhdtmTmkaewsiDX3u1TmMaJvNHUPKL5f".to_string();
    let private_key2 = "flag weasel bone office welcome actress reject bubble crowd kid bind item artwork soup video amused neither immense awake cattle wild announce loan drill
".to_string();
    let recipient2 = "5GzD9joTTewLC5ZNLPnsC7SiTtS9FdoYEdSX5a6A9JmvUQG5".to_string();

    let wallet3 = "5GzD9joTTewLC5ZNLPnsC7SiTtS9FdoYEdSX5a6A9JmvUQG5".to_string();
    let private_key3 = "awake huge child trip pupil village shallow doctor home hunt audit device poverty quality hard violin swamp stick medal setup comfort dismiss hour scheme
".to_string();
    let recipient3 = "5FAGvcnSCa4hvh5YbWxecDCz1weEjttgBYbLK4RcABJX1nD1".to_string();

    let handle1 = tokio::spawn(async move {
        loop {
            let len = rand::random::<f64>() * 0.001;
            let value = format!("{:.12}", len).to_string();
            match Transaction::make_and_send(
                wallet.clone(),
                private_key.clone(),
                recipient.clone(),
                value.clone(),
            )
            .await
            {
                Ok(_tx) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    println!("Error in transaction 1: {}", e);
                    break;
                }
            }
        }
    });

    let handle2 = tokio::spawn(async move {
        loop {
            let len = rand::random::<f64>() * 0.001;
            let value = format!("{:.12}", len).to_string();
            match Transaction::make_and_send(
                wallet2.clone(),
                private_key2.clone(),
                recipient2.clone(),
                value.clone(),
            )
            .await
            {
                Ok(_tx) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                }
                Err(e) => {
                    println!("Error in transaction 2: {}", e);
                    break;
                }
            }
        }
    });

    let handle3 = tokio::spawn(async move {
        loop {
            let len = rand::random::<f64>() * 0.001;
            let value = format!("{:.12}", len).to_string();
            match Transaction::make_and_send(
                wallet3.clone(),
                private_key3.clone(),
                recipient3.clone(),
                value.clone(),
            )
            .await
            {
                Ok(_tx) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                }
                Err(e) => {
                    println!("Error in transaction 2: {}", e);
                    break;
                }
            }
        }
    });

    let _ = tokio::join!(handle1, handle2, handle3);
}

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;

#[derive(Serialize)]
struct TradeData {
    token: String,
    price_in_sol: f64,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Redpanda Trade Data Producer");
    
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "10000")
        .create()?;

    println!("✅ Connected to Redpanda");
    
    let tokens = vec!["SOL", "ETH", "BTC", "USDC", "RAY"];
    let mut prices = vec![100.0, 3000.0, 50000.0, 1.0, 5.0];
    let mut message_count = 0;

    println!("🎯 Starting to send trade data to 'trade-data' topic...");
    println!("==========================================");

    loop {
        for (i, token) in tokens.iter().enumerate() {
            message_count += 1;
            
            // Generate realistic price movement
            let variation = (rand::random::<f64>() - 0.5) * prices[i] * 0.02;
            prices[i] = (prices[i] + variation).max(0.1);

            let trade_data = TradeData {
                token: token.to_string(),
                price_in_sol: prices[i],
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            };

            let json_data = serde_json::to_string(&trade_data)?;
            
            let key_string = token.to_string();
            let record = FutureRecord::to("trade-data")
                .payload(&json_data)
                .key(&key_string);

            println!("📤 Sending {}: {:.4} SOL", token, prices[i]);
            
            match producer.send(record, Duration::from_secs(5)).await {
                Ok((partition, offset)) => {
                    println!("✅ Success! Partition: {}, Offset: {}", partition, offset);
                }
                Err((e, _)) => {
                    println!("❌ Failed to send: {}", e);
                }
            }
        }

        println!("📊 Total messages sent: {}", message_count);
        println!("----------------------------------------");
        
        time::sleep(Duration::from_secs(2)).await;
    }
}
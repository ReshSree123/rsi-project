use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Message, BorrowedMessage};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct TradeData {
    token: String,
    price_in_sol: f64,
    timestamp: u64,
}

struct RsiCalculator {
    prices: Vec<f64>,
}

impl RsiCalculator {
    const PERIOD: usize = 14;

    fn new() -> Self {
        Self { prices: Vec::new() }
    }

    fn add_price(&mut self, price: f64) {
        self.prices.push(price);
        if self.prices.len() > Self::PERIOD + 1 {
            self.prices.remove(0);
        }
    }

    fn calculate(&self) -> Option<f64> {
        if self.prices.len() <= Self::PERIOD {
            return None;
        }

        let mut gains = 0.0;
        let mut losses = 0.0;

        for i in 1..self.prices.len() {
            let change = self.prices[i] - self.prices[i - 1];
            if change > 0.0 {
                gains += change;
            } else {
                losses -= change;
            }
        }

        let avg_gain = gains / Self::PERIOD as f64;
        let avg_loss = losses / Self::PERIOD as f64;

        if avg_loss.abs() < f64::EPSILON {
            return Some(100.0);
        }

        let rs = avg_gain / avg_loss;
        let rsi = 100.0 - (100.0 / (1.0 + rs));

        Some(rsi.max(0.0).min(100.0))
    }

    fn get_status(rsi: f64) -> String {
        match rsi {
            r if r >= 70.0 => "OVERBOUGHT".to_string(),
            r if r <= 30.0 => "OVERSOLD".to_string(),
            _ => "NEUTRAL".to_string(),
        }
    }
}

fn main() -> Result<()> {
    println!("🚀 ENHANCED Redpanda Consumer");
    println!("📡 Attempting to connect...");
    
    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", format!("consumer-{}", rand::random::<u32>())) // Unique group ID
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.partition.eof", "false")
        .create()?;

    println!("✅ Consumer created successfully");
    
    // Subscribe to topic
    println!("🔍 Subscribing to 'trade-data'...");
    match consumer.subscribe(&["trade-data"]) {
        Ok(_) => println!("✅ Subscribed to 'trade-data' topic"),
        Err(e) => {
            eprintln!("❌ Failed to subscribe: {}", e);
            eprintln!("💡 The topic might not exist. Create it with:");
            eprintln!("   docker exec redpanda rpk topic create trade-data");
            return Ok(());
        }
    }
    
    println!("🎯 Ready to consume messages!");
    println!("💡 Start the producer in another terminal: cargo run --bin producer");
    println!("==========================================");
    
    let mut calculators = std::collections::HashMap::new();
    let mut message_count = 0;
    let mut last_status_time = std::time::Instant::now();
    
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                message_count += 1;
                if let Err(e) = handle_message(&message, message_count, &mut calculators) {
                    eprintln!("Error processing message: {}", e);
                }
                last_status_time = std::time::Instant::now();
            }
            Some(Err(e)) => {
                eprintln!("❌ Kafka error: {}", e);
            }
            None => {
                // Show status periodically
                if last_status_time.elapsed().as_secs() > 10 && message_count == 0 {
                    println!("⏳ Waiting for messages...");
                    println!("   Make sure:");
                    println!("   1. Redpanda is running: docker ps");
                    println!("   2. Topic exists: docker exec redpanda rpk topic list");
                    println!("   3. Producer is running: cargo run --bin producer");
                    last_status_time = std::time::Instant::now();
                }
            }
        }
    }
}

fn handle_message(
    message: &BorrowedMessage<'_>, 
    count: i32,
    calculators: &mut std::collections::HashMap<String, RsiCalculator>
) -> Result<()> {
    let payload = match message.payload() {
        Some(p) => p,
        None => {
            println!("📨 Message #{}: (no payload)", count);
            return Ok(());
        }
    };
    
    // Try to parse as JSON trade data
    match serde_json::from_slice::<TradeData>(payload) {
        Ok(trade_data) => {
            println!("📨 Message #{}:", count);
            println!("   💰 {}: {:.4} SOL", trade_data.token, trade_data.price_in_sol);
            println!("   ⏰ {}", trade_data.timestamp);
            
            // Show partition/offset info (FIXED - no Option wrapping)
            let partition = message.partition();
            let offset = message.offset();
            println!("   📊 Partition: {}, Offset: {}", partition, offset);
            
            // Process RSI calculation
            process_rsi_calculation(&trade_data, calculators);
        }
        Err(_) => {
            // If not JSON, show as raw text
            let text = String::from_utf8_lossy(payload);
            println!("📨 Message #{}: {}", count, text);
        }
    }
    
    println!("----------------------------------------");
    Ok(())
}

fn process_rsi_calculation(
    trade_data: &TradeData,
    calculators: &mut std::collections::HashMap<String, RsiCalculator>
) {
    let calculator = calculators
        .entry(trade_data.token.clone())
        .or_insert_with(RsiCalculator::new);

    calculator.add_price(trade_data.price_in_sol);

    if let Some(rsi) = calculator.calculate() {
        let status = RsiCalculator::get_status(rsi);
        println!("   📊 RSI: {:.2} ({})", rsi, status);
        println!("   📈 Based on {} price points", calculator.prices.len() - 1);
    } else {
        let collected = calculator.prices.len().saturating_sub(1);
        println!("   📈 Collecting data: {}/14 periods", collected);
    }
}
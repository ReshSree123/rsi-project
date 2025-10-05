use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;  // Add this import
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing Redpanda Connection...");
    
    // Test Producer
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "10000")
        .create()?;
    
    println!("✅ Producer created");
    
    // Test Consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "test-group")
        .set("auto.offset.reset", "earliest")
        .create()?;
        
    println!("✅ Consumer created");
    
    // Subscribe to test topic
    consumer.subscribe(&["test-topic"])?;
    println!("✅ Subscribed to test-topic");
    
    // Send a test message
    let test_message = "Hello Redpanda!";
    let record = BaseRecord::to("test-topic")
        .payload(test_message)
        .key("test-key");
    
    println!("📤 Sending test message...");
    
    match producer.send(record) {
        Ok(_) => {
            producer.poll(Duration::from_millis(1000));
            println!("✅ Message sent to producer buffer");
            
            // Try to receive the message
            println!("📥 Attempting to receive message...");
            for _ in 0..10 { // Try for 10 attempts
                if let Some(message) = consumer.poll(Duration::from_millis(1000)) {
                    match message {
                        Ok(msg) => {
                            if let Some(payload) = msg.payload() {
                                let text = String::from_utf8_lossy(payload);
                                println!("🎉 SUCCESS: Received message: {}", text);
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            println!("❌ Consumer error: {}", e);
                        }
                    }
                } else {
                    println!("⏳ No message received yet...");
                }
            }
            
            println!("❌ No message received after 10 seconds");
        }
        Err((e, _)) => {
            println!("❌ Failed to send message: {}", e);
        }
    }
    
    Ok(())
}
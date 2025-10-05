use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Fixing Redpanda Topics...");
    
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()?;

    // First, try to delete existing topics (ignore errors if they don't exist)
    println!("🗑️  Deleting existing topics...");
    let _ = admin_client.delete_topics(&["trade-data", "rsi-data"], &AdminOptions::new()).await;

    // Wait a moment
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create topics with proper configuration
    println!("📝 Creating new topics...");
    let topics = vec![
        NewTopic {
            name: "trade-data",
            num_partitions: 1,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        },
        NewTopic {
            name: "rsi-data", 
            num_partitions: 1,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        },
    ];

    let result = admin_client
        .create_topics(&topics, &AdminOptions::new())
        .await?;

    for topic_result in result {
        match topic_result {
            Ok(topic) => println!("✅ Created topic: {}", topic),
            Err((topic, error)) => eprintln!("❌ Failed to create topic {}: {}", topic, error),
        }
    }

    println!("🎉 Topics fixed successfully!");
    println!("💡 Now try running the producer again.");
    Ok(())
}
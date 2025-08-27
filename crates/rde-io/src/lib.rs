//! # RDE I/O - Data Source and Sink Implementations
//!
//! This crate provides the concrete implementations of data sources and sinks for the RDE pipeline.
//! It includes connectors for various data systems including Kafka, CSV files, Parquet files,
//! Apache Iceberg tables, and more.
//!
//! ## Features
//!
//! ### Data Sources
//! - **Kafka**: High-performance streaming data ingestion with JSON parsing and schema evolution
//! - **CSV**: Batch file processing with automatic schema inference
//! 
//! ### Data Sinks  
//! - **Apache Iceberg**: Production-grade data lake tables with ACID guarantees
//! - **Parquet**: High-performance columnar file format
//! - **Stdout**: Development and debugging output
//!
//! ### Advanced Features
//! - **Topic Mapping**: Direct Kafka-to-Iceberg streaming with schema evolution
//! - **Dynamic Schema Management**: Automatic schema inference and evolution
//! - **SQL Transformations**: Topic-specific data transformations using DataFusion
//!
//! ## Example Usage
//!
//! ```rust
//! use rde_io::source_kafka::KafkaPipelineSource;
//! use rde_io::sink_iceberg::IcebergSink;
//! use rde_core::{KafkaSourceSpec, IcebergSinkSpec};
//!
//! // Create Kafka source
//! let kafka_spec = KafkaSourceSpec {
//!     id: "kafka-source".to_string(),
//!     brokers: "localhost:9092".to_string(),
//!     group_id: "my-group".to_string(),
//!     topic: "input-topic".to_string(),
//!     schema: None,
//!     topic_mapping: None,
//! };
//!
//! let source = KafkaPipelineSource::new(kafka_spec);
//! ```

#![allow(unused)]

/// Parquet file sink implementation
pub mod sink_parquet;

/// Standard output sink for debugging
pub mod sink_stdout;

/// Apache Iceberg table sink implementation  
pub mod sink_iceberg;

/// CSV file source implementation
pub mod source_csv;

/// Apache Kafka source implementation
pub mod source_kafka;

/// Topic-to-table mapping management
pub mod topic_mapping;

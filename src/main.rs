use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::process::exit;

use chrono::Utc;
use clap::{App, Arg};
use csv::{QuoteStyle, Writer, WriterBuilder};
use jsonschema::{Draft, JSONSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use log::{debug, error, warn};
use regex::Regex;

// Initialize env_logger to target stderr.
use env_logger;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Config {
    delimiter: Option<char>,
    quotechar: Option<char>,
    destination_path: Option<String>,
    fixed_headers: Option<HashMap<String, Vec<String>>>,
    validate: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SchemaMessage {
    #[serde(rename = "type")]
    message_type: String,
    stream: String,
    schema: Value,
    key_properties: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct RecordMessage {
    #[serde(rename = "type")]
    message_type: String,
    stream: String,
    record: Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct StateMessage {
    #[serde(rename = "type")]
    message_type: String,
    value: Value,
}

// Custom format validator for date-time that accepts both formats with and without timezone
fn validate_datetime(value: &str) -> bool {
    // Accept empty strings
    if value.is_empty() {
        return true;
    }
    // Regex for ISO 8601 date-time with optional timezone (accepts both +HH:MM and +HHMM formats)
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$").unwrap();
    re.is_match(value)
}

fn emit_state(state: &Option<Value>) {
    // The state message is printed to stdout.
    let state_value = state.clone().unwrap_or(json!({}));
    let line = serde_json::to_string(&state_value).unwrap();
    debug!("Emitting state {}", line); // This debug log goes to stderr.
    println!("{}", line);
    io::stdout().flush().unwrap();
}

fn persist_messages(
    delimiter: char,
    quotechar: char,
    messages: impl BufRead,
    destination_path: &str,
    fixed_headers: &Option<HashMap<String, Vec<String>>>,
    validate: bool,
) -> Option<Value> {
    let mut state = None;
    let mut schemas = HashMap::new();
    let mut validators: HashMap<String, JSONSchema> = HashMap::new();

    // Cache CSV writers and headers per stream
    let mut writers: HashMap<String, Writer<File>> = HashMap::new();
    let mut headers_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut record_counts: HashMap<String, u64> = HashMap::new();

    let now = Utc::now().format("%Y%m%dT%H%M%S").to_string();
    let dest_path = Path::new(destination_path);

    for line in messages.lines() {
        let message = match line {
            Ok(msg) => msg,
            Err(e) => {
                error!("Error reading line: {}", e);
                continue;
            }
        };

        let message_value: Value = match serde_json::from_str(&message) {
            Ok(v) => v,
            Err(e) => {
                error!("Unable to parse: {}\nError: {}", message, e);
                continue;
            }
        };

        let message_obj = match message_value.as_object() {
            Some(obj) => obj,
            None => {
                error!("Message is not a valid JSON object");
                continue;
            }
        };

        let message_type = match message_obj.get("type") {
            Some(Value::String(t)) => t,
            _ => {
                error!("Message has no type field");
                continue;
            }
        };

        match message_type.as_str() {
            "RECORD" => {
                let record_message: RecordMessage = match serde_json::from_value(message_value) {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Failed to parse RECORD message: {}", e);
                        continue;
                    }
                };

                // Clean stream name
                let stream = record_message.stream.replace("/", "_");

                if !schemas.contains_key(&stream) {
                    error!(
                        "A record for stream {} was encountered before a corresponding schema",
                        stream
                    );
                    continue;
                }

                if validate {
                    if let Some(validator) = validators.get(&stream) {
                        if let Err(errors) = validator.validate(&record_message.record) {
                            for error in errors {
                                error!("Validation error: {}", error);
                            }
                            continue;
                        }
                    }
                }

                // Get (or create) the CSV writer for this stream.
                if !writers.contains_key(&stream) {
                    let filename = format!("{}-{}.csv", stream, now);
                    let file_path = dest_path.join(&filename);
                    let file_exists = file_path.exists()
                        && file_path.metadata().map(|m| m.len() > 0).unwrap_or(false);

                    let stream_headers: Vec<String> = if let Some(fixed_map) = fixed_headers {
                        if let Some(fixed) = fixed_map.get(&stream) {
                            fixed.clone()
                        } else if let Value::Object(ref obj) = record_message.record {
                            obj.keys().cloned().collect()
                        } else {
                            error!("Record is not a valid JSON object");
                            continue;
                        }
                    } else if let Value::Object(ref obj) = record_message.record {
                        obj.keys().cloned().collect()
                    } else {
                        error!("Record is not a valid JSON object");
                        continue;
                    };

                    headers_map.insert(stream.clone(), stream_headers.clone());

                    let file = match OpenOptions::new()
                        .write(true)
                        .create(true)
                        .append(true)
                        .open(&file_path)
                    {
                        Ok(f) => f,
                        Err(e) => {
                            error!("Failed to open file {}: {}", file_path.display(), e);
                            continue;
                        }
                    };

                    let mut csv_writer = WriterBuilder::new()
                        .delimiter(delimiter as u8)
                        .quote_style(QuoteStyle::Necessary)
                        .quote(quotechar as u8)
                        .from_writer(file);

                    if !file_exists {
                        if let Err(e) = csv_writer.write_record(&stream_headers) {
                            error!("Failed to write headers: {}", e);
                            continue;
                        }
                    }
                    writers.insert(stream.clone(), csv_writer);
                }

                if let Some(writer) = writers.get_mut(&stream) {
                    let stream_headers = headers_map.get(&stream).unwrap();
                    let mut row = Vec::new();
                    if let Value::Object(ref obj) = record_message.record {
                        for header in stream_headers {
                            let value_str = match obj.get(header) {
                                Some(Value::String(s)) => s.clone(),
                                Some(Value::Null) | None => String::new(),
                                Some(v) => v.to_string().replace("\\u0000", "\\n"),
                            };
                            row.push(value_str);
                        }
                        if let Err(e) = writer.write_record(&row) {
                            error!("Failed to write record: {}", e);
                            continue;
                        }
                    } else {
                        error!("Record is not a valid JSON object");
                        continue;
                    }

                    *record_counts.entry(stream.clone()).or_insert(0) += 1;

                    // Write job metrics after each record
                    let metrics_path = dest_path.join("job_metrics.json");
                    let mut metrics = HashMap::new();
                    let mut record_count_value = Map::new();
                    for (stream, count) in &record_counts {
                        record_count_value.insert(stream.clone(), json!(count));
                    }
                    metrics.insert("recordCount".to_string(), Value::Object(record_count_value));
                    if let Ok(file) = File::create(&metrics_path) {
                        if let Err(e) = serde_json::to_writer(file, &metrics) {
                            error!("Failed to write job metrics: {}", e);
                        }
                    } else {
                        error!("Failed to create job metrics file");
                    }
                }
            }
            "STATE" => {
                let state_message: StateMessage = match serde_json::from_value(message_value) {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Failed to parse STATE message: {}", e);
                        continue;
                    }
                };
                state = Some(state_message.value);
            }
            "SCHEMA" => {
                let schema_message: SchemaMessage = match serde_json::from_value(message_value) {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Failed to parse SCHEMA message: {}", e);
                        continue;
                    }
                };

                let stream = schema_message.stream.replace("/", "_");
                schemas.insert(stream.clone(), schema_message.schema.clone());

                if validate {
                    let compiled = match JSONSchema::options()
                        .with_draft(Draft::Draft4)
                        .with_format("date-time", validate_datetime)
                        .compile(&schema_message.schema)
                    {
                        Ok(schema) => schema,
                        Err(e) => {
                            error!("Failed to compile schema for stream {}: {}", stream, e);
                            continue;
                        }
                    };
                    validators.insert(stream, compiled);
                }
            }
            _ => {
                warn!(
                    "Unknown message type {} in message {}",
                    message_type, message
                );
            }
        }
    }

    // Flush all CSV writers
    for (_stream, writer) in writers.iter_mut() {
        if let Err(e) = writer.flush() {
            error!("Failed to flush writer: {}", e);
        }
    }

    state
}

fn main() {
    // Initialize env_logger so that logs are sent to stderr (terminal).
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .target(env_logger::Target::Stderr)
        .init();

    let matches = App::new("target-csv")
        .version(env!("CARGO_PKG_VERSION", "0.1.0"))
        .about("Singer target that writes to CSV files")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Config file")
                .takes_value(true),
        )
        .get_matches();

    let config_path = matches.value_of("config");
    let config: Config = if let Some(config_file) = config_path {
        match File::open(config_file) {
            Ok(file) => match serde_json::from_reader(file) {
                Ok(config) => config,
                Err(e) => {
                    error!("Failed to parse config file: {}", e);
                    exit(1);
                }
            },
            Err(e) => {
                error!("Failed to open config file: {}", e);
                exit(1);
            }
        }
    } else {
        Config {
            delimiter: None,
            quotechar: None,
            destination_path: None,
            fixed_headers: None,
            validate: None,
        }
    };

    let stdin = io::stdin();
    let input_messages = BufReader::new(stdin.lock());

    let delimiter = config.delimiter.unwrap_or(',');
    let quotechar = config.quotechar.unwrap_or('"');
    let destination_path = config.destination_path.as_deref().unwrap_or("");
    let fixed_headers = &config.fixed_headers;
    let validate = config.validate.unwrap_or(true);

    let state = persist_messages(
        delimiter,
        quotechar,
        input_messages,
        destination_path,
        fixed_headers,
        validate,
    );

    emit_state(&state);
    debug!("Exiting normally");
}

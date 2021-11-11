use dashmap::DashMap;
use log::{error, info};
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Error};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::ops::AddAssign;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc};
use std::thread;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Log {
    #[serde(rename = "type")]
    log_type: String,
    // the rest can be omitted
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct LogRegister {
    pub counter: u32,
    pub num_of_bytes: u64,
}

impl LogRegister {
    fn new(num_of_bytes: u64) -> Self {
        Self {
            counter: 1,
            num_of_bytes,
        }
    }

    fn zero() -> Self {
        Self {
            counter: 0,
            num_of_bytes: 0,
        }
    }
}

impl AddAssign for LogRegister {
    fn add_assign(&mut self, rhs: Self) {
        *self = Self {
            counter: self.counter + rhs.counter,
            num_of_bytes: self.num_of_bytes + rhs.num_of_bytes,
        }
    }
}

/// Multi thread parser with concurrent hash map.
pub fn multi_thread_parser_dashmap(
    num_of_thread: u8,
    input_file: &str,
) -> Arc<DashMap<String, LogRegister>> {
    let num_of_thread = num_of_thread as u64;

    let file = File::open(input_file).expect("Can't open file");
    let size = file.metadata().expect("Can't read file metadata").len();
    info!("File size in bytes: {}", size);

    // calculate number of bytes to be parsed by every thread
    let bytes_portion = size / num_of_thread;

    // use concurrent HashMap
    let log_register: DashMap<String, LogRegister> = DashMap::new();
    let log_register_arc = Arc::new(log_register);
    // keep started thread in vector
    let mut threads = vec![];
    for idx in 0..num_of_thread {
        let start_idx = idx * bytes_portion;

        let log_reg = log_register_arc.clone();

        // fail fast if can't open the file - stop the program
        let file = File::open(input_file).expect("Can't open file");
        let handle = thread::spawn(move || {
            let log_hasher = |bytes_read: u64, log_type: String| {
                let mut value = log_reg.entry(log_type).or_insert(LogRegister::zero());
                value.add_assign(LogRegister::new(bytes_read));
            };
            partially_read_file(start_idx, bytes_portion, file, log_hasher)
        });

        threads.push(handle);
    }

    for thread in threads {
        thread
            .join()
            .expect("Couldn't join on the associated thread");
    }

    log_register_arc
}

/// Read part of the file from index until number of bytes consumed.
fn partially_read_file<F>(start_idx: u64, num_of_bytes: u64, file: File, log_handler: F)
    where
        F: Fn(u64, String) -> (),
{
    let mut buffered = BufReader::new(file);
    // go to position where we need to start consuming
    buffered
        .seek(SeekFrom::Start(start_idx))
        .expect(&format!("Can't seek to position: {}", start_idx));

    let mut line = String::new();
    if start_idx > 0 {
        // move cursor to the beginning of the next line (only if we are in the middle of the file)
        buffered.read_line(&mut line).expect("Unexpected I/O error");
    }
    let offset = line.len() as u64;
    line.clear();

    // we need to take into account how many bytes we moved to find the next line
    let mut total_bytes_read = offset;
    // panic if we face I/O error - we can't recover
    while buffered.read_line(&mut line).expect("Unexpected I/O error") > 0 {
        let bytes_read = line.len() as u64;
        let log_result: Result<Log, Error> = serde_json::from_str(line.as_str());
        match log_result {
            Ok(log) => {
                log_handler(bytes_read, log.log_type);
            }
            Err(err) => {
                error!("Problem to parse line: [{}]. Error: [{}]", line, err);
            }
        }

        // add bytes read in line (error as well to be correctly sum at the end)
        total_bytes_read += bytes_read;

        // clear to reuse the buffer
        line.clear();

        if total_bytes_read > num_of_bytes {
            break;
        }
    }
}

/// Multi thread parser with channels.
pub fn multi_thread_parser_channel(
    num_of_thread: u8,
    input_file: &str,
) -> HashMap<String, LogRegister> {
    let num_of_thread = num_of_thread as u64;

    let file = File::open(input_file).expect("Can't open file");
    let size = file.metadata().expect("Can't read file metadata").len();
    info!("File size in bytes: {}", size);

    // calculate number of bytes to be parsed by every thread
    let bytes_portion = size / num_of_thread;

    // channels for communication between parsing threads and main consumer to update hashmap
    let (tx, rx): (
        Sender<(String, LogRegister)>,
        Receiver<(String, LogRegister)>,
    ) = mpsc::channel();

    // keep started thread in vector
    let mut threads = vec![];
    for idx in 0..num_of_thread {
        let start_idx = idx * bytes_portion;

        let tx = tx.clone();

        // fail fast if can't open the file - stop the program
        let file = File::open(input_file).expect("Can't open file");
        let handle = thread::spawn(move || {
            let log_hasher = |bytes_read: u64, log_type: String| {
                if tx.send((log_type, LogRegister::new(bytes_read))).is_err() {
                    error!("Can't send via channel");
                }
            };
            partially_read_file(start_idx, bytes_portion, file, log_hasher)
        });

        threads.push(handle);
    }
    drop(tx);

    let mut log_register: HashMap<String, LogRegister> = HashMap::new();
    for (log_type, log_value) in rx {
        let value = log_register.entry(log_type).or_insert(LogRegister::zero());
        value.add_assign(log_value);
    }

    log_register
}

/// Single thread parser.
pub fn single_thread_parser(input_file: &str) -> HashMap<String, LogRegister> {
    let mut log_register: HashMap<String, LogRegister> = HashMap::new();

    // fail fast - panic if can't open the file
    let file = File::open(input_file).expect("Can't open file");
    let mut buffered = BufReader::new(file);

    let mut line = String::new();
    while buffered.read_line(&mut line).expect("Unexpected I/O error") > 0 {
        let bytes_read = line.len();
        let log_result: Result<Log, Error> = serde_json::from_str(line.as_str());
        match log_result {
            Ok(log) => {
                let value = log_register
                    .entry(log.log_type)
                    .or_insert(LogRegister::zero());
                value.add_assign(LogRegister::new(bytes_read as u64));
            }
            Err(err) => {
                error!("Problem to parse line: [{}]. Error: [{}]", line, err);
            }
        }

        // clear to reuse the buffer
        line.clear();
    }

    log_register
}

/// Use it to generate sample file.
pub fn prepare_sample_file(
    num_of_lines: u32,
    num_of_log_types: u32,
    max_msg_size: u32,
    file: &str,
) -> HashMap<String, LogRegister> {
    let mut log_register: HashMap<String, LogRegister> = HashMap::new();

    let mut file = File::create(file).expect("Can't create sample file");

    let mut rng = rand::thread_rng();
    for _ in 0..num_of_lines {
        let log_type = rng.gen_range(0, num_of_log_types).to_string();

        let id = Uuid::new_v4();

        let msg_size = rng.gen_range(1, max_msg_size + 1);
        let msg: String = rng
            .sample_iter(&Alphanumeric)
            .take(msg_size as usize)
            .collect();

        let log = json!({"type" : log_type.clone(), "id" : id, "message" : msg});
        let log = log.to_string();
        let log_as_bytes = log.as_bytes();

        file.write(log_as_bytes).unwrap();
        file.write(b"\n").unwrap();

        let num_of_bytes: u64 = log_as_bytes.len() as u64 + 1;

        // register inserted logs
        let value = log_register.entry(log_type).or_insert(LogRegister::zero());
        value.add_assign(LogRegister::new(num_of_bytes));
    }

    file.flush().unwrap();

    log_register
}

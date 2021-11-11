use clap::{App, Arg};
use env_logger::Env;
use log_parser::parser;
use std::time::Instant;

#[macro_use]
extern crate log;

fn main() {
    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "info")
        .write_style_or("MY_LOG_STYLE", "always");
    env_logger::init_from_env(env);

    let matches = App::new("log-parser")
        .version("0.1.0")
        .author("Piotr Babel <piotr.babel@gmail.com>")
        .about("log parser")
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .required(true)
                .help("Input file path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("num-of-threads")
                .short("t")
                .long("num-of-threads")
                .required(true)
                .help("Number of thread used for execution")
                .takes_value(true),
        )
        .get_matches();

    info!("Reading args...");

    // args are required so we can unwrap
    let input = matches.value_of("input").unwrap();
    let num_of_threads: u8 = matches
        .value_of("num-of-threads")
        .unwrap_or("1")
        .parse()
        .unwrap();

    if num_of_threads <= 0 {
        panic!("Number of threads should be greater than 0")
    }

    info!("Parsing...");

    let now = Instant::now();

    // parser::prepare_sample_file(10000, 120, 100);
    // parser::single_thread_parser(input);
    // parser::multi_thread_parser_channel(num_of_threads, input);
    let result = parser::multi_thread_parser_dashmap(num_of_threads, input);
    result.iter().for_each(|elem| {
        info!(
            "log_type: {}, counter: {}, number_of_bytes: {}",
            elem.key(),
            elem.counter,
            elem.num_of_bytes
        );
    });

    info!("Parsed in: {} sec", now.elapsed().as_secs());
}

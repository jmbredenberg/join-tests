extern crate clap;
extern crate join_tests;

use clap::{Arg, App};
use join_tests::test_queries_from_file;
use std::path::Path;

fn main() {
    let matches = App::new("Join planning strategy tests")
                          .version("0.1")
                          .arg(Arg::with_name("INPUT")
                               .help("Sets the input file to use")
                               .required(true)
                               .index(1))
                          .arg(Arg::with_name("LABEL")
                               .help("Label to pass to test_queries_from_file")
                               .takes_value(true)
                               .short("l")
                               .long("label"))
                          .get_matches();

    let file = Path::new(matches.value_of("INPUT").unwrap());
    let label = matches.value_of("LABEL").unwrap_or("undefined");

    test_queries_from_file(file, label).expect("Testing queries failed!");
}

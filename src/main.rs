extern crate clap;
extern crate join_tests;

use clap::{Arg, App};
use join_tests::Optimizations;
use join_tests::test_queries_from_file;
use std::path::Path;
use std::fs::File;
use std::process::Command;



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
                        .arg(Arg::with_name("OUTPUT")
                                .help("Output file for graphviz")
                                .takes_value(true)
                                .short("f")
                                .long("output"))
                        .arg(Arg::with_name("OVERLAP")
                                .help("Whether to attempt to reuse overlap with previous queries")
                                .short("o")
                                .long("overlap"))
                        .arg(Arg::with_name("PERMUTATIONS")
                                .help("Whether to try all permutations of joined tables when looking for overlap")
                                .short("p")
                                .long("permutations"))
                        .arg(Arg::with_name("SORTED")
                                .help("Whether to sort names of joined tables to aid in overlap")
                                .short("s")
                                .long("sorted"))
                        .arg(Arg::with_name("NONPREFIX")
                                .help("Whether to look for overlap in a non-topological way")
                                .short("n")
                                .long("nonprefix"))
                        .arg(Arg::with_name("MEGAJOIN")
                                .help("Whether to outer-join everything to maximize overlap")
                                .short("m")
                                .long("megajoin"))
                        .get_matches();

    let file = Path::new(matches.value_of("INPUT").unwrap());
    let label = matches.value_of("LABEL").unwrap_or("undefined");
    let out_name = matches.value_of("OUTPUT").unwrap_or("graph");
    let graph_out_name = format!("{}.pdf", out_name);
    let output_file = Path::new(out_name);
    let graph_file = Path::new(&graph_out_name);

    let overlap = matches.is_present("OVERLAP");
    let permutations = matches.is_present("PERMUTATIONS");
    let sorted_names = matches.is_present("SORTED");
    let nonprefix = matches.is_present("NONPREFIX");
    let megajoin = matches.is_present("MEGAJOIN");
    let opts = Optimizations{overlap, permutations, sorted_names, nonprefix, megajoin};

    test_queries_from_file(file, label, opts, Some(output_file)).expect("Testing queries failed!");
    let output = Command::new("dot")
                         .arg("-Tpdf")
                         .stdin(File::open(output_file).unwrap())
                         .stdout(File::create(graph_file).unwrap())
                         .spawn();
}

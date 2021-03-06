extern crate nom_sql;


use self::graph::parse_queries;

use std::fs::File;
use std::io::Read;
use std::path::Path;

mod graph;
mod graphviz;
mod join;


#[derive(Clone)]
pub struct Optimizations {
    pub overlap: bool,
    pub permutations: bool,
    pub sorted_names: bool,
    pub nonprefix: bool,
    pub megajoin: bool
}

impl Optimizations {  // default constructor
    fn new() -> Self {
        Optimizations{
            overlap: true,
            permutations: true,
            sorted_names: false,
            nonprefix: false,
            megajoin: false}
    }
}


pub fn test_queries_from_file(f: &Path, name: &str, opts: Optimizations, outf: Option<&Path>) -> Result<i32, i32> {
    let mut f = File::open(f).unwrap();
    let mut s = String::new();

    // Load queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<String> = s
        .lines()
        .filter(|l| {
            !l.is_empty() && !l.starts_with("#") && !l.starts_with("--") && !l.starts_with("/*")
        })
        .map(|l| {
            if l.starts_with("VIEW") {
                let v: Vec<&str> = l.splitn(2, ":").collect();
                format!("CREATE VIEW {} AS{}", &v[0][5..], v[1])
            }
            else if l.starts_with("QUERY") {
                let v: Vec<&str> = l.splitn(2, ":").collect();
                format!("CREATE VIEW {} AS{}", &v[0][6..], v[1])
            }
            else {
                String::from(l)
            }
        })
        .map(|l| {
            if !(l.ends_with("\n") || l.ends_with(";")) {
                l + "\n"
            } else {
                l
            }
        }).collect();
    println!("Loaded {} {} queries", lines.len(), name);

    // Try parsing them all
    let (ok, err) = parse_queries(lines, opts, outf);

    println!("Parsing failed: {} queries", err);
    println!("Parsed successfully: {} queries", ok);

    if err > 0 {
        return Err(err);
    }
    Ok(ok)
}


#[test]
fn tpcw_test_queries() {
    assert!(test_queries_from_file(Path::new("tests/tpc-w-queries.txt"), "TPC-W", Optimizations::new(), None).is_ok());
}

#[test]
fn test_lobsters_schema() {
    assert!(test_queries_from_file(Path::new("tests/lobsters-schema.txt"), "TPC-W", Optimizations::new(), None).is_ok());
}

#[test]
fn test_long_join() {
    assert!(test_queries_from_file(Path::new("tests/long-join.txt"), "TPC-W", Optimizations::new(), None).is_ok());
}

#[test]
fn test_combo_join() {
    assert!(test_queries_from_file(Path::new("tests/combo-join.txt"), "TPC-W", Optimizations::new(), None).is_ok());
}

extern crate nom_sql;

use nom_sql::SqlQuery;
use nom_sql::{SelectStatement, FieldDefinitionExpression};




pub struct Column {
    pub name: String,
}

pub struct TestNode {
    pub data: TestNodeData,
    pub columns: Vec<Column>,
    pub ancestors: Vec<TestNode>,
    pub children: Vec<TestNode>,
}

pub enum TestNodeData {
    /// over column, group_by columns
    /*Aggregation {
        on: Column,
        group_by: Vec<Column>,
        kind: AggregationKind,
    },*/
    /// column specifications, keys (non-compound)
    Base {
        column_specs: Vec<(Column, Option<usize>)>,
        keys: Vec<Column>,
    },
    /// over column, group_by columns
    /*Extremum {
        on: Column,
        group_by: Vec<Column>,
        kind: ExtremumKind,
    },*/
    /// filter conditions (one for each parent column)
    /*Filter {
        conditions: Vec<Option<FilterCondition>>,
    },*/
    /// over column, separator
    /*GroupConcat {
        on: Column,
        separator: String,
    },*/
    /// no extra info required
    /*Identity,*/
    /// left node, right node, on left columns, on right columns, emit columns
    InnerJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// on left column, on right column, emit columns
    LeftJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// on left column, on right column, emit columns
    OuterJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// emit columns
    /*Project {
        emit: Vec<Column>,
        arithmetic: Vec<(String, ArithmeticExpression)>,
        literals: Vec<(String, DataType)>,
    },*/
    /// emit columns
    Union {
        emit: Vec<Vec<Column>>,
    },
    /// order function, group columns, k
    /*TopK {
        order: Option<Vec<(Column, OrderType)>>,
        group_by: Vec<Column>,
        k: usize,
        offset: usize,
    },*/
    // Get the distinct element sorted by a specific column
    Distinct {
        group_by: Vec<Column>,
    },
    /// reuse another node
    Reuse,
    /// leaf (reader) node, keys
    Leaf {
        keys: Vec<Column>,
    },
}

pub fn parse_queries(queries: Vec<String>) -> (i32, i32) {
    let mut parsed_ok = Vec::new();
    let mut parsed_err = 0;
    for query in queries.iter() {
        //println!("Trying to parse '{}': ", &query);
        match nom_sql::parser::parse_query(&query) {
            Ok(q) => {
                //println!("ok");
                parsed_ok.push(query);
                match q {
                    SqlQuery::Select(ref select) => make_graph(select),
                    SqlQuery::Insert(ref insert) => (),
                    SqlQuery::CreateTable(ref create) => (),
                    SqlQuery::CreateView(ref create) => (),
                    SqlQuery::Delete(ref delete) => (),
                    SqlQuery::DropTable(ref drop) => (),
                    SqlQuery::Update(ref update) => (),
                    SqlQuery::Set(ref set) => (),
                    _ => unimplemented!(),
                }
            }
            Err(_) => {
                //println!("failed");
                parsed_err += 1;
            }
        }
    }
    
    (parsed_ok.len() as i32, parsed_err)
}

pub fn make_graph(s: &SelectStatement) -> () {
    println!("making graph for: {}", s);
    for field in s.fields.iter() {
        match field {
            FieldDefinitionExpression::All => println!("all: *"),
            FieldDefinitionExpression::AllInTable(ref table) => {
                println!("allintable: {}.*", table)
            }
            FieldDefinitionExpression::Value(ref val) => println!("value: {}", val),
            FieldDefinitionExpression::Col(ref col) => println!("col: {}", col),
            _ => unimplemented!(),
        }
    }
}

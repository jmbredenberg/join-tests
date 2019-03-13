extern crate nom_sql;

use nom_sql::SqlQuery;
use nom_sql::{SelectStatement, SelectSpecification, CreateTableStatement, CreateViewStatement,
    FieldDefinitionExpression};

use std::collections::HashMap;




pub struct Column {
    pub name: String,
}

pub struct TestNode {
    pub name: String,
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
        column_specs: Vec<Column>,
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
    UnimplementedNode,
}

pub fn parse_queries(queries: Vec<String>) -> (i32, i32) {
    let mut parsed_ok = Vec::new();
    let mut parsed_err = 0;

    let mut tables = HashMap::new();  // map <name:String, columns:Vec<String>>
    let mut graph = Vec::new(); //Vec<TestNode>
    /*schema.insert(
        "users".into(),
        vec!["id".into(), "name".into(), "age".into()],
    );*/

    for query in queries.iter() {
        //println!("Trying to parse '{}': ", &query);
        match nom_sql::parser::parse_query(&query) {
            Ok(q) => {
                //println!("ok");
                parsed_ok.push(query);
                match q {
                    SqlQuery::Select(ref select) => {
                        make_select(select, &tables, &mut graph);
                    },
                    SqlQuery::Insert(ref insert) => (),
                    SqlQuery::CreateTable(ref create) => make_table(create, &mut tables),
                    SqlQuery::CreateView(ref create) => make_view(create, &tables, &mut graph),
                    SqlQuery::Delete(ref delete) => (),
                    SqlQuery::DropTable(ref drop) => (),
                    SqlQuery::Update(ref update) => (),
                    SqlQuery::Set(ref set) => (),
                    _ => unimplemented!(),
                }
            }
            Err(_) => {
                println!("failed to parse '{}'", &query);
                parsed_err += 1;
            }
        }
    }

    (parsed_ok.len() as i32, parsed_err)
}

pub fn make_table(s: &CreateTableStatement, tables: &mut HashMap<String, Vec<String>>) -> () {
    let t: String = s.table.name.clone();
    let fields = s.fields.clone()
                  .into_iter()
                  .map(|column_spec| column_spec.column.name.clone())
                  .collect();
    tables.insert(t, fields);
    //println!("tables: {:?}", tables);
}

pub fn make_select(s: &SelectStatement, tables: &HashMap<String, Vec<String>>, graph: &mut Vec<TestNode>) -> TestNode {
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
    TestNode {
        name: "unimplemented".to_string(),
        data: TestNodeData::UnimplementedNode,
        columns: Vec::new(),
        ancestors: Vec::new(),
        children: Vec::new(),
    }  // TODO return something legit
}

pub fn make_view(s: &CreateViewStatement, tables: &HashMap<String, Vec<String>>, graph: &mut Vec<TestNode>) -> () {
    println!("making view for: {}", s);
    match *(s.clone().definition) {
        SelectSpecification::Compound(css) => unimplemented!(),
        SelectSpecification::Simple(ss) => {
            let select_node = make_select(&ss, tables, graph);
            graph.push(TestNode {
                name: s.name.clone(),
                data: TestNodeData::Leaf {
                    keys: Vec::new(),  // TODO what should this be? also columns
                },
                columns: Vec::new(),
                ancestors: vec![select_node],
                children: Vec::new(),
            })
        }
    }
}

extern crate nom_sql;

use nom_sql::SqlQuery;
use nom_sql::{SelectStatement, SelectSpecification, CreateTableStatement, CreateViewStatement,
    FieldDefinitionExpression, JoinRightSide};

use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;




pub struct Column {
    pub name: String,
}

pub struct TestNode {
    pub name: String,
    pub data: TestNodeData,
    pub columns: Vec<Column>,
    pub ancestors: Vec<TestNodeRef>,
    pub children: Vec<TestNodeRef>,
}
pub type TestNodeRef = Rc<RefCell<TestNode>>;

pub enum TestNodeData {
    /// over column, group_by columns
    /*Aggregation {
        on: Column,
        group_by: Vec<Column>,
        kind: AggregationKind,
    },*/
    /// column specifications, keys (non-compound)
    Base {
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

impl TestNode {
    pub fn new(
        name: &str,
        data: TestNodeData,
        columns: Vec<Column>,
        ancestors: Vec<TestNodeRef>,
        children: Vec<TestNodeRef>,
    ) -> TestNodeRef {
        let mn = TestNode {
            name: String::from(name),
            data: data,
            columns: columns,
            ancestors: ancestors.clone(),
            children: children.clone(),
        };

        let rc_mn = Rc::new(RefCell::new(mn));

        // register as child on ancestors
        for ref ancestor in ancestors {
            ancestor.borrow_mut().add_child(rc_mn.clone());
        }

        rc_mn
    }

    pub fn add_child(&mut self, c: TestNodeRef) {
        self.children.push(c)
    }
}

pub fn get_empty_node() -> TestNodeRef {
    TestNode::new(
        "unimplemented",
        TestNodeData::UnimplementedNode,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

pub fn parse_queries(queries: Vec<String>) -> (i32, i32) {
    let mut parsed_ok = Vec::new();
    let mut parsed_err = 0;

    let mut graph = Vec::new(); //Vec<TestNodeRef>
    let mut tables = HashMap::new();  // map <name:String, basenode:TestNodeRef>

    for query in queries.iter() {
        //println!("Trying to parse '{}': ", &query);
        match nom_sql::parser::parse_query(&query) {
            Ok(q) => {
                //println!("ok");
                parsed_ok.push(query);
                match q {
                    SqlQuery::Select(ref select) => {
                        let node = make_select(select, &tables, &mut graph);
                        graph.push(node);
                    },
                    SqlQuery::Insert(ref _insert) => (),
                    SqlQuery::CreateTable(ref create) => {
                        let (t, base) = make_table(create);
                        tables.insert(t, base);
                    },
                    SqlQuery::CreateView(ref create) => {
                        let (t, view) = make_view(create, &tables, &mut graph);
                        tables.insert(t, view);
                    },
                    SqlQuery::Delete(ref _delete) => (),
                    SqlQuery::DropTable(ref _drop) => (),
                    SqlQuery::Update(ref _update) => (),
                    SqlQuery::Set(ref _set) => (),
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

pub fn make_table(s: &CreateTableStatement) -> (String, TestNodeRef) {
    let t: String = s.table.name.clone();
    let fields = s.fields.clone()
                  .into_iter()
                  .map(|column_spec| Column {
                      name: column_spec.column.name.clone()
                  })
                  .collect();
    let base = TestNode::new(
        &t.clone(),
        TestNodeData::Base {
            keys: Vec::new(),  // TODO get this from s.keys, looks like a pain
        },
        fields,
        Vec::new(),
        Vec::new(),
    );
    (t, base)
}

pub fn make_join(n1: &TestNodeRef, n2: &TestNodeRef) -> TestNodeRef {
    TestNode::new(
        "join",
        TestNodeData::InnerJoin {
            on_left: Vec::new(),  // TODO
            on_right: Vec::new(),  // TODO
            project: Vec::new(),  // TODO
        },
        Vec::new(),  // TODO columns
        Vec::new(), // TODO ancestors
        Vec::new(), // children
    )
}

pub fn make_select(s: &SelectStatement, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
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

    // join all entries of tables and joins together; TODO make this use on/where
    let mut previous_base: Option<&TestNodeRef> = None;
    let mut previous_join: Option<TestNodeRef> = None;
    let mut joinable_names: Vec<String> = s.tables.iter()
                                           .map(|t| t.name.clone())
                                           .collect();
    let mut more_joinables: Vec<String> = s.join.iter()
                                           .map(|j| {
                                               match j.right {
                                                   JoinRightSide::Table(ref t) => t.name.clone(),
                                                   _ => unimplemented!(),
                                               }
                                           })
                                           .collect();
    joinable_names.append(&mut more_joinables);
    for name in joinable_names {
        match previous_base {
            None => {
                println!("JOIN STEP 0");
                previous_base = tables.get(&name);
            }
            Some (base) => {
                // prev is either referencing a base table, or a join thereof
                println!("JOIN STEP 1/2: {}", name);
                let base_to_add = tables.get(&name).unwrap();
                match previous_join {
                    None => {
                        println!("JOIN STEP 1: making join: {} JOIN {}", base.borrow().name, base_to_add.borrow().name);
                        previous_join = Some(make_join(base, base_to_add));
                    }
                    Some (prev) => {
                        println!("JOIN STEP 2: making join: {} JOIN {}", prev.borrow().name, base_to_add.borrow().name);
                        previous_join = Some(make_join(&prev, base_to_add));
                    }
                }
                previous_base = Some(base_to_add);
            }
        }
    }
    for join in s.join.iter() {
        println!("join with op={}, right={}, constraint={}", join.operator, join.right, join.constraint);
    }


    get_empty_node()  // TODO return something legit
}

pub fn make_view(s: &CreateViewStatement, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> (String, TestNodeRef) {
    println!("making view for: {}", s);
    match *(s.clone().definition) {
        SelectSpecification::Compound(_) => unimplemented!(),
        SelectSpecification::Simple(ss) => {
            let select_node = make_select(&ss, tables, graph);
            let view = TestNode::new(
                &s.name.clone(),
                TestNodeData::Leaf {
                    keys: Vec::new(),  // TODO what should this be? also columns
                },
                Vec::new(),
                vec![select_node],
                Vec::new(),
            );
            (s.name.clone(), view)
        }
    }
}

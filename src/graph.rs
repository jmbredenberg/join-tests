extern crate nom_sql;

use nom_sql::SqlQuery;
use nom_sql::{SelectStatement, SelectSpecification, CreateTableStatement, CreateViewStatement,
    FieldDefinitionExpression, JoinRightSide};

use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::fmt;



#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
}

#[derive(Clone)]
pub struct TestNode {
    pub name: String,
    pub data: TestNodeData,
    pub columns: Vec<Column>,
    pub ancestors: Vec<TestNodeRef>,
    pub children: Vec<TestNodeRef>,
}
pub type TestNodeRef = Rc<RefCell<TestNode>>;

impl fmt::Debug for TestNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let column_strings: Vec<String> = self.columns.iter().map(|a| a.clone().name).collect();
        let ancestor_strings: Vec<String> = self.ancestors.iter().map(|a| a.borrow().clone().name).collect();
        let children_strings: Vec<String> = self.children.iter().map(|a| a.borrow().clone().name).collect();
        write!(f, "TestNode {{ name: {}, data: {:?}, columns: {:?}, ancestors: {:?}, children: {:?} }}",
                self.name,
                self.data,
                column_strings,
                ancestor_strings,
                children_strings
              )
    }
}

#[derive(Clone, Debug)]
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
    InnerJoin,
    LeftJoin,
    OuterJoin,
    /// emit columns
    Project,
    /// emit columns
    /*Union {
        emit: Vec<Vec<Column>>,
    },*/
    /// order function, group columns, k
    /*TopK {
        order: Option<Vec<(Column, OrderType)>>,
        group_by: Vec<Column>,
        k: usize,
        offset: usize,
    },*/
    // Get the distinct element sorted by a specific column
    /*Distinct {
        group_by: Vec<Column>,
    },*/
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
    let mut columns = n1.borrow().columns.clone();
    columns.append(&mut n2.borrow().columns.clone());
    TestNode::new(
        "join",
        TestNodeData::InnerJoin,
        columns,
        vec![n1.clone(), n2.clone()], // ancestors
        Vec::new(), // children
    )
}

pub fn make_all_joins(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>) -> TestNodeRef {
    // join all entries of tables and joins together; TODO make this use on/where
    let mut previous_base: Option<&TestNodeRef> = None;
    let mut previous_join: Option<TestNodeRef> = None;
    for name in joinable_names {
        match previous_base {
            None => previous_base = tables.get(&name),
            Some (base) => {
                // prev is either referencing a base table, or a join thereof
                let base_to_add = tables.get(&name).unwrap();
                match previous_join {
                    None => previous_join = Some(make_join(base, base_to_add)),
                    Some (prev) => previous_join = Some(make_join(&prev, base_to_add)),
                }
                previous_base = Some(base_to_add);
            },
        }
    }
    let join_result = match previous_join {
        Some(j) => j,
        None => match previous_base {
            Some(j) => j.clone(),
            None => unimplemented!(),
        },
    };
    join_result
}

pub fn make_select(s: &SelectStatement, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    // joins
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
    let join_result = make_all_joins(joinable_names, tables);

    // projection
    let mut columns_to_project = Vec::new();
    for field in s.fields.iter() {
        match field {
            FieldDefinitionExpression::All => {
                let mut columns = join_result.borrow().columns.clone();
                columns_to_project.append(&mut columns);
            }
            FieldDefinitionExpression::AllInTable(ref table) => {
                let mut columns = tables.get(table).unwrap().borrow().columns.clone();
                columns_to_project.append(&mut columns);
            }
            FieldDefinitionExpression::Value(ref val) => println!("value: {}", val), // TODO
            FieldDefinitionExpression::Col(ref col) => {
                let column = Column{ name: col.name.clone() };  // TODO this might include expressions
                columns_to_project.push(column);
            }
            _ => unimplemented!(),
        }
    }
    let projection = TestNode::new(
        "project",
        TestNodeData::Project,
        columns_to_project,
        vec![join_result], // ancestors
        Vec::new(), // children
    );
    projection
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
            println!("view: {:?}", view);
            (s.name.clone(), view)
        }
    }
}

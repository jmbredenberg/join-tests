extern crate nom_sql;
extern crate permutohedron;

use nom_sql::SqlQuery;
use nom_sql::{SelectStatement, SelectSpecification, CreateTableStatement, CreateViewStatement,
    FieldDefinitionExpression, JoinRightSide};
use graphviz::graphviz;

use self::permutohedron::Heap;

use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::fmt;


static JOIN_ALL : bool = false;
static TRY_PERMUTATIONS : bool = true;


#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
}

#[derive(Clone)]
pub struct TestNode {
    pub name: String,
    pub index: usize,
    pub data: TestNodeData,
    pub columns: Vec<Column>,
    pub ancestors: Vec<TestNodeRef>,
    pub children: Vec<TestNodeRef>,
}
pub type TestNodeRef = Rc<RefCell<TestNode>>;

impl fmt::Debug for TestNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        /*let column_strings: Vec<String> = self.columns.iter().map(|a| a.clone().name).collect();
        let ancestor_strings: Vec<String> = self.ancestors.iter().map(|a| a.borrow().clone().name).collect();
        let children_strings: Vec<String> = self.children.iter().map(|a| a.borrow().clone().name).collect();
        write!(f, "TestNode {{ name: {}, data: {:?}, columns: {:?}, ancestors: {:?}, children: {:?} }}",
                self.name,
                self.data,
                column_strings,
                ancestor_strings,
                children_strings
            )*/
        write!(f, "{}: {:?}", self.name, self.data)
    }
}

impl fmt::Display for TestNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        indented_print(self, 0, f)
    }
}

pub fn indented_print(node: &TestNode, indent:usize, f: &mut fmt::Formatter) -> fmt::Result {
    let spaces = (0..indent*3).map(|_| " ").collect::<String>();
    write!(f, "{}-- {}:{:?}\n", spaces, node.name, node.data);
    for a in node.ancestors.iter() {
        indented_print(&a.borrow(), indent+1, f);
    }
    write!(f,"")
}

impl TestNode {
    pub fn describe(&self) -> String {
        let mut s = String::new();

        s.push_str(&format!(
            "[label=\"{}\"]\n",
            self.name
        )); // "⋈", "⋉", "π", "⋃" etc. from noria-server/dataflow/src/ops/<type>::description

        s
    }
}

impl PartialEq for TestNode {
    fn eq(&self, other: &TestNode) -> bool {
        self.index == other.index
    }
}




#[derive(Clone, Debug, PartialEq)]
pub enum TestNodeData {
    /// over column, group_by columns
    /*Aggregation {
        on: Column,
        group_by: Vec<Column>,
        kind: AggregationKind,
    },*/
    /// column specifications, keys (non-compound)
    Base,
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
    Leaf,
    UnimplementedNode,
}

impl TestNode {
    pub fn new(
        name: &str,
        index: usize,
        data: TestNodeData,
        columns: Vec<Column>,
        ancestors: Vec<TestNodeRef>,
        children: Vec<TestNodeRef>,
    ) -> TestNodeRef {
        let mn = TestNode {
            name: String::from(name),
            index: index,
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
        0,
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
                        make_select(select, &tables, &mut graph);
                    },
                    SqlQuery::Insert(ref _insert) => (),
                    SqlQuery::CreateTable(ref create) => {
                        make_table(create, &mut tables, &mut graph);
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
    let njoins = graph.iter()
                      .filter(|node| match node.borrow().data {
                          TestNodeData::InnerJoin => true,
                          TestNodeData::OuterJoin => true,
                          TestNodeData::LeftJoin => true,
                          _ => false,
                      })
                      .count();
    println!("NUM_NODES: {}\nNUM_JOINS: {}", graph.len(), njoins);
    println!("GRAPHVIZ:\n{}", graphviz(&graph));

    (parsed_ok.len() as i32, parsed_err)
}

pub fn make_table(s: &CreateTableStatement, tables: &mut HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> () {
    let t: String = s.table.name.clone();
    let fields = s.fields.clone()
                  .into_iter()
                  .map(|column_spec| Column {
                      name: column_spec.column.name.clone()
                  })
                  .collect();
    let base = TestNode::new(
        &t.clone(),
        graph.len(),
        TestNodeData::Base,
        fields,
        Vec::new(),
        Vec::new(),
    );
    graph.push(base.clone());
    tables.insert(t, base);
}

pub fn make_join(n1: &TestNodeRef, n2: &TestNodeRef, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    let mut columns = n1.borrow().columns.clone();
    columns.append(&mut n2.borrow().columns.clone());
    let node = TestNode::new(
        "join",
        graph.len(),
        TestNodeData::InnerJoin,
        columns,
        vec![n1.clone(), n2.clone()], // ancestors
        Vec::new(), // children
    );
    graph.push(node.clone());
    node
}

pub fn overlap_existing(n1: &TestNodeRef, n2: &TestNodeRef, graph: &mut Vec<TestNodeRef>) -> Option<TestNodeRef> {
    for node in graph {
        if node.borrow().data == TestNodeData::InnerJoin {
            let ancestors = node.borrow().ancestors.clone();
            if ancestors.len() != 2 {
                unimplemented!();
            }
            if ancestors.contains(n1) && ancestors.contains(n2) {
                return Some(node.clone());
            }
        }
    }
    return None
}

pub fn make_all_joins(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    // join all entries of tables and joins together; TODO make this use on/where
    let mut previous_base: Option<&TestNodeRef> = None;
    let mut previous_join: Option<TestNodeRef> = None;
    for name in joinable_names {
        match previous_base {
            None => previous_base = tables.get(&name),
            Some (base) => {
                // prev is either referencing a base table, or a join thereof
                let base_to_add = tables.get(&name).unwrap();
                // check whether we already have a join node in the graph between these nodes
                match previous_join {
                    None => {
                        match overlap_existing(base, base_to_add, graph) {
                            Some (overlap) => previous_join = Some(overlap),
                            None => previous_join = Some(make_join(base, base_to_add, graph)),
                        }
                    }
                    Some (prev) => {
                        match overlap_existing(&prev, base_to_add, graph) {
                            Some (overlap) => previous_join = Some(overlap),
                            None => previous_join = Some(make_join(&prev, base_to_add, graph)),
                        }
                    }
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

pub fn make_joins_with_permutations(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    let mut names = joinable_names.clone();
    let heap = Heap::new(&mut names);

    let mut best_order = joinable_names.clone();
    let mut best_overlap = 0;

    for name_order in heap {
        let mut overlap = 0;

        let mut name = name_order[0].clone();
        let mut prev_node = tables.get(&name).unwrap().clone();
        let mut next_node = prev_node.clone();
        for i in 1..name_order.len() {
            name = name_order[i].clone();
            next_node = tables.get(&name).unwrap().clone();
            match overlap_existing(&prev_node, &next_node, graph) {
                Some (overlap_node) => prev_node = overlap_node,
                None => break,
            }
            overlap += 1;
        }

        if overlap > best_overlap {
            best_overlap = overlap;
            best_order = name_order.clone();
        }
    }

    make_all_joins(best_order, tables, graph)
}

pub fn make_combined_joins(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    // join all entries of tables and joins together; TODO make this use on/where
    let empty_node = get_empty_node();
    let mut previous_base: Option<&TestNodeRef> = Some(&empty_node);
    let mut previous_join: Option<TestNodeRef> = None;

    let mut already_joined_names: Vec<String> = Vec::new();
    for node in graph.clone() {
        if node.borrow().data == TestNodeData::InnerJoin {
            let ancestors = node.borrow().ancestors.clone();
            if ancestors.len() != 2 {
                unimplemented!();
            }
            if ancestors[0].borrow().data == TestNodeData::Base {
                already_joined_names.push(ancestors[0].borrow().name.clone());
            }
            if ancestors[1].borrow().data == TestNodeData::Base {
                already_joined_names.push(ancestors[1].borrow().name.clone());
            }
            previous_join = Some(node.clone());
        }
    }

    if previous_join == None {
        previous_base = None;
    }

    let mut names: Vec<String> = Vec::new();
    for name in joinable_names {
        if !already_joined_names.contains(&name) && !names.contains(&name) {
            names.push(name);
        }
    }

    for name in names {
        match previous_base {
            None => previous_base = tables.get(&name),
            Some (base) => {
                // prev is either referencing a base table, or a join thereof
                let base_to_add = tables.get(&name).unwrap();
                // check whether we already have a join node in the graph between these nodes
                match previous_join {
                    None => previous_join = Some(make_join(base, base_to_add, graph)),
                    Some (prev) => previous_join = Some(make_join(&prev, base_to_add, graph)),
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
    println!("making select for: {}", s);
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
    let join_result = if JOIN_ALL {
        make_combined_joins(joinable_names, tables, graph)
    } else if TRY_PERMUTATIONS{
        make_joins_with_permutations(joinable_names, tables, graph)
    } else {
        make_all_joins(joinable_names, tables, graph)
    };

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
        graph.len(),
        TestNodeData::Project,
        columns_to_project,
        vec![join_result], // ancestors
        Vec::new(), // children
    );
    graph.push(projection.clone());
    projection
}

pub fn make_view(s: &CreateViewStatement, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>) -> (String, TestNodeRef) {
    match *(s.clone().definition) {
        SelectSpecification::Compound(_) => unimplemented!(),
        SelectSpecification::Simple(ss) => {
            let select_node = make_select(&ss, tables, graph);
            let view = TestNode::new(
                &s.name.clone(),
                graph.len(),
                TestNodeData::Leaf,
                Vec::new(),
                vec![select_node],
                Vec::new(),
            );
            //println!("view:\n{}", view.borrow());
            graph.push(view.clone());
            (s.name.clone(), view)
        }
    }
}

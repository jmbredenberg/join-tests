extern crate nom_sql;

use nom_sql::SqlQuery;
use nom_sql::{SelectStatement, SelectSpecification, CreateTableStatement, CreateViewStatement,
    FieldDefinitionExpression, JoinRightSide};
use graphviz::graphviz;
use join;
use Optimizations;

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


pub fn parse_queries(queries: Vec<String>, opts: Optimizations) -> (i32, i32) {
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
                        make_select(select, &tables, &mut graph, opts.clone());
                    },
                    SqlQuery::Insert(ref _insert) => (),
                    SqlQuery::CreateTable(ref create) => {
                        make_table(create, &mut tables, &mut graph);
                    },
                    SqlQuery::CreateView(ref create) => {
                        let (t, view) = make_view(create, &tables, &mut graph, opts.clone());
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

pub fn make_select(s: &SelectStatement, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> TestNodeRef {
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
    let join_result = if opts.megajoin {
        join::make_combined_joins(joinable_names, tables, graph)
    } else if opts.permutations {
        join::make_joins_with_permutations(joinable_names, tables, graph)
    } else {
        join::make_all_joins(joinable_names, tables, graph)
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

pub fn make_view(s: &CreateViewStatement, tables: &HashMap<String, TestNodeRef>, graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> (String, TestNodeRef) {
    match *(s.clone().definition) {
        SelectSpecification::Compound(_) => unimplemented!(),
        SelectSpecification::Simple(ss) => {
            let select_node = make_select(&ss, tables, graph, opts);
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

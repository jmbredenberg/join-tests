extern crate permutohedron;

use graph::{TestNodeRef, TestNode, TestNodeData, get_empty_node};
use Optimizations;

use self::permutohedron::Heap;
use std::collections::HashMap;
use std::cmp;



pub fn make_inner_join(n1: &TestNodeRef, n2: &TestNodeRef, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    let maxrows = cmp::min(n1.borrow().maxrows, n2.borrow().maxrows);  // assuming primary key for now
    let mut columns = n1.borrow().columns.clone();
    columns.append(&mut n2.borrow().columns.clone());
    let node = TestNode::new(
        "join",
        graph.len(),
        TestNodeData::InnerJoin,
        columns,
        vec![n1.clone(), n2.clone()], // ancestors
        Vec::new(), // children,
        maxrows,
    );
    graph.push(node.clone());
    node
}

pub fn make_outer_join(n1: &TestNodeRef, n2: &TestNodeRef, graph: &mut Vec<TestNodeRef>) -> TestNodeRef {
    let maxrows = n1.borrow().maxrows + n2.borrow().maxrows; // assuming primary key for now
    let mut columns = n1.borrow().columns.clone();
    columns.append(&mut n2.borrow().columns.clone());
    let node = TestNode::new(
        "outer join",
        graph.len(),
        TestNodeData::OuterJoin,
        columns,
        vec![n1.clone(), n2.clone()], // ancestors
        Vec::new(), // children
        maxrows,
    );
    graph.push(node.clone());
    node
}

pub fn overlap_existing(n1: &TestNodeRef, n2: &TestNodeRef, graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> Option<TestNodeRef> {
    if !opts.overlap {
        return None;
    }
    // check whether a join already exists of these nodes
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
    None
}

pub fn make_all_joins(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>,
                      graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> TestNodeRef {
    // join all entries of tables and joins together; TODO make this use on/where
    if joinable_names.len() == 0 {
        unimplemented!();
    }

    let mut name = joinable_names[0].clone();
    let mut prev_node = tables.get(&name).unwrap().clone();
    let mut next_node = prev_node.clone();
    for i in 1..joinable_names.len() {
        name = joinable_names[i].clone();
        next_node = tables.get(&name).unwrap().clone();
        match overlap_existing(&prev_node, &next_node, graph, opts.clone()) {
            Some (overlap_node) => prev_node = overlap_node,
            None => prev_node = make_inner_join(&prev_node, &next_node, graph),
        }
    }

    prev_node
}

pub fn get_all_ancestors(node: &TestNodeRef) -> Vec<String> {
    match node.borrow().data {
        TestNodeData::Base{..} => Vec::new(),
        _ => {
            let mut ancs: Vec<String> = node.borrow().ancestors.clone()
                                                     .into_iter()
                                                     .map(|anc| anc.borrow().name.clone())
                                                     .collect();
            for anc in node.borrow().ancestors.clone() {
                ancs.append(&mut get_all_ancestors(&anc));
            }

            ancs
        }
    }
}

pub fn all_acceptable(tables: &Vec<String>, joinable_names: &Vec<String>) -> bool {
    for t in tables {
        if !joinable_names.contains(&t) {
            return false;
        }
    }
    true
}

pub fn some_needed(tables: &Vec<String>, joinable_names: &Vec<String>, covered_tables: &Vec<String>) -> bool {
    let mut count = 0;
    for t in tables {
        if joinable_names.contains(&t) && !covered_tables.contains(&t) {
            count += 1;
        }
    }
    count >= 2
}

pub fn make_joins_nonprefix_overlap(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>,
                                    graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> TestNodeRef {
    let mut covered_tables: Vec<String> = Vec::new();
    let mut existing_joins: Vec<TestNodeRef> = Vec::new();
    for node in graph.clone() {
        if node.borrow().data == TestNodeData::InnerJoin {
            let mut tables = get_all_ancestors(&node);
            if all_acceptable(&tables, &joinable_names) && some_needed(&tables, &joinable_names, &covered_tables) {
                existing_joins.push(node);
                covered_tables.append(&mut tables);
            }
        }
    }
    if existing_joins.len() == 0 {
        return make_all_joins(joinable_names, tables, graph, opts);
    }
    let mut prev_node = existing_joins[0].clone();
    let mut next_node = prev_node.clone();
    for i in 1..existing_joins.len() {
        prev_node = make_inner_join(&prev_node, &existing_joins[i], graph);
    }
    for name in joinable_names {
        if covered_tables.contains(&name) {
            continue;
        }
        next_node = tables.get(&name).unwrap().clone();
        match overlap_existing(&prev_node, &next_node, graph, opts.clone()) {
            Some (overlap_node) => prev_node = overlap_node,
            None => prev_node = make_inner_join(&prev_node, &next_node, graph),
        }
    }

    prev_node
}

pub fn make_joins_with_permutations(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>,
                                    graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> TestNodeRef {
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
            match overlap_existing(&prev_node, &next_node, graph, opts.clone()) {
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

    make_all_joins(best_order, tables, graph, opts)
}

pub fn make_combined_joins(joinable_names: Vec<String>, tables: &HashMap<String, TestNodeRef>,
                           graph: &mut Vec<TestNodeRef>, opts: Optimizations) -> TestNodeRef {
    // join all entries of tables and joins together; TODO make this use on/where
    let empty_node = get_empty_node();
    let mut previous_base: Option<&TestNodeRef> = Some(&empty_node);
    let mut previous_join: Option<TestNodeRef> = None;

    let mut already_joined_names: Vec<String> = Vec::new();
    for node in graph.clone() {
        if node.borrow().data == TestNodeData::OuterJoin {
            let ancestors = node.borrow().ancestors.clone();
            if ancestors.len() != 2 {
                unimplemented!();
            }
            match ancestors[0].borrow().data{
                TestNodeData::Base {..} => {
                    already_joined_names.push(ancestors[0].borrow().name.clone());
                },
                _ => (),
            }
            match ancestors[1].borrow().data{
                TestNodeData::Base {..} => {
                    already_joined_names.push(ancestors[1].borrow().name.clone());
                },
                _ => (),
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
                    None => previous_join = Some(make_outer_join(base, base_to_add, graph)),
                    Some (prev) => previous_join = Some(make_outer_join(&prev, base_to_add, graph)),
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

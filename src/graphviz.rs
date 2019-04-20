extern crate nom_sql;



use graph::TestNodeRef;


pub fn graphviz(graph: &Vec<TestNodeRef>) -> String {
    let mut s = String::new();

    let indentln = |s: &mut String| s.push_str("    ");

    // header.
    s.push_str("digraph {{\n");

    // global formatting.
    indentln(&mut s);
    s.push_str("graph [ fontsize=24 fontcolor=\"#0C6fA9\", outputorder=edgesfirst ]\n");
    s.push_str("edge [ color=\"#0C6fA9\", style=bold ]\n");
    s.push_str("node [ color=\"#0C6fA9\", shape=box, style=\"rounded,bold\" ]\n");

    // node descriptions.
    for node in graph {
        indentln(&mut s);
        s.push_str(&format!("n{}", node.borrow().index));
        s.push_str(&node.borrow().describe());
    }

    // edges.
    for node in graph {
        for child in node.borrow().children.clone() {
            indentln(&mut s);
            s.push_str(&format!(
                "n{} -> n{} [ ]",
                node.borrow().index,
                child.borrow().index
            ));
            s.push_str("\n");
        }
    }

    // footer.
    s.push_str("}}");

    s
}

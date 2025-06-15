use std::sync::atomic::compiler_fence;
use indexmap::{IndexMap, IndexSet};
use indexmap::map::Entry;

#[derive(Debug)]
struct MiniPather {
    whoami: u16,
    nodes: IndexMap<u16, Vec<u16>>
}

#[derive(Debug)]
pub struct FastPather {
    whoami: u16,
    temp: IndexSet<u16>,
    nodes: IndexMap<u16, IndexSet<u16>>,
    // Mapping from node, to everyone who hears said node
    heard_by: IndexMap<u16, IndexSet<u16>>
}

impl FastPather {
    pub fn new(whoami: u16,)-> Self {
        Self {
            whoami,
            temp: Default::default(),
            nodes: Default::default(),
            heard_by: Default::default(),
        }
    }
    pub fn report_neighbors(&mut self, node: u16, hears_neighbors: impl IntoIterator<Item = u16>) {
        self.temp.clear();
        self.temp.extend(hears_neighbors.into_iter());

        match self.nodes.entry(node) {
            Entry::Occupied(mut o) => {

                for t in self.temp.iter() {
                    if !o.get().contains(t) {
                        self.heard_by.entry(*t).or_default().insert(node);
                    }
                }

                std::mem::swap(o.get_mut(), &mut self.temp);
                for t in self.temp.iter() {
                    if !o.get().contains(t) {
                        match self.heard_by.entry(*t) {
                            Entry::Occupied(mut e) => {
                                e.get_mut().swap_remove(&node);
                                if e.get().is_empty() {
                                    e.swap_remove();
                                }
                            }
                            Entry::Vacant(e) => {}
                        }
                    }
                }
            }
            Entry::Vacant(e) => {
                let t = e.insert(Default::default());
                t.extend(&self.temp);
                for t in self.temp.iter() {
                    self.heard_by.entry(*t).or_default().insert(node);
                }
            }
        }
    }
    pub fn who_can_hear(&self, node: u16) -> Vec<u16> {
        self.heard_by.get(&node).into_iter().flatten().copied().collect()
    }

    pub fn should_i_forward(&mut self, origin: u16, received_from: u16) -> bool {
        if origin == self.whoami || received_from == self.whoami {
            return false;
        }


        let empty = IndexSet::new();
        self.temp.clear();
        for check in [origin, received_from] {
            let hears = self.heard_by.get(&check).unwrap_or(&empty);
            self.temp.extend(hears);
            for hearer in hears.iter().copied().filter(|hearer| *hearer < self.whoami) {
                if let Some(cand) = self.heard_by.get(&hearer) {
                    self.temp.extend(cand);
                }
            }
        }

        for cur_node in self.nodes.keys() {

            let taken = self.temp.contains(cur_node);
            let hears = self.heard_by.get(&self.whoami).unwrap_or(&empty);
            let hears_us = hears.contains(cur_node);
            dbg!(taken, cur_node, hears, hears_us);
            if !taken && hears_us &&  *cur_node != origin && *cur_node != received_from {
                return true;
            }
        }

        false
    }
}

impl MiniPather {
    pub fn new(whoami: u16,)-> Self {
        Self {
            whoami,
            nodes: Default::default(),
        }
    }

    pub fn report_neighbors(&mut self, node: u16, hears_neighbors: impl IntoIterator<Item = u16>) {
        self.nodes.insert(node, hears_neighbors.into_iter().collect());
    }

    pub fn who_can_hear(&self, node: u16) -> Vec<u16> {
        let mut ret = vec![];
        for (other_node, other_hears) in &self.nodes {
            if other_hears.contains(&node) {
                ret.push(*other_node);
            }
        }
        ret
    }

    pub fn should_i_forward(&self, origin: u16, received_from: u16) -> bool {
        if origin == self.whoami || received_from == self.whoami {
            return false;
        }

        let mut recipients_solved = IndexSet::new();
        recipients_solved.insert(origin);
        recipients_solved.insert(received_from);
        for (other_forwarder, other_forwarder_hears) in self.nodes.iter().filter(|(x,_)|**x < self.whoami) {

            if other_forwarder_hears.contains(&origin) || other_forwarder_hears.contains(&received_from) {
                for (other_node, other_hears) in &self.nodes {
                    if other_hears.contains(&received_from) || other_hears.contains(other_forwarder) ||
                        other_hears.contains(&origin) || *other_node == origin || *other_node == received_from
                    {
                        println!("{} maybe not forwarding from {} (via {}) to {} because we think {} will", self.whoami, origin,received_from, other_node, other_forwarder);
                        recipients_solved.insert(*other_node);
                        break;
                    }
                }
            }
        }

        for (other_node, other_hears) in &self.nodes {
            if other_hears.contains(&self.whoami) && !recipients_solved.contains(other_node)
            {
                return true;
            } else {
                println!("Node {} decided {} hears {} (via {}) through other means", self.whoami, other_node, origin, received_from);
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexSet;
    use itertools::enumerate;
    use super::{FastPather, MiniPather};
    #[test]
    fn test() {
        let mut pather = MiniPather::new(1);
        let mut pather2 = FastPather::new(1);

        pather.report_neighbors(3, [1,2,5]);
        pather.report_neighbors(4, [1,2,3]);

        pather2.report_neighbors(3, [1,2,5]);
        pather2.report_neighbors(4, [1,2,3]);

        assert_eq!(pather.who_can_hear(2), vec![3,4]);
        assert_eq!(pather.should_i_forward(42, 5), true);

        assert_eq!(pather2.who_can_hear(2), vec![3,4]);
        assert_eq!(pather2.should_i_forward(42, 5), true);
    }

    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn verify_someone_always_forwards(node_neighbors: Vec<Vec<u16>>) {

        println!("-----------------------");

        let islands = islands(&node_neighbors);
        let mut pathers: Vec<MiniPather> = vec![];
        let mut all_nodes = IndexSet::new();
        let node_count = node_neighbors.len();
        for (node,neighbors) in node_neighbors.iter().enumerate(){
            all_nodes.insert(node as u16);
            let mut pather = MiniPather::new(node as u16);
            for i in 0..node_count {
                if i != node {
                    pather.report_neighbors(i as u16, node_neighbors[i].iter().copied());
                }
            }
            pathers.push(pather);
        }

        fn get_who_hears(node: u16, neighbors: &Vec<Vec<u16>>) -> impl Iterator<Item=u16> + use<'_> {
            neighbors.iter().enumerate().filter_map(move |(x_node, x_neighbor)| if x_neighbor.contains(&(node as u16)) { Some(x_node as u16) } else { None })
        }

        for src in 0..node_count {
            println!("--- Analyzing src {} ---", src);
            let mut front = IndexSet::new();
            let mut covered = IndexSet::new();
            let mut nodes_that_have_received_msg = IndexSet::new();
            front.extend(get_who_hears(src as u16, &node_neighbors).into_iter().map(|x|(x, src as u16)));
            while let Some((dest, received_from)) = front.pop() {
                nodes_that_have_received_msg.insert(dest);
                if !covered.insert((dest,received_from)) {
                    continue;
                }
                {
                    let node = &pathers[dest as usize];
                    if node.should_i_forward(src as u16, received_from) {
                        println!("Node {} decided to forward msg from {} via {}", dest, src, received_from);
                        for hearing_node in get_who_hears(dest, &node_neighbors) {
                            front.insert((hearing_node, dest));
                        }
                    } else {
                        println!("Node {} decided NOT to forward msg from {} via {}", dest, src, received_from);
                    }
                }
            }

            for (node_index, node) in pathers.iter().enumerate() {
                if node_index == src {
                    // We don't want re-delivery to the src
                    continue;
                }
                if islands[node_index] != islands[src] {
                    // Not expected to be forwarded correctly, no connection exists
                    continue;
                }
                let ok = nodes_that_have_received_msg.contains(&(node_index as u16));
                if !ok {
                    println!("Src: {}, reached {nodes_that_have_received_msg:?}, ok = {}, islands: {:?} (msg didn't reach {})", src, ok, islands, node_index);
                    std::process::abort();
                }
                assert!(ok);
            }
        }
    }
    fn neighborhood() -> impl Strategy<Value = Vec<Vec<u16>>> {
        let n = 10 as usize;
        proptest::collection::vec(proptest::collection::vec(any::<u16>().prop_map(move |x|x%(n as u16)), n..n+1),n..n+1)
    }


    proptest! {
        #[test]
        fn verify_someone_always_forwards_test(neighbor_reports in neighborhood()) {
    compile_error!("I think this works nicely. Implement and check how it works in visualizer. Then maybe optimize the algo (maybe just cache/memoize lookup table for each (origin/received_from pair) .")

            verify_someone_always_forwards(neighbor_reports);
        }
    }

    #[test]
    fn regression_verify_someone_always_forwards1() {
        let input = vec! [vec![0, 2, 0], vec![0, 1, 2], vec![0, 0, 1]];
        verify_someone_always_forwards(input);
    }

    fn islands(node_neighbors: &Vec<Vec<u16>>) -> Vec<u8> {

        println!("Input: {:?}", node_neighbors);
        let mut explored = IndexSet::new();
        fn explore(explored: &mut IndexSet<u16>, seed: u16, neighbors: &Vec<Vec<u16>>) -> BTreeSet<u16> {
            let mut front = IndexSet::new();
            let mut island = BTreeSet::new();
            front.insert(seed);
            while let Some(seed) = front.pop() {
                island.insert(seed);
                if explored.insert(seed) {
                    for neighbor in neighbors[seed as usize].iter() {
                        if neighbors[*neighbor as usize].contains(&seed) {
                            front.insert(*neighbor);
                        }
                    }
                }
            }
            island
        }
        let mut islands = BTreeSet::new();
        for seed in 0..node_neighbors.len() {
            let mut seed = seed as u16;
            if !explored.contains(&seed) {
                islands.insert(explore(&mut explored, seed, node_neighbors));
            }
        }

        let mut ret = vec![0; node_neighbors.len()];
        for (i,island_contents) in islands.iter().enumerate() {
            for island_inhabitant in island_contents {
                ret[*island_inhabitant as usize] = i.try_into().unwrap();
            }
        }
        ret
    }

    fn verify_fast_path_equals_mini_path(neighbor_reports: Vec<(u8,u8,u8, Vec<u8>)>) {
        let mut pather = MiniPather::new(1);
        let mut fast_pather = FastPather::new(1);
        for (node,query1, query2, hears) in neighbor_reports {
            let mut node = (node%10).into();
            if node == 1 {
                node = 2;
            }
            let query1 = (query1%10).into();
            let query2 = (query2%10).into();
            let hears: Vec<u16> = hears.iter().map(|x|(*x as u16)%10).collect();
            pather.report_neighbors(node, hears.clone());
            fast_pather.report_neighbors(node, hears);
            let mut h1 = pather.who_can_hear(node);
            let mut h2 = fast_pather.who_can_hear(node);
            h1.sort();
            h2.sort();
            assert_eq!(h1, h2);
            println!("Slow pather: {:?}", pather);
            println!("Fast pather: {:?}", fast_pather);
            assert_eq!(pather.should_i_forward(query1, query2), fast_pather.should_i_forward(query1, query2));
        }

    }

    #[test]
    fn verify_islands() {
        assert_eq!(
            islands(&vec![
                vec![0,1],
                vec![0,1],
            ]),
            vec![0,0]);

        assert_eq!(
            islands(&vec![
                vec![0],
                vec![1],
            ]),
            vec![0,1]);

        assert_eq!(
            islands(&vec![
                vec![0,1],
                vec![1],
            ]),
            vec![0,1]);


        assert_eq!(
            islands(&vec![
                vec![0,1],
                vec![1,2],
                vec![1,2],
            ]),
            vec![0,1,1]);

        assert_eq!(
            islands(&vec![
                vec![],
                vec![2],
                vec![1],
            ]),
            vec![0,1,1]);
        assert_eq!(
            islands(&vec![
                vec![1],
                vec![0,2],
                vec![1,3],
                vec![2,4],
                vec![3],
            ]),
            vec![0,0,0,0,0]);
        assert_eq!(
            islands(&vec![
                vec![1],
                vec![0,2],
                vec![1,3],
                vec![2],
                vec![4],
            ]),
            vec![0,0,0,0,1]);
        assert_eq!(
            islands(&vec![
                vec![],
                vec![],
                vec![],
                vec![],
                vec![],
            ]),
            vec![0,1,2,3,4]);
    }

    proptest! {
        #[test]
        fn fast_path_equals_mini_path(neighbor_reports: Vec<(u8, u8 ,u8, Vec<u8>)>) {
            verify_fast_path_equals_mini_path(neighbor_reports);
        }
    }

    #[test]
    fn regression_fast_path_equals_mini_path1() {
        let neighbor_reports = vec![
            (
                0,
                34,
                14,
                vec![
                    151,
                ],
            ),
        ];
        println!("INput: {:?}", neighbor_reports);
        verify_fast_path_equals_mini_path(neighbor_reports);
    }
    #[test]
    fn regression_fast_path_equals_mini_path2() {
        let neighbor_reports = vec![
            (
                1,
                2,
                2,
                vec![
                    191,
                    192,
                ],
            ),

        ];
        println!("INput: {:?}", neighbor_reports);
        verify_fast_path_equals_mini_path(neighbor_reports);
    }
    #[test]
    fn regression_fast_path_equals_mini_path3() {
        let neighbor_reports = vec![
            (
                5,
                2,
                2,
                vec![
                    191,
                    192,
                ],
            ),

        ];
        println!("INput: {:?}", neighbor_reports);
        verify_fast_path_equals_mini_path(neighbor_reports);
    }



}


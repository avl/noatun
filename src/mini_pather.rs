use indexmap::{IndexMap, IndexSet};

#[derive(Debug)]
pub struct MiniPather {
    whoami: u16,
    nodes: IndexMap<u16, Vec<u16>>
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

    pub fn report_own_neighbors(&mut self, hears_neighbors: impl IntoIterator<Item = u16>) {
        self.nodes.insert(self.whoami, hears_neighbors.into_iter().collect());
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
    fn ranking(&self, of: u16) -> (isize, u16) {
        let neighbors = self.nodes.get(&of).map(|x|x.len()).unwrap_or(0);
        (-(neighbors as isize), of)
    }

    pub fn should_i_forward(&self, origin: u16, received_from: u16) -> bool {
        if origin == self.whoami || received_from == self.whoami {
            return false;
        }

        let mut recipients_solved = IndexSet::new();
        recipients_solved.insert(origin);
        recipients_solved.insert(received_from);
        recipients_solved.extend(self.who_can_hear(origin));
        recipients_solved.extend(self.who_can_hear(received_from));
        println!("Node {} decided origin {}.{} reaches {:?} naturally", self.whoami, origin, received_from, recipients_solved);
        for (other_forwarder, other_forwarder_hears) in self.nodes.iter().filter(|(x,_)|
            self.ranking(**x)
                <
                self.ranking(self.whoami)
        ) {

            if other_forwarder_hears.contains(&origin) || other_forwarder_hears.contains(&received_from) {
                println!("Node {} will forward from {}.{}", *other_forwarder, origin, received_from);
                recipients_solved.extend(self.who_can_hear(*other_forwarder));
            }
        }

        for (other_node, other_hears) in &self.nodes {
            if !other_hears.contains(&self.whoami) {
                continue;
            }
            if !recipients_solved.contains(other_node)
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
    use super::{MiniPather};

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
                } else {
                    pather.report_own_neighbors(node_neighbors[i].iter().copied());
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
                println!(" == == Analyzing what {} does when it receives {}.{} == ==", dest, src, received_from);
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
                    println!("Input: {:#?}", node_neighbors);
                    println!("Src: {}, reached {nodes_that_have_received_msg:?}, ok = {}, islands: {:?} (msg didn't reach {})", src, ok, islands, node_index);
                    std::process::abort();
                }
                assert!(ok);
            }
        }
    }
    fn neighborhood() -> impl Strategy<Value = Vec<Vec<u16>>> {
        let n = 8 as usize;
        proptest::collection::vec(proptest::collection::vec(any::<u16>().prop_map(move |x|x%(n as u16)), n..n+1),n..n+1)
    }


    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100_000))]
        #[test]
        fn verify_someone_always_forwards_test(neighbor_reports in neighborhood()) {
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


}


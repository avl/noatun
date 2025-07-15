use std::sync::atomic::AtomicBool;
use indexmap::{IndexMap};
use indexmap::map::Entry;

mod known_good_mini_pather;

#[derive(Debug, Default)]
struct Peer {
    visited: AtomicBool,
    /// Each node that this node hears
    neighbors: Vec<u16>,
}

#[derive(Debug)]
pub struct MiniPather {
    whoami: u16,
    /// Mapping from node to each node it hears
    nodes: IndexMap<u16, Peer>,
    /// Mapping from node, to each node that hears it
    reverse: IndexMap<u16, Vec<u16>>,
    /// Map from `(origin, received_from) tuple to result of
    /// calling 'should_i_forward(origin, received_from)`
    memoization: IndexMap<(u16,u16), bool>,
}

impl MiniPather {
    /// Return our id
    pub fn my_id(&self) -> u16 {
        self.whoami
    }

    pub fn new(whoami: u16,)-> Self {
        Self {
            whoami,
            nodes: Default::default(),
            reverse: Default::default(),
            memoization: Default::default(),
        }
    }
    fn add_reverse(node: u16, reverse: &mut IndexMap<u16, Vec<u16>>, added: &[u16]) {
        for added in added {
            let temp = reverse.entry(*added).or_default();
            assert!(!temp.contains(&node));
            temp.push(node);
        }
    }
    fn del_reverse(node: u16, reverse: &mut IndexMap<u16, Vec<u16>>, deleted: &[u16]) {
        for deleted in deleted {
            match reverse.entry(*deleted) {
                Entry::Occupied(mut occ) => {
                    occ.get_mut().retain(|x| *x != node);
                    if occ.get().is_empty() {
                        occ.swap_remove();
                    }
                }
                Entry::Vacant(_vac) => {}
            }
        }
    }

    /// Report which neighbors 'node' can hear
    pub fn report_neighbors(&mut self, node: u16, hears_neighbors: impl Iterator<Item=u16>) {
        let mut hears_neighbors : Vec<u16> = hears_neighbors.collect();


        match self.nodes.entry(node) {
            Entry::Occupied(mut cur) => {
                for new in &hears_neighbors {
                    if !cur.get().neighbors.contains(new) {
                        Self::add_reverse(node, &mut self.reverse, &[*new]);
                        self.memoization.clear();
                        cur.get_mut().neighbors.push(*new);
                    }
                }
                cur.get_mut().neighbors.retain(|old|{
                    if !hears_neighbors.contains(old) {
                        Self::del_reverse(node, &mut self.reverse, &[*old]);
                        self.memoization.clear();
                        false
                    } else {
                        true
                    }
                });
            }
            Entry::Vacant(entry) => {
                hears_neighbors.sort_unstable();
                hears_neighbors.dedup();
                Self::add_reverse(node, &mut self.reverse, &hears_neighbors);
                self.memoization.clear();
                entry.insert(Peer {
                    visited: AtomicBool::new(false),
                    neighbors: hears_neighbors,
                });
            }
        }
    }

    /// Report which neighbors we ourselves can hear
    pub fn report_own_neighbors(&mut self, hears_neighbors: impl Iterator<Item=u16>) {
        self.report_neighbors(self.whoami, hears_neighbors);
    }

    /// Return everybody who can hear 'node'
    pub fn who_can_hear<'a>(&'a self, node: u16) -> impl Iterator<Item=u16> + use<'a> {
        self.reverse.get(&node).into_iter().flatten().copied()
    }

    fn ranking(&self, of: u16) -> (isize, u16) {
        let neighbors = self.nodes.get(&of).map(|x|x.neighbors.len()).unwrap_or(0);
        (-(neighbors as isize), of)
    }

    /// Returns the ordinal for ourselves, regarding retransmission requests. Some(0) means
    /// we should ask for retransmission immediately. Some(5) means there are 5 others
    /// that we believe will ask for retransmission. Some(1) means that one other is believed
    /// to do so. In general, we should wait longer the more there are before us that should
    /// retransmit. If retransmission is carried out, we should, of course, cancel our request,
    /// regardless of whether anyone else actually managed to get a request in.
    ///
    /// Returns None if we should *not* ask for retransmission
    pub fn should_i_ask_for_retransmission(&mut self, received_from: u16) -> Option<usize> {
        if received_from == self.whoami {
            return None;
        }
        let Some(neighbor ) = self.nodes.get(&received_from)
         else {
            // We *do* ask for retransmission even for nodes we've never actually established
            // contact with, but we do it with some delay to try and avoid storms when connectivity
            // is bad.
            return Some(2 + self.whoami as usize %10);
        };

        if neighbor.neighbors.is_empty() {
            return Some(2 + self.whoami as usize %10);
        }
        if !neighbor.neighbors.contains(&self.whoami) {
            return None;
        }

        let my_rank = self.ranking(self.whoami);
        let mut count = 0;
        //TODO: Simplify, use 'count' or something
        for (_other_forwarder, _other_forwarder_hears) in self.nodes.iter().filter(|(x,_)|
            **x != received_from &&
            neighbor.neighbors.contains(x) &&
                self.ranking(**x)
                    <
                    my_rank
        ) {
            //println!("{} is a better sender", other_forwarder);
            count += 1;
        }

        Some(count)
    }

    pub fn should_i_forward(&mut self, origin: u16, received_from: u16) -> bool {
        if origin == self.whoami || received_from == self.whoami {
            return false;
        }

        if let Some(memoized ) = self.memoization.get(&(origin, received_from)) {

            return *memoized;
        }

        for peer in self.nodes.values_mut() {
            if !peer.neighbors.contains(&self.whoami) {
                // Since it can't hear us, we can't help it.
                // We consider it taken care of for the purpose of deciding if to forward
                peer.visited = AtomicBool::new(true);
            }   else {
                peer.visited = AtomicBool::new(false);
            }
        }

        if let Some(temp) = self.nodes.get_mut(&origin) {
            temp.visited = AtomicBool::new(true);
        }
        if let Some(temp) = self.nodes.get_mut(&received_from) {
            temp.visited = AtomicBool::new(true);
        }
        for temp in self.reverse.get(&origin).into_iter().flatten().copied()/*self.who_can_hear inlined*/ {
            if let Some(temp) = self.nodes.get_mut(&temp) {
                temp.visited = AtomicBool::new(true);
            }
        }
        for temp in self.reverse.get(&received_from).into_iter().flatten().copied()/*self.who_can_hear inlined*/ {
            if let Some(temp) = self.nodes.get_mut(&temp) {
                temp.visited = AtomicBool::new(true);
            }
        }


        //println!("Node {} decided origin {}.{} reaches {:?} naturally", self.whoami, origin, received_from, recipients_solved);
        let my_rank = self.ranking(self.whoami);
        for (other_forwarder, other_forwarder_hears) in self.nodes.iter().filter(|(x,_)|
            **x != origin && **x != received_from &&
            self.ranking(**x)
                <
                my_rank
        ) {
            if other_forwarder_hears.neighbors.contains(&origin) || other_forwarder_hears.neighbors.contains(&received_from) {

                //recipients_solved.extend(self.who_can_hear(*other_forwarder));

                for temp in self.reverse.get(other_forwarder).into_iter().flatten().copied()/*self.who_can_hear inlined*/ {
                    if let Some(temp) = self.nodes.get(&temp) {
                        temp.visited.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        let all_visited = self.nodes.values().all(|x| x.visited.load(std::sync::atomic::Ordering::Relaxed));
        if self.memoization.len() > 1000 {
            let l = self.memoization.len() / 2;
            self.memoization.drain(0..l);
        }
        self.memoization.insert((origin, received_from), !all_visited);

        !all_visited
        /*

        for (other_node, other_hears) in &mut self.nodes {

            if !other_hears.neighbors.contains(&self.whoami) {
                // Since it can't hear us, we can't help it.
                // We consider it taken care of for the purpose of deciding if to forward
                other_hears.visited = AtomicBool::new(true);
                continue;
            }
            if !recipients_solved.contains(other_node)
            {
                let all_visited = self.nodes.values().all(|x| x.visited.load(std::sync::atomic::Ordering::Relaxed));
                assert!(!all_visited);
                return true;
            }
        }

        let all_visited = self.nodes.values().all(|x| x.visited.load(std::sync::atomic::Ordering::Relaxed));
        assert!(all_visited);
        false*/
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexSet;

    use super::{known_good_mini_pather, MiniPather};

    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn verify_someone_always_forwards(node_neighbors: Vec<Vec<u16>>) {

        //println!("-----------------------");

        let islands = islands(&node_neighbors);
        let mut pathers: Vec<MiniPather> = vec![];
        let mut all_nodes = IndexSet::new();
        let node_count = node_neighbors.len();
        // TODO: Just iterate over indices
        for (node, _neighbors) in node_neighbors.iter().enumerate(){
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
            //println!("--- Analyzing src {} ---", src);
            let mut front = IndexSet::new();
            let mut covered = IndexSet::new();
            let mut nodes_that_have_received_msg = IndexSet::new();
            front.extend(get_who_hears(src as u16, &node_neighbors).into_iter().map(|x|(x, src as u16)));
            while let Some((dest, received_from)) = front.pop() {
                nodes_that_have_received_msg.insert(dest);
                if !covered.insert((dest,received_from)) {
                    continue;
                }
                //println!(" == == Analyzing what {} does when it receives {}.{} == ==", dest, src, received_from);
                {
                    let node = &mut pathers[dest as usize];
                    if node.should_i_forward(src as u16, received_from) {
                        //println!("Node {} decided to forward msg from {} via {}", dest, src, received_from);

                        assert_eq!(node.should_i_forward(src as u16, received_from), true);
                        for hearing_node in get_who_hears(dest, &node_neighbors) {
                            front.insert((hearing_node, dest));
                        }
                    } else {
                        assert_eq!(node.should_i_forward(src as u16, received_from), false);
                        //println!("Node {} decided NOT to forward msg from {} via {}", dest, src, received_from);
                    }
                }
            }
            
            // TODO: Just iterate over indices
            for (node_index, _node) in pathers.iter().enumerate() {

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

        for src in 0..node_count {
            let mut num_in_same_island = 0;
            let mut some_ask = false;
            let mut some_ask0 = false;
            let mut have_same_island = 0;
            for dst in 0..node_count {
                if src == dst {
                    continue;
                }
                let ask = pathers[dst].should_i_ask_for_retransmission(src as u16);
                println!("{} -> {} Ask: {:?} (islands: {} {})",src,dst,ask, islands[src], islands[dst]);
                if islands[src] == islands[dst] {
                    have_same_island+=1;
                }
                num_in_same_island += 1;
                if ask.is_some() {
                    some_ask = true;
                }
                if ask == Some(0) {
                    println!("Set asome_ask0 = true");
                    some_ask0 = true;
                }
            }
            if have_same_island >= 2 {
                assert!(some_ask);
                assert!(some_ask0);
            }
        }

    }
    fn neighborhood() -> impl Strategy<Value = Vec<Vec<u16>>> {
        let n = 4 as usize;
        proptest::collection::vec(proptest::collection::vec(any::<u16>().prop_map(move |x|x%(n as u16)), n..n+1),n..n+1)
    }


    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]
        #[test]
        fn verify_someone_always_forwards_test(neighbor_reports in neighborhood()) {
            verify_someone_always_forwards(neighbor_reports);
        }

    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000))]
        #[test]
        fn minipather_equals_to_ref(node_neighbors in neighborhood()) {
            let node_count = node_neighbors.len();
            for (node, _neighbors) in node_neighbors.iter().enumerate(){
                let mut pather = MiniPather::new(node as u16);
                let mut ref_pather = known_good_mini_pather::MiniPather::new(node as u16);
                for i in 0..node_count {
                    if i != node {
                        pather.report_neighbors(i as u16, node_neighbors[i].iter().copied());
                        ref_pather.report_neighbors(i as u16, node_neighbors[i].iter().copied());
                    }
                }
                for i in 0..node_count {
                    for j in 0..node_count {
                        assert_eq!(pather.should_i_forward(i as u16, j as u16),
                            ref_pather.should_i_forward(i as u16, j as u16));

                        assert_eq!(pather.should_i_forward(i as u16, j as u16),
                            ref_pather.should_i_forward(i as u16, j as u16));
                    }
                }
            }
        }

    }

    #[test]
    fn regression_verify_someone_always_forwards1() {
        let input = vec! [vec![0, 2, 0], vec![0, 1, 2], vec![0, 0, 1]];
        verify_someone_always_forwards(input);
    }
    #[test]
    fn regression_verify_someone_always_forwards2() {
        let input = vec![
            vec![
                0,
                0,
                0,
                1,
            ],
            vec![
                0,
                0,
                0,
                0,
            ],
            vec![
                0,
                0,
                3,
                0,
            ],
            vec![
                2,
                2,
                2,
                0,
            ]
        ];

        verify_someone_always_forwards(input);
    }

    fn islands(node_neighbors: &Vec<Vec<u16>>) -> Vec<u8> {

        //println!("Input: {:?}", node_neighbors);
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
            let seed = seed as u16;
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


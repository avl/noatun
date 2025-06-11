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
        compile_error!("Continue here! make more efficient?")

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

        for (candidate, candidate_hears) in self.nodes.iter().filter(|(x,_)|**x < self.whoami) {

            if candidate_hears.contains(&origin) || candidate_hears.contains(&received_from) {
                for (_other_node, other_hears) in &self.nodes {
                    if !other_hears.contains(&received_from) && other_hears.contains(candidate) &&
                        !other_hears.contains(&origin)
                    {
                        return false;
                    }
                }
            }
        }
        for (other_node, other_hears) in &self.nodes {
            if !other_hears.contains(&received_from) && other_hears.contains(&self.whoami) &&
                !other_hears.contains(&origin) && *other_node != origin && *other_node != received_from
            {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
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


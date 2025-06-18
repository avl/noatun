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
        let neighbors = self.nodes.get(&of).map(|x|{
            let mut x = x.clone();
            x.sort();
            x.dedup();
            x.len()
        }).unwrap_or(0);
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
        
        //println!("Node {} decided origin {}.{} reaches {:?} naturally", self.whoami, origin, received_from, recipients_solved);
        for (other_forwarder, other_forwarder_hears) in self.nodes.iter().filter(|(x,_)|
            self.ranking(**x)
                <
                self.ranking(self.whoami)
        ) {

            if other_forwarder_hears.contains(&origin) || other_forwarder_hears.contains(&received_from) {
//                    println!("Node {} will forward from {}.{}", *other_forwarder, origin, received_from);
                
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
//                    println!("Node {} decided {} hears {} (via {}) through other means", self.whoami, other_node, origin, received_from);
            }
        }

        false
    }
}


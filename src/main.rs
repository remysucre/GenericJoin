use indexmap::IndexMap;
use std::hash::Hash;
use std::rc::Rc;
use std::cell::RefCell;

// Any relation can extend some prefix
trait PrefixExtender<Prefix, Extension> {
    // Estimate how many extensions it would propose for a given prefix (a_1, ... a_i)
    fn count(&self, prefix: &Prefix) -> u64;
    // Propose specific extensions for a given prefix (a_1, ... a_i)
    fn propose(&self, prefix: &Prefix) -> Vec<Extension>;
    // Intersect its own extensions with other proposed extensions for a given prefix (a_1, ... a_i)
    fn intersect(&self, prefix: &Prefix, extensions: &mut Vec<Extension>);
}

// Given a number of relations (PrefixExtenders), compute extensions for each prefix
trait GenericJoinExt<P, E> {
    fn extend(&mut self, extenders: Vec<impl PrefixExtender<P, E>>) -> Vec<(P, Vec<E>)>;
}

impl<P: Eq + Hash + Clone, E> GenericJoinExt<P, E> for Vec<P> {
    fn extend(&mut self, extenders: Vec<impl PrefixExtender<P, E>>) -> Vec<(P, Vec<E>)> {
        // Map each prefix to the smallest extender
        let mut extend_by = IndexMap::new();
        for prefix in self.iter() {
            for extender in extenders.iter() {
                let count: u64 = extender.count(prefix);
                extend_by.entry(prefix)
                         .and_modify(|(c, e)| {
                             if count < *c {
                                 *c = count;
                                 *e = extender;
                             }
                         })
                         .or_insert((count, extender));
            }
        }

        let mut results: Vec<(P, Vec<E>)> = vec![];

        for extender in extenders.iter() {
            // Find prefixes it extends
            let nominations: Vec<_> = extend_by
                .iter()
                .filter_map(|(p, (_, e))| {
                    if *e as *const _ == extender as *const _ {Some(*p)} else {None}
                })
                .collect();

            for prefix in nominations.iter() {
                let mut extension = extender.propose(prefix);
                for other in extenders.iter().filter(|&x| x as *const _ != extender as *const _) {
                    other.intersect(prefix, &mut extension);
                }
                results.push(((*prefix).clone(), extension));
            }
        }

        results
    }
}

struct GraphFragment<E: Ord> {
    nodes: Vec<usize>,
    edges: Vec<E>,
}

impl<E: Ord> GraphFragment<E> {
    fn edges(&self, node: usize) -> &[E] {
        if node + 1 < self.nodes.len() {
            &self.edges[self.nodes[node]..self.nodes[node+1]]
        }
        else { &[] }
    }
}

//impl<P, E, L> PrefixExtender<P, E> for (Rc<RefCell<GraphFragment<E>>>, Box<L>)
//where E: Ord + Clone, L: Fn(&P)->u64 {
impl<P, E> PrefixExtender<P, E> for (Rc<RefCell<GraphFragment<E>>>, Box<dyn Fn(&P) -> u64>)
where E: Ord + Clone {
    // counting is just looking up the edges
    fn count(&self, prefix: &P) -> u64 {
        let &(ref graph, ref logic) = self;
        let node = logic(&prefix) as usize;
        graph.borrow().edges(node).len() as u64
    }

    // proposing is just reporting the slice back
    fn propose(&self, prefix: &P) -> Vec<E> {
        let &(ref graph, ref logic) = self;
        let node = logic(prefix) as usize;
        graph.borrow().edges(node).to_vec()
    }

    // intersection 'gallops' through a sorted list to find matches
    // what is "galloping", you ask? details coming in just a moment
    fn intersect(&self, prefix: &P, list: &mut Vec<E>) {
        let &(ref graph, ref logic) = self;
        let node = logic(prefix) as usize;
        let gb = graph.borrow();
        let mut slice = gb.edges(node);
        list.retain(move |value| {
            slice = gallop(slice, value); // skips past elements < value
            !slice.is_empty() && &slice[0] == value
        });
    }
}

pub fn gallop<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    if !slice.is_empty() && &slice[0] < value {
        let mut step = 1;
        while step < slice.len() && &slice[step] < value {
            slice = &slice[step..];
            step <<= 1;
        }

        step >>= 1;
        while step > 0 {
            if step < slice.len() && &slice[step] < value {
                slice = &slice[step..];
            }
            step >>= 1;
        }

        &slice[1..]   // this shouldn't explode... right?
    }
    else { slice }
}



fn main() {

    let triangle: GraphFragment<u64> = GraphFragment {nodes: vec![0, 2, 3], edges: vec![1, 2, 2]};

    let graph = Rc::new(RefCell::new(triangle));

    let mut v = vec![0, 1];

    let ext_b = vec![(graph.clone(), Box::new(|a: &u64| { *a }) as Box<dyn Fn(&u64) -> u64 + 'static>)];
    let pairs = GenericJoinExt::extend(&mut v, ext_b);
    let mut tups = vec![];
    for (p, es) in pairs {
        for e in es {
            tups.push((p, e));
            println!("{:?}", (p, e))
        }
    }

    let ext_c = vec![(graph.clone(), Box::new(|(a,_): &(u64, u64)| { *a }) as Box<dyn Fn(&(u64, u64)) -> u64 + 'static>),
                         (graph, Box::new(|(_,b): &(u64, u64)| { *b }) as Box<dyn Fn(&(u64, u64)) -> u64 + 'static>)];

    let triangles = GenericJoinExt::extend(&mut tups, ext_c);
    let mut ans = vec![];

    for ((a, b), cs) in triangles {
        for c in cs {
            ans.push((a, b, c));
        }
    }

    for a in ans {
        println!("{:?}", a);
    }
}

use std::cmp;

use crate::link::egress::load::credit::wait::{
    store::Nodes,
    tree::{Index, Node, Oldest},
};

enum Tilt {
    Left(Index),
    Level,
    Right(Index),
}

pub(super) struct Edit<'a, 'b>(&'a mut Nodes<'b>);

impl<'a, 'b> Edit<'a, 'b> {
    pub(super) const fn new(nodes: &'a mut Nodes<'b>) -> Self {
        Self(nodes)
    }

    fn left(&mut self, parent: Index, child: Option<Index>) {
        self.0.get_mut(parent).left = child;
        if let Some(child) = child {
            self.0.get_mut(child).parent = Some(parent);
        }
    }

    fn right(&mut self, parent: Index, child: Option<Index>) {
        self.0.get_mut(parent).right = child;
        if let Some(child) = child {
            self.0.get_mut(child).parent = Some(parent);
        }
    }

    fn splice(&mut self, parent: Index, index: Index, child: Option<Index>) {
        if self.0.get(parent).left == Some(index) {
            self.left(parent, child);
        } else {
            self.right(parent, child);
        }
    }

    fn update(&mut self, index: Index) {
        let node = self.0.get(index);
        let (left, right) = (node.left, node.right);
        let own = Some(Oldest {
            ticket: node.ticket,
            lane: index,
        });
        let oldest = Oldest::best(
            Oldest::best(Oldest::at(self.0, left), own),
            Oldest::at(self.0, right),
        );
        let height = cmp::max(Node::height(self.0, left), Node::height(self.0, right)) + 1;
        let node = self.0.get_mut(index);
        node.height = height;
        node.oldest = oldest;
    }

    fn tilt(&self, index: Index) -> Tilt {
        let node = self.0.get(index);
        let left = Node::height(self.0, node.left);
        let right = Node::height(self.0, node.right);
        if left > right + 1 {
            return node.left.map_or(Tilt::Level, Tilt::Left);
        }
        if right > left + 1 {
            return node.right.map_or(Tilt::Level, Tilt::Right);
        }
        Tilt::Level
    }

    fn rotate_left(&mut self, index: Index, pivot: Index) -> Index {
        let parent = self.0.get(index).parent;
        let middle = self.0.get(pivot).left;
        self.right(index, middle);
        self.left(pivot, Some(index));
        self.0.get_mut(pivot).parent = parent;
        self.update(index);
        self.update(pivot);
        pivot
    }

    fn rotate_right(&mut self, index: Index, pivot: Index) -> Index {
        let parent = self.0.get(index).parent;
        let middle = self.0.get(pivot).right;
        self.left(index, middle);
        self.right(pivot, Some(index));
        self.0.get_mut(pivot).parent = parent;
        self.update(index);
        self.update(pivot);
        pivot
    }

    fn rebalance(&mut self, index: Index) -> Index {
        self.update(index);
        match self.tilt(index) {
            Tilt::Left(mut pivot) => {
                let node = self.0.get(pivot);
                if let Some(child) = node.right
                    && Node::height(self.0, node.right) > Node::height(self.0, node.left)
                {
                    pivot = self.rotate_left(pivot, child);
                    self.left(index, Some(pivot));
                }
                self.rotate_right(index, pivot)
            }
            Tilt::Right(mut pivot) => {
                let node = self.0.get(pivot);
                if let Some(child) = node.left
                    && Node::height(self.0, node.left) > Node::height(self.0, node.right)
                {
                    pivot = self.rotate_right(pivot, child);
                    self.right(index, Some(pivot));
                }
                self.rotate_left(index, pivot)
            }
            Tilt::Level => index,
        }
    }

    pub(super) fn insert(&mut self, root: Option<Index>, index: Index) -> Index {
        let Some(root) = root else {
            return index;
        };
        if (self.0.get(index).resident, index) < (self.0.get(root).resident, root) {
            let left = self.insert(self.0.get(root).left, index);
            self.left(root, Some(left));
        } else {
            let right = self.insert(self.0.get(root).right, index);
            self.right(root, Some(right));
        }
        self.rebalance(root)
    }

    fn minimum(&self, mut root: Index) -> (Index, Option<Index>) {
        let mut parent = None;
        while let Some(left) = self.0.get(root).left {
            parent = Some(root);
            root = left;
        }
        (root, parent)
    }

    fn replace(&mut self, root: &mut Option<Index>, index: Index, child: Option<Index>) {
        let parent = self.0.get(index).parent;
        match parent {
            Some(parent) => self.splice(parent, index, child),
            None => {
                *root = child;
                if let Some(child) = child {
                    self.0.get_mut(child).parent = None;
                }
            }
        }
    }

    fn restore(&mut self, root: &mut Option<Index>, mut current: Option<Index>) {
        while let Some(index) = current {
            let parent = self.0.get(index).parent;
            let balanced = self.rebalance(index);
            match parent {
                Some(parent) => self.splice(parent, index, Some(balanced)),
                None => {
                    *root = Some(balanced);
                    self.0.get_mut(balanced).parent = None;
                }
            }
            current = parent;
        }
    }

    pub(super) fn remove(&mut self, mut root: Option<Index>, index: Index) -> Option<Index> {
        let node = self.0.get(index);
        let (left, right) = (node.left, node.right);
        match (left, right) {
            (None, child) | (child, None) => {
                let parent = self.0.get(index).parent;
                self.replace(&mut root, index, child);
                self.restore(&mut root, parent);
            }
            (Some(left), Some(right)) => {
                let (next, parent) = self.minimum(right);
                match parent {
                    None => {
                        self.replace(&mut root, index, Some(next));
                        self.left(next, Some(left));
                        self.restore(&mut root, Some(next));
                    }
                    Some(parent) => {
                        let child = self.0.get(next).right;
                        self.left(parent, child);
                        self.replace(&mut root, index, Some(next));
                        self.left(next, Some(left));
                        self.right(next, Some(right));
                        self.restore(&mut root, Some(parent));
                    }
                }
            }
        }
        root
    }
}

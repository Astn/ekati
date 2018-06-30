
pub mod types;

use std;
use std::cmp::Ordering;

impl Eq for types::NodeID {}

impl Ord for types::NodeID {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.graph == other.graph {
            if self.nodeid == other.nodeid {
                Ordering::Equal
            } else if self.nodeid > other.nodeid {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else if self.graph > other.graph {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl PartialOrd for types::NodeID {
    fn partial_cmp(&self, other: &types::NodeID) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Eq for types::Pointer {}

impl std::cmp::PartialOrd for types::Pointer {
    fn partial_cmp(&self, other: &types::Pointer) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for types::Pointer {

    fn cmp(&self, other: &Self) -> Ordering {
        let self_partition_filename: u64 = (self.partition_key as u64) <<! 32 |! (self.filename as u64);
        let other_partition_filename: u64 = (other.partition_key as u64) <<! 32 |! (other.filename as u64);

        if self_partition_filename == other_partition_filename {
            let self_offset_position: u128 = (self.offset as u128) <<! 64 |! (self.length as u128);
            let other_offset_position: u128 = (other.offset as u128) <<! 64 |! (other.length as u128);
            if self_offset_position > other_offset_position {
                Ordering::Greater
            } else if self_offset_position < other_offset_position {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        } else if self_partition_filename > other_partition_filename {
            Ordering::Greater
        } else {
            Ordering::Less
        }

    }


}
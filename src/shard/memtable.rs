use std::cell::RefCell;


const TABLE_SIZE: usize = 1024*1024*64;


pub enum ActiveSlot{
    TableA,
    TableB
}
pub enum OtherSlotState{
    Persisting,
    Ready
}

pub enum MemTableWriteError {
    Busy,
    FailedFlushingMemTableToDiskInBackground
}

// we use the pattern from video cards where they have multiple buffers,
// so they can be writing to one while they are sending the other over the wire.
// but in our case we will be sending one to disk.
pub struct MemTableData {
    active_slot: ActiveSlot,
    other_slot_state: OtherSlotState,
    table_a: RefCell<Vec<u8>>, //;1024*1024*64], // 64mib
    table_b: RefCell<Vec<u8>>,  // 64mib
}

impl MemTableData {


    fn new() -> MemTableData {
        let mtd = MemTableData {
            active_slot: ActiveSlot::TableA,
            other_slot_state: OtherSlotState::Ready,
            table_a: RefCell::new(vec![0;TABLE_SIZE]),
            table_b: RefCell::new( vec![0;TABLE_SIZE])
        };
        mtd
    }

    fn len(&self) -> usize {
        match self.active_slot {
            ActiveSlot::TableA => {
                self.table_a.borrow().len().to_owned()
            },
            ActiveSlot::TableB => {
                self.table_b.borrow().len().to_owned()
            }
        }
    }
    // we need to be able to continue writing to this object and have it flush when it needs to.
    // or at least return a value indicating you cannot write without flushing.
    fn try_write(&self, write: &Fn(&mut Vec<u8>) -> ()) -> Result<usize, MemTableWriteError> {
        match self.active_slot {
            ActiveSlot::TableA => {
                let mut bm = self.table_a.borrow_mut();
                let len_before = (*bm).len();
                {
                    write(&mut *bm);
                }
                let len_after = (*bm).len();
                if len_after > TABLE_SIZE {
                    // todo: swap tables, and start flushing this one to disk
                    // need to make sure the other table is ready to be swapped though.
                }
                Ok(len_after - len_before)
            },
            ActiveSlot::TableB => {
                let mut bm = self.table_b.borrow_mut();
                let len_before = (*bm).len();
                {
                    write(&mut *bm);
                }
                let len_after = (*bm).len();
                if len_after > TABLE_SIZE {
                    // todo: swap tables, and start flushing this one to disk
                    // need to make sure the other table is ready to be swapped though.
                }
                Ok(len_after - len_before)
            }
        }
    }
}
#![deny(
    clippy::pedantic,
    clippy::nursery,
    deprecated,
    intra_doc_link_resolution_failure
)]
#![forbid(unsafe_code)]
// While dev
#![allow(unused_variables, dead_code)]

use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use wasmer_runtime::Instance;

const QUEUE_STORAGE: u8 = b'q';
const QUEUE_INDEX: u8 = b'i';
const INDEX_FUNCTIONS: u8 = b'f';
const NAME_LOOKUPS: u8 = b'n';
const INDEX_REVISIONS: u8 = b'r';

const LOOKUP_QUEUE_FWD: u8 = b'Q';
const LOOKUP_INDEX_FWD: u8 = b'I';
const LOOKUP_FUNCTION_FWD: u8 = b'F';
const LOOKUP_QUEUE_REV: u8 = b'q';
const LOOKUP_INDEX_REV: u8 = b'i';
const LOOKUP_FUNCTION_REV: u8 = b'f';

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub enum Named {
    Queue,
    Index,
    Function,
}

impl Named {
    pub fn fwd(self) -> u8 {
        match self {
            Named::Queue => LOOKUP_QUEUE_FWD,
            Named::Index => LOOKUP_INDEX_FWD,
            Named::Function => LOOKUP_FUNCTION_FWD,
        }
    }

    pub fn rev(self) -> u8 {
        match self {
            Named::Queue => LOOKUP_QUEUE_REV,
            Named::Index => LOOKUP_INDEX_REV,
            Named::Function => LOOKUP_FUNCTION_REV,
        }
    }
}

#[derive(Clone)]
pub struct Db {
    db: Arc<sled::Db>,
}

impl From<sled::Db> for Db {
    fn from(db: sled::Db) -> Self {
        Arc::new(db).into()
    }
}

impl From<Arc<sled::Db>> for Db {
    fn from(db: Arc<sled::Db>) -> Self {
        Self { db }
    }
}

impl Db {
    fn queue_tree(&self, id: u64) -> sled::Result<sled::Tree> {
        let mut name = Vec::with_capacity(9);
        name.push(QUEUE_STORAGE);
        name.extend_from_slice(&id.to_le_bytes()[..]);
        self.db.open_tree(name)
    }

    fn index_tree(&self, id: u64, rev: u8, queue: u64, function: u64) -> sled::Result<sled::Tree> {
        let mut name = Vec::with_capacity(26);
        name.push(QUEUE_INDEX);
        name.extend_from_slice(&id.to_le_bytes()[..]);
        name.push(rev);
        name.extend_from_slice(&queue.to_le_bytes()[..]);
        name.extend_from_slice(&function.to_le_bytes()[..]);
        self.db.open_tree(name)
    }

    fn function_tree(&self) -> sled::Result<sled::Tree> {
        self.db.open_tree([INDEX_FUNCTIONS])
    }

    fn names_tree(&self) -> sled::Result<sled::Tree> {
        self.db.open_tree([NAME_LOOKUPS])
    }

    fn index_rev_tree(&self) -> sled::Result<sled::Tree> {
        self.db.open_tree([INDEX_REVISIONS])
    }

    pub fn name_for(&self, t: Named, id: u64) -> sled::Result<Option<String>> {
        let mut key = Vec::with_capacity(9);
        key.push(t.rev());
        key.extend_from_slice(&id.to_le_bytes()[..]);

        Ok(self
            .names_tree()?
            .get(key)?
            .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap()))
    }

    pub fn name_of(&self, t: Named, name: &str) -> sled::Result<Option<u64>> {
        let mut key = Vec::with_capacity(name.len() + 1);
        key.push(t.fwd());
        key.extend_from_slice(name.as_bytes());

        Ok(self
            .names_tree()?
            .get(key)?
            .map(|bytes| u64::from_le_bytes(bytes.as_ref().try_into().unwrap())))
    }

    pub fn name_a(&self, t: Named, name: &str) -> sled::Result<u64> {
        let mut key = Vec::with_capacity(name.len() + 1);
        key.push(t.fwd());
        key.extend_from_slice(name.as_bytes());

        let tree = self.names_tree()?;
        let new_id = self.db.generate_id()?;
        let new_bytes = &new_id.to_le_bytes()[..];

        Ok(
            if let Err(old_bytes) = tree.cas(&key, None as Option<&[u8]>, Some(new_bytes))? {
                u64::from_le_bytes(old_bytes.unwrap().as_ref().try_into().unwrap())
            } else {
                new_id
            },
        )
    }

    fn open_queue(&self, queue: &str) -> sled::Result<Queue> {
        let queue_id = self.name_a(Named::Queue, queue)?;
        let tree = self.queue_tree(queue_id)?;
        Ok(Queue {
            id: queue_id,
            db: self.db.clone(),
            tree,
        })
    }

    pub fn add_job(&self, queue: &str, job: &Job) -> sled::Result<u64> {
        let queue = self.open_queue(queue)?;
        let id = queue.add(job)?;

        for index in &queue.indexes()? {
            index.add(id, job)?;
        }

        Ok(id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Job {
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct Queue {
    id: u64,
    db: Arc<sled::Db>,
    tree: sled::Tree,
}

impl Queue {
    pub fn add(&self, job: &Job) -> sled::Result<u64> {
        let id = self.db.generate_id()?;
        self.tree
            .insert(id.to_le_bytes(), bincode::serialize(job).unwrap())?;
        Ok(id)
    }

    pub fn get(&self, id: u64) -> sled::Result<Option<Job>> {
        self.tree
            .get(id.to_le_bytes())
            .map(|e| e.as_ref().map(|v| bincode::deserialize(v).unwrap()))
    }

    pub fn del(&self, id: u64) -> sled::Result<()> {
        self.tree.remove(id.to_le_bytes()).map(|_| ())
    }

    pub fn indexes(&self) -> sled::Result<Vec<Index>> {
        let id_bytes = self.id.to_le_bytes();
        let mut is = Vec::new();

        for name in self.db.tree_names().iter().filter(|name| {
            name.len() == 26 && name.starts_with(&[QUEUE_INDEX]) && name[11..=17] == id_bytes
        }) {
            let _tree = self.db.open_tree(name)?;
            let index = Index::from_tree_name(name, self.db.clone())?;
            is.push(index);
        }

        // TODO: filter revisions

        Ok(is)
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum IndexMode {
    Hash = 1,    // SipHash: low collisions, good distribution, only == queries
    Ordered,     // no hashing, resultant bucketing, no collisions, poor distribution, range queries
    OrderedHash, // hashing only as discriminant, low collisions, medium distribution, range queries
}

impl Default for IndexMode {
    fn default() -> Self {
        IndexMode::OrderedHash
    }
}

#[derive(Clone)]
pub struct Index {
    id: u64,
    rev: u8,
    queue: u64,
    mode: IndexMode,
    function: Function,
    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
}

impl Index {
    /// Parses an index tree name and fetches the relevant structures.
    #[allow(clippy::needless_pass_by_value)]
    pub fn from_tree_name(_key: &[u8], _db: Arc<sled::Db>) -> sled::Result<Self> {
        // - parse name in (id: u64, rev: u8, queue: u64, mode: u8(enum), function: u64)
        // - retrieve tree from db
        // - retrieve function from db
        // - construct
        unimplemented!()
    }

    /// Indexes a job id at the position that the index's function puts the provided job.
    pub fn add(&self, job_id: u64, job: &Job) -> sled::Result<()> {
        let n = self.db.generate_id()?; // TODO: derive from job and index function
        self.tree
            .insert(n.to_le_bytes(), &job_id.to_le_bytes()[..])
            .map(|_| ())
    }

    /// Returns the nth job id from the top of the index, or the last if there's less than n, or
    /// None if there's nothing there.
    pub fn nth(&self, n: u64) -> sled::Result<Option<u64>> {
        unimplemented!()
    }

    pub fn first(&self) -> sled::Result<Option<u64>> {
        self.nth(1)
    }

    /// Removes the nth job id. See `nth()` for details.
    pub fn pop_nth(&self, n: u64) -> sled::Result<Option<u64>> {
        unimplemented!()
    }

    pub fn pop(&self) -> sled::Result<Option<u64>> {
        self.pop_nth(1)
    }
}

#[derive(Clone)]
pub struct Function {
    id: u64,
    instance: Arc<Mutex<Instance>>,
}

// function is instead a wasm module
// module required to have two exports:
// - constant: key_length (u8), which is the length of the returned key
// - keying function: key_factory (in_offset: i32, in_length: i32, out_offset: i32) -> i32
//      Takes in offset+length for an input byte string that contains
//      the input data in the module's memory, and an offset (length is
//      the key_length constant) to write the output byte string to.
//      The output byte string is always initialised and zeroed.
//      Needs to return 0 for success, >0 for standard errors, <0 for custom errors.

impl Function {
    pub fn new(id: u64, source: &[u8]) -> wasmer_runtime::error::Result<Self> {
        use wasmer_runtime::{func, imports, instantiate};

        let import = imports! {
            "env" => {
                "log" => func!(wasm::log),
            },
        };

        instantiate(source, &import).map(|i| Self {
            id,
            instance: Arc::new(Mutex::new(i)),
        })
    }
}

mod wasm {
    use std::cell::Cell;
    use wasmer_runtime::{Ctx, Memory};

    fn memory_bytes(memory: &Memory, offset: u32, length: u32) -> Vec<u8> {
        use std::convert::TryFrom;
        let start = usize::try_from(offset).unwrap();
        let end = start + usize::try_from(length).unwrap();
        memory.view()[start..end].iter().map(Cell::get).collect()
    }

    pub fn log(ctx: &mut Ctx, ptr: u32, len: u32) {
        println!(
            "{}",
            String::from_utf8_lossy(&memory_bytes(ctx.memory(0), ptr, len))
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

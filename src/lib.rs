#![deny(
    clippy::pedantic,
    clippy::nursery,
    deprecated,
    intra_doc_link_resolution_failure
)]
#![forbid(unsafe_code)]
// While dev
#![allow(unused_variables, dead_code)]

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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
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

    pub fn add_item(&self, queue: &str, item: &Item) -> sled::Result<u64> {
        let queue = self.open_queue(queue)?;
        let id = queue.add(item)?;

        for index in &queue.indexes()? {
            index.add(id, item)?;
        }

        Ok(id)
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Item(pub Vec<u8>);

impl std::ops::Deref for Item {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct Queue {
    id: u64,
    db: Arc<sled::Db>,
    tree: sled::Tree,
}

impl Queue {
    pub fn add(&self, item: &Item) -> sled::Result<u64> {
        let id = self.db.generate_id()?;
        self.tree.insert(id.to_le_bytes(), item as &[u8])?;
        Ok(id)
    }

    pub fn get(&self, id: u64) -> sled::Result<Option<Item>> {
        self.tree
            .get(id.to_le_bytes())
            .map(|e| e.as_ref().map(|v| Item(v.to_vec())))
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
    OrderedHash = 1, // hashing only as discriminant, low collisions, range queries
    Ordered, // no hashing, resultant bucketing, no collisions, range queries
    SipHash, // SipHash: low collisions, only == queries
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
    pub fn from_tree_name(key: &[u8], db: Arc<sled::Db>) -> sled::Result<Self> {
        // - parse name in (id: u64, rev: u8, queue: u64, mode: u8(enum), function: u64)
        assert_eq!(key[0], QUEUE_INDEX);
        let id = u64::from_le_bytes(key[1..=8].try_into().unwrap());
        let rev = key[9];
        let queue = u64::from_le_bytes(key[10..=17].try_into().unwrap());
        let mode: IndexMode = match key[18] {
            1 => IndexMode::OrderedHash,
            _ => panic!("Invalid mode"),
        };
        let function_id = u64::from_le_bytes(key[19..=26].try_into().unwrap());
        assert_eq!(key.len(), 27);

        // - retrieve tree from db
        let tree = db.open_tree(key)?;

        // - retrieve function from db
        let function_tree = db.open_tree([INDEX_FUNCTIONS])?;
        let function_source = function_tree.get(function_id.to_le_bytes())?.unwrap();
        let function = Function::new(function_id, &function_source).unwrap();

        // - construct
        Ok(Self {
            id,
            rev,
            queue,
            mode,
            function,
            db,
            tree: Arc::new(tree),
        })
    }

    /// Indexes a job id at the position that the index's function puts the provided job.
    pub fn add(&self, item_id: u64, job: &Item) -> sled::Result<()> {
        let n = self.db.generate_id()?; // TODO: derive from job and index function
        self.tree
            .insert(n.to_le_bytes(), &item_id.to_le_bytes()[..])
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
    key_length: u8,
    instance: Arc<Mutex<Instance>>,
}

impl Function {
    pub fn new(id: u64, source: &[u8]) -> wasmer_runtime::error::Result<Self> {
        use wasmer_runtime::{func, imports, instantiate, Export};

        let import = imports! {
            "env" => {
                "log" => func!(wasm::log),
            },
        };

        let instance = instantiate(source, &import)?;

        let key_length = instance
            .exports()
            .find_map(|(s, e)| match (s.as_str(), e) {
                ("key_length", Export::Global(g)) => Some(g),
                _ => None,
            })
            .expect("No key_length global export")
            .get()
            .to_u128()
            .try_into()
            .expect("Length is too large");

        if !instance.exports().any(|(name, _)| name == "key_factory") {
            panic!("No key_factory function export");
        }

        Ok(Self {
            id,
            key_length,
            instance: Arc::new(Mutex::new(instance)),
        })
    }

    pub fn call(&self, value: &[u8]) -> Result<Vec<u8>, wasmer_runtime::error::CallError> {
        use wasmer_runtime::Value;
        let mut instance = self.instance.lock().unwrap();
        let memory = instance.context_mut().memory(0);

        // input section written with input data
        let in_start = 0;
        let in_end = in_start + value.len();

        // 1..=32 zero bytes as padding

        // output section zeroed of size key_length
        let out_start = {
            let pad = in_end % 32;
            in_end + (32 - pad)
        };
        let out_end = out_start + self.key_length as usize;

        // TODO: figure out how (or if it's needed) to shrink memory after use
        for (byte, cell) in value
            .iter()
            .chain(std::iter::repeat(&0).take(out_end - in_end))
            .zip(memory.view()[in_start..in_end].iter())
        {
            cell.set(*byte);
        }

        let params: [i32; 3] = [
            in_start.try_into().expect("in_start too large"),
            in_end.try_into().expect("in_end too large"),
            out_start.try_into().expect("out_start too large"),
        ];

        let result = instance.call(
            "key_factory",
            &[params[0].into(), params[1].into(), params[2].into()],
        )?;

        let result = result.first().map_or(0, Value::to_u128);

        if result != 0 {
            panic!("Index function returned {}", result);
        }

        let memory = instance.context_mut().memory(0);
        let output = wasm::memory_bytes(
            memory,
            out_start.try_into().unwrap(),
            out_end.try_into().unwrap(),
        );

        Ok(output)
    }
}

mod wasm {
    use std::cell::Cell;
    use wasmer_runtime::{Ctx, Memory};

    pub fn memory_bytes(memory: &Memory, offset: u32, length: u32) -> Vec<u8> {
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

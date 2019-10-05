#![forbid(
    clippy::pedantic,
    clippy::nursery,
    deprecated,
    intra_doc_link_resolution_failure,
    unsafe_code,
//    missing_docs,
//    clippy::option_unwrap_used,
//    clippy::result_unwrap_used,
)]
// While dev
#![allow(unused_variables, dead_code, clippy::expect_fun_call)]

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
			Self::Queue => LOOKUP_QUEUE_FWD,
			Self::Index => LOOKUP_INDEX_FWD,
			Self::Function => LOOKUP_FUNCTION_FWD,
		}
	}

	pub fn rev(self) -> u8 {
		match self {
			Self::Queue => LOOKUP_QUEUE_REV,
			Self::Index => LOOKUP_INDEX_REV,
			Self::Function => LOOKUP_FUNCTION_REV,
		}
	}
}

#[cfg(test)]
pub(crate) fn temporary_sled_db() -> Arc<sled::Db> {
	Arc::new(sled::Db::start(sled::ConfigBuilder::new().temporary(true).build()).unwrap())
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
			index.insert(id, item)?;
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

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum IndexMode {
	OrderedHash = 1, // hashing only as discriminant, low collisions, range queries
}

impl Default for IndexMode {
	fn default() -> Self {
		Self::OrderedHash
	}
}

impl IndexMode {
	pub fn key(self, function: &Function, item: &Item) -> Vec<u8> {
		match self {
			Self::OrderedHash => self.ordered_hash_keying(function, item),
		}
	}

	fn ordered_hash_keying(self, function: &Function, item: &Item) -> Vec<u8> {
		use std::collections::hash_map::DefaultHasher;
		use std::hash::Hasher;

		let mut order = function.call(item).unwrap();
		let mut hasher = DefaultHasher::new();
		hasher.write(item);
		order.extend(&hasher.finish().to_le_bytes());
		order
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
		let function = Function::new(function_id, db.clone(), &function_source).unwrap();

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

	/// Computes an item’s key according to the keying mode and function.
	pub fn key(&self, item: &Item) -> Vec<u8> {
		self.mode.key(&self.function, item)
	}

	/// Inserts an item into the index.
	pub fn insert(&self, item_id: u64, item: &Item) -> sled::Result<()> {
		self.tree
			.insert(self.key(item), &item_id.to_le_bytes()[..])
			.map(|_| ())
	}
}

#[derive(Clone)]
pub struct Function {
	id: u64,
	db: Arc<sled::Db>,
	key_length: u8,
	instance: Arc<Mutex<Instance>>,
}

impl Function {
	/// Initialise a keying function from its source.
	///
	/// In practice, the "source" here is compiled WASM bytecode, not WAST any other kind of
	/// source. We do not include a compiler here beyond the WASM runtime.
	pub fn new(id: u64, db: Arc<sled::Db>, source: &[u8]) -> wasmer_runtime::error::Result<Self> {
		let (instance, key_length) = Self::make_instance_from_db(id, &db)?;

		Ok(Self {
			id,
			db,
			key_length,
			instance,
		})
	}

	#[cfg(test)]
	pub(crate) fn new_from_source(id: u64, source: &[u8]) -> wasmer_runtime::error::Result<Self> {
		let (instance, key_length) = Self::make_instance(id, source)?;

		Ok(Self {
			id,
			db: temporary_sled_db(),
			key_length,
			instance,
		})
	}

	/// Re-initialise the function (to free its memory).
	///
	/// If the item inserted in the instance's memory exceeds one page (64KB) minus some overhead,
	/// the only way to reclaim that memory is to destroy the instance and make it anew.
	pub fn reinstantiate(&mut self) -> wasmer_runtime::error::Result<()> {
		let (instance, key_length) = Self::make_instance_from_db(self.id, &self.db)?;
		assert_eq!(self.key_length, key_length);
		self.instance = instance;
		Ok(())
	}

	fn make_instance_from_db(
		id: u64,
		db: &Arc<sled::Db>,
	) -> wasmer_runtime::error::Result<(Arc<Mutex<Instance>>, u8)> {
		let tree = db.open_tree([INDEX_FUNCTIONS]).unwrap();
		let source = tree.get(id.to_le_bytes()).unwrap().unwrap();
		Self::make_instance(id, &source)
	}

	fn make_instance(
		id: u64,
		source: &[u8],
	) -> wasmer_runtime::error::Result<(Arc<Mutex<Instance>>, u8)> {
		use wasmer_runtime::{func, instantiate, Export, Func, ImportObject, Memory};
		use wasmer_runtime_core::{import::Namespace, types::MemoryDescriptor, units::Pages};

		let mut namespace = Namespace::new();
		namespace.insert("log", func!(wasm::log));
		namespace.insert(
			"key_space",
			Memory::new(MemoryDescriptor {
				minimum: Pages(1),
				maximum: None,
				shared: false,
			})
			.unwrap(),
		);

		let mut import = ImportObject::new();
		import.register("env", namespace);

		let instance = instantiate(source, &import)?;

		let key_length = instance
			.exports()
			.find_map(|(s, e)| match (s.as_str(), e) {
				("KEY_LENGTH", Export::Global(g)) => Some({
					let g = g.get();
					g.to_u128()
						.try_into()
						.expect(&format!("Length is too large: {:?}", g))
				}),
				("key_length", Export::Function { .. }) => Some({
					let func: Func<(), (i32)> = instance.func("key_length").ok()?;
					let n = func.call().unwrap();
					n.try_into()
						.expect(&format!("Length is too large: {:?}", n))
				}),
				_ => None,
			})
			.expect("No KEY_LENGTH global or key_length function export");

		if !instance.exports().any(|(name, _)| name == "key_factory") {
			panic!("No key_factory function export");
		}

		Ok((Arc::new(Mutex::new(instance)), key_length))
	}

	pub fn call(&self, value: &[u8]) -> Result<Vec<u8>, wasmer_runtime::error::CallError> {
		use wasmer_runtime::Func;
		let mut instance = self.instance.lock().unwrap();
		let memory = instance.context_mut().memory(0);

		let in_start = 0;
		let in_end = in_start + value.len();

		for (byte, cell) in value.iter().zip(memory.view()[in_start..in_end].iter()) {
			cell.set(*byte);
		}

		let func: Func<(i32, i32), (i32)> = instance.func("key_factory").unwrap();
		let result = func.call(
			in_start.try_into().expect("in_start too large"),
			in_end.try_into().expect("in_end too large"),
		)?;

		if result < 0 {
			panic!("Index function returned {}", result);
		}

		let memory = instance.context_mut().memory(0);
		let output = wasm::memory_bytes(
			memory,
			result.try_into().unwrap(),
			self.key_length.try_into().unwrap(),
		);

		Ok(output)
	}
}

#[cfg(test)]
mod function_tests {
	use super::Function;

	#[test]
	fn zero() {
		let bytes = include_bytes!("../wats/keying-zero.wasm");
		let zero = Function::new_from_source(0, bytes).unwrap();
		assert_eq!(zero.id, 0);
		assert_eq!(zero.key_length, 0);

		let key = zero.call(&[]).unwrap();
		assert_eq!(key, vec![]);
	}

	#[test]
	fn passthru64() {
		let bytes = include_bytes!("../wats/keying-passthru-64.wasm");
		let func = Function::new_from_source(0, bytes).unwrap();
		assert_eq!(func.id, 0);
		assert_eq!(func.key_length, 64);

		let key = func.call(b"In Aziraphale's case, the relevant knowledge was, Crowley believed, largely theoretical.").unwrap();
		assert_eq!(
			key,
			b"In Aziraphale's case, the relevant knowledge was, Crowley believ".to_vec()
		);
	}

	#[test]
	fn static_key() {
		let bytes = include_bytes!("../wats/keying-static.wasm");
		let func = Function::new_from_source(0, bytes).unwrap();
		assert_eq!(func.id, 0);
		assert_eq!(func.key_length, 8);

		let key = func
			.call(b"even your lame goth cousin thinks it's weird")
			.unwrap();
		assert_eq!(key, b"kraken69".to_vec());
	}

	#[test]
	fn xor() {
		let bytes = include_bytes!("../wats/keying-xor.wasm");
		let func = Function::new_from_source(0, bytes).unwrap();
		assert_eq!(func.id, 0);
		assert_eq!(func.key_length, 1);

		let key = func
			.call(b"The world is becoming out of joint.")
			.unwrap();
		assert_eq!(key, vec![24]);
	}

	#[test]
	fn keylength_function() {
		let bytes = include_bytes!("../wats/keylength-func.wasm");
		let func = Function::new_from_source(0, bytes).unwrap();
		assert_eq!(func.id, 0);
		assert_eq!(func.key_length, 0);
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

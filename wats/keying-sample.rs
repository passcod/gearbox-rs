// This is a Rust sample of the zero keying function

#[no_mangle]
pub extern "C" fn key_length() -> i32 {
	0
}

#[no_mangle]
pub extern "C" fn key_factory(ptr: i32, _: i32) -> i32 {
	ptr
}

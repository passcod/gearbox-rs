;; The zero keying function returns an empty, zero-sized byte array.

(module
  (memory (import "env" "key_space") 1)
  (global (export "key_length") i32 i32.const 0)
  (func
    (export "key_factory")
    (param i32)
    (param i32)
    (param i32)
    (result i32)
    i32.const 0
  )
)

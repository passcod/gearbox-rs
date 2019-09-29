;; Uses a key_length function instead of the KEY_LENGTH global.

(module
  (memory (import "env" "key_space") 1)
  (func (export "key_length") (result i32) i32.const 0)
  (func
    (export "key_factory")
    (param i32 i32)
    (result i32)
    local.get 0
  )
)

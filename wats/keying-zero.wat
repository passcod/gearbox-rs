;; The zero keying function returns an empty, zero-sized byte array.
;; It does this by declaring an empty key length and returning the input.

(module
  (memory (import "env" "key_space") 1)
  (global (export "KEY_LENGTH") i32 i32.const 0)
  (func
    (export "key_factory")
    (param i32)
    (param i32)
    (result i32)
    local.get 0
  )
)

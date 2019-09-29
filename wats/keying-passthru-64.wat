;; The passthru-64 keying function returns 64 bytes of the input.
;; It does this by declaring a 64-byte key length and returning the input.

(module
  (memory (import "env" "key_space") 1)
  (global (export "KEY_LENGTH") i32 i32.const 64)
  (func
    (export "key_factory")
    (param i32 i32)
    (result i32)
    local.get 0
  )
)

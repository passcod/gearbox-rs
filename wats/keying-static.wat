;; The static keying function returns a static 8-byte string.

(module
  (memory (import "env" "key_space") 1)
  (global (export "KEY_LENGTH") i32 i32.const 8)
  (data (i32.const 12345) "kraken69")
  (func
	(export "key_factory")
	(param i32 i32)
	(result i32)
    i32.const 12345
  )
)

;; The xor keying function returns a single-byte key by xoring all input.

(module
	(memory (import "env" "key_space") 1)
	(global (export "KEY_LENGTH") i32 i32.const 1)
	(func
		(export "key_factory")
		(param $ptr i32)
		(param $len i32)
		(result i32)
		(local $val i32)
		(local $cur i32)
		(local $end i32)
		(local $hash i32)

		(set_local $val (i32.const 0))
		(set_local $cur (get_local $ptr))
		(set_local $end (i32.add (get_local $ptr) (get_local $len)))
		(set_local $hash (i32.add (i32.const 1) (get_local $end)))

		(block (loop
			(set_local $val (get_local $val) (i32.xor (i32.load8_u (get_local $cur))))
			(set_local $cur (i32.add (i32.const 1) (get_local $cur)))
			(br_if 1 (i32.ge_u (get_local $cur) (get_local $end)))
			(br 0)
		))

		(i32.store8 (get_local $hash) (get_local $val))
		(get_local $hash)
	)
)

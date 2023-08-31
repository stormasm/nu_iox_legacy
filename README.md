
This code is from the summer of 2022 when I was integrating for the first time
nushell into iox...

The nu_iox folder is the nu-command crate with an additional iox folder
which contains all of the nushell iox commands.

The iox_nu folder is the main nushell binary along with a Cargo.toml file
which shows

```rust
nu-command = { path="../nu_iox", version = "0.66.3"  }
```

the integration of nu_iox into nushell via the nu-command crate.

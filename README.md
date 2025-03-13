# Build

If you're on macOS, start a linux container

```bash
docker run -it --platform linux/amd64 --rm -v $(pwd):/hg -w /hg rust:1.85.0-alpine3.21 /bin/sh
```

Inside the container, install the dependencies

```bash
apk add libressl-dev gcc musl-dev
```

Inside the container, build the binary

```bash
cargo build --release
```

The binary is `target/release/target-csv`

# Run

```bash
./target.csv --config config.json
```

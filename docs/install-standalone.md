# Installing Redka as a standalone server

Redka server is a single-file binary. Download it from the [releases](https://github.com/nalgeon/redka/releases).

Linux (x86 CPU only):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/v0.5.3/redka_linux_amd64.zip"
unzip redka_linux_amd64.zip
chmod +x redka
```

macOS (x86 CPU):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/v0.5.3/redka_darwin_amd64.zip"
unzip redka_darwin_amd64.zip
# remove the build from quarantine
# (macOS disables unsigned binaries)
xattr -d com.apple.quarantine redka
chmod +x redka
```

macOS (ARM/Apple Silicon CPU):

```shell
curl -L -O "https://github.com/nalgeon/redka/releases/download/v0.5.3/redka_darwin_arm64.zip"
unzip redka_darwin_arm64.zip
# remove the build from quarantine
# (macOS disables unsigned binaries)
xattr -d com.apple.quarantine redka
chmod +x redka
```

Or pull with Docker as follows (x86/ARM):

```shell
docker pull nalgeon/redka
```

Or build from source (requires Go 1.22 and GCC):

```shell
git clone https://github.com/nalgeon/redka.git
cd redka
make setup build
# the path to the binary after the build
# will be ./build/redka
```

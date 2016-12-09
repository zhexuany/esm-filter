go build -v -v -ldflags="-X main.version=$(git describe --always --long) -X main.commit=$(git log --pretty=format:'%h' -n 1) -X main.branch=$(git rev-parse --abbrev-ref HEAD)"


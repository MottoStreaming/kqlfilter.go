# kqlfilter.go

[![GoDoc][godoc:image]][godoc:url]

This package contains Kibana Query Language parser.

```bash
go get "github.com/MottoStreaming/kqlfilter.go"
```

## Usage

You can use either `Parse` or `ParseAST` to parse a KQL filter.

`Parse` will return a `Filter` struct, which is simple to use, but does not support all KQL features.
```go
package main

import (
    "fmt"

    "github.com/MottoStreaming/kqlfilter.go"
)

func main() {
    filter, err := kqlfilter.Parse("foo:bar", false)
    if err != nil {
        panic(err)
    }

    fmt.Println(filter)
}
```

`ParseAST` will return an `AST` struct, which is more complex to use, but supports all KQL features.
It returns an `AST` struct, which is a tree of `Node`s.
```go
package main

import (
    "fmt"

    "github.com/MottoStreaming/kqlfilter.go"
)

func main() {
    ast, err := kqlfilter.ParseAST("foo:bar")
    if err != nil {
        panic(err)
    }

    fmt.Println(ast)
}
```

[godoc:image]:    https://pkg.go.dev/badge/github.com/MottoStreaming/kqlfilter.go
[godoc:url]:      https://pkg.go.dev/github.com/MottoStreaming/kqlfilter.go

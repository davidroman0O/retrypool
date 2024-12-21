I will see that later

I think we can re-use the basic ideas of `tempolite` to make the DependecyPooler super useful for simpler usecases.

Since we know we can grab the definition of each functions, which allow dynamic input/output, we could leverage that to make a builder that manage exactly that!

```
Group (defined)
    -> TaskRuntime (hidden)
    -> Task (defined)
```

```go

group, err := NewGroup("group1")
    .Add(
        "taskA", 
        func(ctx context.Context, input int) (int, error) {
            return input * 2, nil
        },
    ).
    Require("input").
    Map("int", "output")
    .Add(
        "taskB", 
        func(ctx context.Context, input int) (int, error) {
            return input * 2, nil
        },
    ).
    DependsOn("taskA").
    Match("output").To("input").
    Build(42)

if err != nil {
    panic(err)
}

if err := group.Submit(dp); err != nil {
    panic(err)
}

```

something like that 
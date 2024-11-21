# GenBatcher

`GenBatcher` is a simple and lightweight batching utility for Elixir.

## Installation

This package can be installed by adding `:gen_batcher` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:gen_batcher "~> 1.0.0"}
  ]
end
```

## Documentation

TODO(Gordon) - Add this once published

## Usage

`GenBatcher` processes are easy to start and have a number of powerful
configuration options:

### Child Spec Example

A `GenBatcher` can be started with a simple child spec:

```elixir
opts = [
  name: :my_gen_batcher,
  handle_flush: fn items, _ -> IO.inspect(items) end,
  flush_trigger: {:size, 3},
  batch_timeout: 30_000,
]

children = [
  {GenBatcher, opts}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Once started, items can be inserted into the `GenBatcher`:

```elixir
GenBatcher.insert(:my_gen_batcher, "foo")
GenBatcher.insert(:my_gen_batcher, "bar")
```

And, once a flush condition has been met, a flush operation will be triggered:

```elixir
GenBatcher.insert(:my_gen_batcher, "baz")
# Flush operation outputs ["foo", "bar", "baz"]

GenBatcher.insert(:my_gen_batcher, "foo")
# After 30 seconds pass...
# Flush operation outputs ["foo"]
```

### Behaviour Example

A `GenBatcher` can also be started with any module that implements the
`GenBatcher` behaviour:

```elixir
defmodule MyGenBatcher do
  use GenBatcher

  def start_link(opts \\ []) do
    GenBatcher.start_link(__MODULE__, opts)
  end

  @impl GenBatcher
  def handle_flush(items, _) do
    IO.inspect(items)
  end

  @impl GenBatcher
  def handle_insert(item, acc) do
    size = acc + byte_size(item)
    if size >= 9, do: :flush, else: {:cont, size}
  end

  @impl GenBatcher
  def initial_acc, do: 0
end
```

Again, items can be inserted into the `GenBatcher` once it starts:

```elixir
GenBatcher.insert(MyGenBatcher, "foo")
GenBatcher.insert(MyGenBatcher, "bar")
```

And, again, a flush operation will be triggered once a flush condition is met:

```elixir
GenBatcher.insert(MyGenBatcher, "baz")
# Flush operation outputs ["foo", "bar", "baz"]
```

### Flushing

By default, flush operations are asynchronous to both the caller and the
`GenBatcher` process itself. In practice, this is achieved by making use of the
`Task.Supervisor` module.
--------------------------------------------------------------------------------
However, 


### Complex Flush Triggers

handle insert never called "out of bounds"

### Partitioning

TODO(Gordon) - jittering

### Operation Timeouts

something something something dark side

### Shutdown

TODO(Gordon) - data will always be flushed once

## ExBuffer

`GenBatcher` was created as a conceptual fork of the now-archived
[`ExBuffer`](https://hexdocs.pm/ex_buffer/readme.html) package and is intended
to supersede it.

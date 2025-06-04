# GenBatcher

![CI](https://github.com/gdwoolbert3/gen_batcher/actions/workflows/ci.yml/badge.svg)
[![Package](https://img.shields.io/hexpm/v/gen_batcher.svg)](https://hex.pm/packages/gen_batcher)

`GenBatcher` is a simple and lightweight batching utility for Elixir.

## Installation

This package can be installed by adding `:gen_batcher` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:gen_batcher "~> 1.1.0"}
  ]
end
```

## Documentation

For additional documentation, see [HexDocs](https://hexdocs.pm/gen_batcher/readme.html).

## Usage

`GenBatcher` processes are easy to start and have a number of powerful
configuration options:

### Child Spec Example

A `GenBatcher` can be started with a simple child spec:

```elixir
opts = [
  name: :my_gen_batcher,
  flush_trigger: {:size, 3},
  batch_timeout: 30_000,
  handle_flush:
    fn items, _ ->
      items
      |> Enum.join(",")
      |> IO.puts()
    end
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
# Flush operation outputs "foo,bar,baz"

GenBatcher.insert(:my_gen_batcher, "foo")
# After 30 seconds pass...
# Flush operation outputs "foo"
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
    items
    |> Enum.join(",")
    |> IO.puts()
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
# Flush operation outputs "foo,bar,baz"
```

### Flushing

By default, flush operations are asynchronous to both the caller and the
`GenBatcher` partition. In practice, this is achieved by making use of the
`Task.Supervisor` module. However, this can be configured with the
`:blocking_flush?` option for `GenBatcher.start_link/2`. If set to `true`, the
partition process will perform the flush operation instead of delegating to a
`Task`. This can be useful for applying backpressure and ensuring the system
isn't completely flooded.

A `GenBatcher` with an extremely cheap flush operation _might_ see a higher
throughput when utilizing blocking flushes but this is generally not the case.

### Complex Flush Triggers

In general, a size condition and/or timeout condition is sufficient for most use
cases. However, `GenBatcher` also supports defining custom item-based flush
triggers. [For example](#behaviour-example), these callbacks can be used to
trigger a flush based on byte size.

In cases where an item-based flush trigger is temporarily delayed (ie
`GenBatcher.insert_all/3`), the `c:GenBatcher.handle_insert/2` callback will not
be called again until after a flush operation is triggered. This means that the
accumulator term is guaranteed to be in a valid state whenever this callback is
invoked.

### Partitioning

`GenBatcher` leverages Elixir's `PartitionSupervisor` in order to support
partitioning. All of a `GenBatcher`'s partitions collect items and flush
independently.

By default, `GenBatcher` uses a round-robin partitioner when inserting items.
However, the partitioner can be overridden with the `:partition_key` option for
`GenBatcher.insert/3` and `GenBatcher.insert_all/3`, allowing for custom
partitioning strategies.

All of a `GenBatcher`'s partitions utilize the same flush conditions. This can
occasionally lead to bursts of flush operations being triggered at around the
same time. The `c:GenBatcher.initial_acc/0` callback can be leveraged to
"jitter" item-based flush triggers in order to "desync" flush operations and
mitigate this issue. For example, the `GenBatcher` below enforces an absolute
maximum size of 1,000 items but randomly assigns each partition a maximum size
between 901 and 1,000 items:

```elixir
defmodule MyJitteredGenBatcher do
  use GenBatcher

  def start_link(opts \\ []) do
    opts = Keyword.put(opts, :partitions, 5)
    GenBatcher.start_link(__MODULE__, opts)
  end

  @impl GenBatcher
  def handle_flush(items, _) do
    items
    |> Enum.join(",")
    |> IO.puts()
  end

  @impl GenBatcher
  def handle_insert(_, 1), do: :flush
  def handle_insert(_, acc), do: {:cont, acc - 1}

  @impl GenBatcher
  def initial_acc, do: 900 + :rand.uniform(100)
end
```

### Shutdown

As long as a `GenBatcher` is shutdown gracefully, it's guaranteed to flush all
inserted items. The last flush operation for each partition is always performed
by the partition itself, regardless of the `:blocking_flush?` option provided to
`GenBatcher.start_link/2`.

By default, non-blocking flushes are given unlimited time to complete during
shutdown to ensure that data is not lost. However, if this behavior is not
desirable and data loss is acceptable, this can be managed with the `:shutdown`
option for `GenBatcher.start_link/2`.

## ExBuffer

`GenBatcher` was created as a conceptual fork of the now-archived
[`ExBuffer`](https://hexdocs.pm/ex_buffer/readme.html) package and is intended
to supersede it.

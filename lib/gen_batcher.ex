defmodule GenBatcher do
  @moduledoc """
  A `GenBatcher` is a process that maintains a collection of items and performs
  a user-defined flush operation on those items once an item-based condition is
  met or a timeout is exceeded.

  TODO(Gordon) - test module-only parameterization?
  TODO(Gordon) - test the blocking default for flush
  TODO(Gordon) - test flush_empty? behavior
  TODO(Gordon) - readme badges
  TODO(Gordon) - remove benchee dep
  TODO(Gordon) - doctests?

  TODO(Gordon) - configure styler and other checks for CI
  TODO(Gordon) - configure styler
  TODO(Gordon) - use partition supervisor for task supervisor?

  TODO(Gordon) - add flush_empty? opt for manual flush function?
  """

  alias GenBatcher.Partition
  alias GenBatcher.Partition.Info

  @default_timeout :infinity

  @opts_schema [
    :flush_meta,
    :flush_trigger,
    :handle_flush,
    :name,
    batch_timeout: :infinity,
    blocking_flush?: false,
    flush_empty?: false,
    partitions: 1,
    shutdown: :infinity
  ]

  ################################
  # Types
  ################################

  @typedoc """
  Information about a `GenBatcher` partition.

  For additional details, see `GenBatcher.Partition.Info`.
  """
  @type partition_info :: Info.t()

  @typedoc """
  A routing key used for selecting a `GenBatcher` partition.

  For additional details, see `m:PartitionSupervisor#module-implementation-notes`.
  """
  @type partition_key :: term()

  @typedoc "An identifier for a `GenBatcher` process."
  @type t :: PartitionSupervisor.name()

  ################################
  # Callbacks
  ################################

  @doc """
  The function invoked to flush a `GenBatcher` partition.

  The first argument is a `t:list/0` of inserted items and the second argument
  is a `t:partition_info/0` struct.

  This callback can return any `t:term/0` as the return value is disregarded.

  This callback is required.
  """
  @callback handle_flush(list(), partition_info()) :: term()

  @doc """
  The function invoked when an item is inserted into a `GenBatcher` partition.

  The first argument is the inserted `t:term/0` and the second argument is the
  accumulator `t:term/0`.

  This callback should return an updated accumulator `t:term/0` wrapped in a
  `:cont` tuple to continue collecting items or `:flush` to trigger a flush.

  This callback is optional and the default implementation always returns
  `{:cont, acc}` where acc is the accumulator `t:term/0`.
  """
  @callback handle_insert(term(), term()) :: {:cont, term()} | :flush

  @doc """
  The function invoked after a flush to generate the initial accumulator
  `t:term/0`.

  This callback is optional and the default implementation simply returns `nil`.
  """
  @callback initial_acc :: term()

  @optional_callbacks handle_insert: 2, initial_acc: 0

  ################################
  # Public API
  ################################

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    partitions = Keyword.get(opts, :partitions, 1)
    type = if partitions == 1, do: :worker, else: :supervisor

    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      shutdown: :infinity,
      type: type
    }
  end

  @doc """
  Starts a `GenBatcher` process linked to the current process.

  For more information, see `start_link/2`.
  """
  @spec start_link(module() | keyword()) :: Supervisor.on_start()
  def start_link(arg) when is_list(arg), do: start_link(nil, arg)
  def start_link(arg), do: start_link(arg, [])

  @doc """
  Starts a `GenBatcher` process linked to the current process.

  A `GenBatcher` can be started with a `t:module/0` that implements the
  `GenBatcher` behaviour, an opts `t:keyword/0`, or a combination of the two.

  ## Options

  A `GenBatcher` can be started with the following options:

    * `:batch_timeout` - An optional `t:timeout/0` denoting the maximum amount
      of time (in ms) allowed between flushes. Defaults to `:infinity`.

    * `:blocking_flush?` - An optional `t:boolean/0` denoting whether or not a
      flush should block a partition process. This can be useful for applying
      backpressure. Defaults to `false`.

    * `:flush_empty?` - An optional `t:boolean/0` denoting whether or not an
      empty partition should be flushed. Defaults to `false`.

    * `:flush_meta` - An optional `t:term/0` to be included in
      `t:partition_info/0` structs. Defaults to `nil`.

    * `:flush_trigger` - An optional specification for an item-based flush
      condition that can take any of the following forms:

      * `{:size, max_size}` where `max_size` is a `t:pos_integer/0` denoting the
        maximum number of items allowed in a partition before a flush is
        triggered.

      * `{:static_custom, init_acc, handle_insert}` where `init_acc` is a
        `term/0` used as the inital accumulator and `handle_insert` is a
        function invoked when an item is inserted into a partition. For more
        information, see `c:handle_insert/2`.

      * `{:dynamic_custom, init_acc_fun, handle_insert}` where `init_acc_fun` is
        a function invoked to generate an initial accumulator and
        `handle_insert` is a function invoked when an item is inserted into a
        partition. For more information about these functions, see
        `c:initial_acc/0` and `c:handle_insert/2` respectively.

      If a `t:module/0` is provided and `:flush_trigger` is not explicitly
      specified, `{:dynamic_custom, init_acc_fun, handle_insert}` will be used
      where `init_acc_fun` and `handle_insert` are that module's `initial_acc/0`
      and `handle_insert/2` implementations respectively.

      If `:flush_trigger` is not specified and a `t:module/0` is not provided,
      the `GenBatcher` will not have an item-based flush condition (meaning that
      inserting items will never trigger a flush).

    * `:handle_flush` - A function invoked to flush a partition. For more
      information, see `c:handle_flush/2`. When a `t:module` is provided, this
      field will be **overridden** by that module's `handle_flush/2`
      implementation. If a `t:module/0` is not provided, `:handle_flush` is
      required.

    * `:name` - An optional identifier for a `GenBatcher`. For more information,
      see `t:t/0`. If a `t:module/0` is provided, that module's name will be
      used unless otherwise specified. Defaults to `GenBatcher`.

    * `:partitions` - An optional `t:pos_integer/0` denoting the number of
      partitions. Defaults to `1`.
      TODO(Gordon) - link to partitioning header

    * `:shutdown` - An optional `t:timeout/0` denoting the maximum amount of
      time (in ms) to allow flushes to finish on shutdown or `:brutal_kill` if
      flushes should be stopped immediately. Defaults to `:infinity`.
      TODO(Gordon) - link to shutdown header
  """
  @spec start_link(module() | nil, keyword()) :: Supervisor.on_start()
  def start_link(module, opts) do
    {partitions, opts} =
      opts
      |> update_opts(module)
      |> Keyword.pop!(:partitions)

    with {:ok, _} = result <- do_start_link(opts, partitions) do
      persist_meta(opts, partitions)
      result
    end
  end

  @doc """
  Dumps the contents of all partitions for the given `GenBatcher` to a nested
  list, bypassing flushes and refreshing the partitions.

  The partitions' contents are returned in index order.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  A `GenBatcher` can be dumped with the following options:

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.

  ## Examples

      iex> GenBatcher.insert(GenBatcher, "foo")
      iex> GenBatcher.insert(GenBatcher, "bar")
      iex> GenBatcher.insert(GenBatcher, "baz")
      iex> GenBatcher.dump(GenBatcher)
      [["foo", "baz"], ["bar"]]
  """
  @spec dump(t()) :: [list()]
  @spec dump(t(), keyword()) :: [list()]
  def dump(gen_batcher, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _} ->
        [Partition.dump(gen_batcher, timeout)]

      {partitions, _} ->
        Enum.map(0..(partitions - 1), fn partition_key ->
          gen_batcher
          |> partition_name(partition_key)
          |> Partition.dump(timeout)
        end)
    end
  end

  @doc """
  Dumps the contents of the given `GenBatcher` partition to a list, bypassing a
  flush and refreshing the partition.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  A `GenBatcher` partition can be dumped with the following options:

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec dump_partition(t(), partition_key()) :: list()
  @spec dump_partition(t(), partition_key(), keyword()) :: list()
  def dump_partition(gen_batcher, partition_key, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _} ->
        Partition.dump(gen_batcher, timeout)

      _ ->
        gen_batcher
        |> partition_name(partition_key)
        |> Partition.dump(timeout)
    end
  end

  @doc """
  Flushes all partitions for the given `GenBatcher`.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  A `GenBatcher` can be flushed with the following options:

    * `:async?` - An optional `t:boolean/0` denoting whether or not a partition
      should be flushed asynchronously. Defaults to `true`.

    * `:concurrent?` - An optional `t:boolean/0` denoting whether or not
      partitions should flush concurrently. Only relevant if `:async?` is
      `false` and the given `GenBatcher` has more than 1 partition. Defaults to
      `true`.

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec flush(t()) :: :ok
  @spec flush(t(), keyword()) :: :ok
  def flush(gen_batcher, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)
    {concurrent?, opts} = Keyword.pop(opts, :concurrent?, true)
    async? = Keyword.get(opts, :async?, true)
    flush_fun = flush_function(async?, timeout)
    {partitions, _} = fetch_meta(gen_batcher)

    case {partitions, async?, concurrent?} do
      {1, _, _} ->
        flush_fun.(gen_batcher)

      {partitions, false, true} ->
        GenBatcher.TaskSupervisor
        |> Task.Supervisor.async_stream_nolink(
          0..(partitions - 1),
          fn partition_key ->
            gen_batcher
            |> partition_name(partition_key)
            |> flush_fun.()
          end,
          max_concurrency: partitions,
          timeout: :infinity,
          ordered: false
        )
        |> Stream.run()

      _ ->
        Enum.each(0..(partitions - 1), fn partition_key ->
          gen_batcher
          |> partition_name(partition_key)
          |> flush_fun.()
        end)
    end
  end

  @doc """
  Flushes the given `GenBatcher` partition.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  A `GenBatcher` partition can be flushed with the following options:

    * `:async?` - An optional `t:boolean/0` denoting whether or not the
      partition should be flushed asynchronously. Defaults to `true`.

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec flush_partition(t(), partition_key()) :: :ok
  @spec flush_partition(t(), partition_key(), keyword()) :: :ok
  def flush_partition(gen_batcher, partition_key, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)
    {partitions, _} = fetch_meta(gen_batcher)

    flush_fun =
      opts
      |> Keyword.get(:async?, true)
      |> flush_function(timeout)

    case partitions do
      1 ->
        flush_fun.(gen_batcher)

      _ ->
        gen_batcher
        |> partition_name(partition_key)
        |> flush_fun.()
    end
  end

  @doc """
  Returns information for all partitions for the given `GenBatcher`.

  The `t:partition_info/0` structs are returned in index order.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  Information about a `GenBatcher` can be retrieved with the following options:

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec info(t()) :: [partition_info()]
  @spec info(t(), keyword()) :: [partition_info()]
  def info(gen_batcher, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _} ->
        [Partition.info(gen_batcher, timeout)]

      {partitions, _} ->
        Enum.map(0..(partitions - 1), fn partition_key ->
          gen_batcher
          |> partition_name(partition_key)
          |> Partition.info(timeout)
        end)
    end
  end

  @doc """
  Inserts an item into the given `GenBatcher`.

  ## Options
  An item can be inserted into a `GenBatcher` with the following options:

    * `:partition_key` - An optional `t:partition_key/0` denoting which
      partition to insert the item into. If not specified, the partition is
      decided by the round-robin partitioner.
      TODO(Gordon) - link to partition doc

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec insert(t(), term()) :: :ok
  @spec insert(t(), term(), keyword()) :: :ok
  def insert(gen_batcher, item, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _} ->
        Partition.insert(gen_batcher, item, timeout)

      {_, partitioner} ->
        partition_key = Keyword.get_lazy(opts, :partition_key, partitioner)

        gen_batcher
        |> partition_name(partition_key)
        |> Partition.insert(item, timeout)
    end
  end

  @doc """
  Inserts multiple items into the given `GenBatcher` and returns the number of
  items inserted.

  All of the given items are inserted into the same partition.

  > #### Tip {: .tip}
  >
  > When inserting multiple items into an `GenBatcher`, this function will be
  > far more performant than calling `insert/2` for each one.

  ## Options

  Items can be inserted into a `GenBatcher` with the following options:

    * `:partition_key` - An optional `t:partition_key/0` denoting which
      partition to insert the items into. If not specified, the partition is
      decided by the round-robin partitioner.
      TODO(Gordon) - link to partition doc

    * `:safe?` - An optional `t:boolean/0` denoting whether or not to flush
      immediately after a flush condition is met. If `true` and a flush
      condition is met, a flush is triggered before inserting the remaining
      items. Otherwise, a flush is triggered once **all** items have been
      inserted. This can improve throughput at the cost of allowing otherwise
      impossible batches. Only relevant if a flush would be triggered before all
      items are inserted. Defaults to `true`.

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec insert_all(t(), Enumerable.t()) :: non_neg_integer()
  @spec insert_all(t(), Enumerable.t(), keyword()) :: non_neg_integer()
  def insert_all(gen_batcher, items, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)
    {partitions, partitioner} = fetch_meta(gen_batcher)

    insert_all_fun =
      opts
      |> Keyword.get(:safe?, true)
      |> insert_all_function(items, timeout)

    case partitions do
      1 ->
        insert_all_fun.(gen_batcher)

      _ ->
        partition_key = Keyword.get_lazy(opts, :partition_key, partitioner)

        gen_batcher
        |> partition_name(partition_key)
        |> insert_all_fun.()
    end
  end

  @doc """
  Returns information for the given `GenBatcher` partition.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  Information about a `GenBatcher` partition can be retrieved with the following
  options:

    * `:timeout` - An optional `t:timeout/0` (in ms) for this operation. For
      more information, see [Operation Timeouts](README.md#operation-timeouts).
      Defaults to `:infinity`.
  """
  @spec partition_info(t(), partition_key()) :: partition_info()
  @spec partition_info(t(), partition_key(), keyword()) :: partition_info()
  def partition_info(gen_batcher, partition_key, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _} ->
        Partition.info(gen_batcher, timeout)

      _ ->
        gen_batcher
        |> partition_name(partition_key)
        |> Partition.info(timeout)
    end
  end

  @doc false
  @spec __using__(keyword()) :: Macro.t()
  defmacro __using__(_opts) do
    quote do
      @behaviour GenBatcher

      def child_spec(opts) do
        partitions = Keyword.get(opts, :partitions, 1)
        type = if partitions == 1, do: :worker, else: :supervisor

        %{
          id: Keyword.get(opts, :name, __MODULE__),
          start: {__MODULE__, :start_link, [opts]},
          shutdown: :infinity,
          type: type
        }
      end

      @impl GenBatcher
      def handle_insert(_, _), do: {:cont, nil}

      @impl GenBatcher
      def initial_acc, do: nil

      defoverridable(child_spec: 1, handle_insert: 2, initial_acc: 0)
    end
  end

  ################################
  # Private API
  ################################

  defp update_opts(opts, module) do
    opts
    |> update_name(module)
    |> update_callbacks(module)
    |> Keyword.validate!(@opts_schema)
    |> update_flush_trigger_opts()
  end

  defp update_name(opts, nil), do: Keyword.put_new(opts, :name, __MODULE__)
  defp update_name(opts, module), do: Keyword.put_new(opts, :name, module)

  defp update_callbacks(opts, nil), do: opts

  defp update_callbacks(opts, module) do
    flush_trigger = {:dynamic_custom, &module.initial_acc/0, &module.handle_insert/2}

    opts
    |> Keyword.put(:handle_flush, &module.handle_flush/2)
    |> Keyword.put_new(:flush_trigger, flush_trigger)
  end

  defp update_flush_trigger_opts(opts) do
    {flush_trigger, opts} = Keyword.pop(opts, :flush_trigger)
    flush_trigger_opts = flush_trigger_opts(flush_trigger)
    Keyword.merge(opts, flush_trigger_opts)
  end

  defp flush_trigger_opts(nil) do
    [handle_insert: fn _, _ -> {:cont, nil} end, initial_acc: {:static, nil}]
  end

  defp flush_trigger_opts({:size, max_size}) do
    [
      handle_insert: fn
        _, acc when acc <= 1 -> :flush
        _, acc -> {:cont, acc - 1}
      end,
      initial_acc: {:static, max_size}
    ]
  end

  defp flush_trigger_opts({:static_custom, initial_acc, handle_insert}) do
    [handle_insert: handle_insert, initial_acc: {:static, initial_acc}]
  end

  defp flush_trigger_opts({:dynamic_custom, init_acc_fun, handle_insert}) do
    [handle_insert: handle_insert, initial_acc: {:dynamic, init_acc_fun}]
  end

  defp do_start_link(opts, 1) do
    opts
    |> Keyword.put(:partition, 0)
    |> Partition.start_link()
  end

  defp do_start_link(opts, partitions) do
    {name, part_opts} = Keyword.pop!(opts, :name)

    sup_opts = [
      with_arguments: fn [opts], part -> [Keyword.put(opts, :partition, part)] end,
      child_spec: {Partition, part_opts},
      partitions: partitions,
      name: name
    ]

    PartitionSupervisor.start_link(sup_opts)
  end

  defp persist_meta(opts, partitions) do
    partitioner = build_partitioner(partitions)

    opts
    |> Keyword.fetch!(:name)
    |> gen_batcher_key()
    |> :persistent_term.put({partitions, partitioner})
  end

  defp build_partitioner(1), do: fn -> 0 end

  defp build_partitioner(partitions) do
    atomics_ref = :atomics.new(1, signed: false)
    :atomics.sub(atomics_ref, 1, 1)
    fn -> rem(:atomics.add_get(atomics_ref, 1, 1), partitions) end
  end

  defp fetch_meta(gen_batcher) do
    gen_batcher
    |> gen_batcher_key()
    |> :persistent_term.get()
  end

  defp gen_batcher_key(gen_batcher), do: {__MODULE__, gen_batcher}

  defp flush_function(true, timeout), do: &Partition.flush_async(&1, timeout)
  defp flush_function(_, timeout), do: &Partition.flush_sync(&1, timeout)

  defp insert_all_function(true, items, timeout) do
    &Partition.insert_all_safe(&1, items, timeout)
  end

  defp insert_all_function(_, items, timeout) do
    &Partition.insert_all_unsafe(&1, items, timeout)
  end

  defp partition_name(gen_batcher, partition_key) do
    {:via, PartitionSupervisor, {gen_batcher, partition_key}}
  end
end

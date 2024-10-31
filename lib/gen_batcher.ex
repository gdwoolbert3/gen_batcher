defmodule GenBatcher do
  @moduledoc """
  A `GenBatcher` is a process that maintains a collection of items and performs
  a user-defined flush operation on those items once an item-based condition is
  met or a timeout is exceeded.

  TODO(Gordon) - test module-only parameterization?
  TODO(Gordon) - test the blocking default for flush
  TODO(Gordon) - reconsider how default blocking behavior is handled?
  TODO(Gordon) - readme badges

  TODO(Gordon) - configure styler and other checks for CI
  TODO(Gordon) - configure styler
  TODO(Gordon) - single child spec function for both start approaches
  TODO(Gordon) - use partition supervisor for task supervisor?
  """

  alias GenBatcher.Partition
  alias GenBatcher.Partition.Info

  @default_timeout 5_000

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

    * `:batch_timeout` - A `t:timeout/0` denoting the maximum amount of time
      (in ms) allowed between flushes. This field is optional and defaults to
      `:infinity`.

    * `:blocking_flush?` - A `t:boolean/0` denoting whether or not a flush
      should block a partition process. This can be useful for applying
      backpressure. This field is optional and defaults to `false`

    * `:flush_empty?` - A `t:boolean/0` denoting whether or not an empty
      partition should be flushed. This field is optional and defaults to
      `false`.

    * `:flush_meta` - A `t:term/0` to be included in `t:partition_info/0`
      structs. This field is optional and defaults to `nil`.

    * `:flush_trigger` - A specification for an item-based flush condition. This
      field is optional and has no default (meaning that, if not included,
      inserting items will never trigger a flush). This field can take any of
      the following forms:

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

      If a `t:module/0` is provided and this field in not explicitly specified,
      `{:dynamic_custom, init_acc_fun, handle_insert}` will be used where
      `init_acc_fun` and `handle_insert` are that module's `initial_acc/0` and
      `handle_insert/2` implementations respectively.

    * `:handle_flush` - A function invoked to flush a partition. For more
      information, see `c:handle_flush/2`. When a `t:module` is provided, this
      field will be **overridden** by that module's `handle_flush/2`
      implementation. This field is required.

    * `:name` - An identifier for a `GenBatcher`. For more information, see
      `t:t/0`. If a `t:module/0` is provided, that module's name will be used
      for this field unless otherwise specified. This field is optional and
      defaults to `GenBatcher`.

    * `:partitions` - A `t:pos_integer/0` denoting the number of partitions.
      This field is optional and defaults to `1`.
      TODO(Gordon) - link to partitioning header

    * `:shutdown` - A `t:timeout/0` denoting the maximum amount of time (in ms)
      to allow flushes to finish on shutdown or `:brutal_kill` if flushes should
      be killed immediately. This field is optional and defaults to `:infinity`.
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

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  A `GenBatcher` can be dumped with the following options:

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.

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
      {1, _, _} ->
        [Partition.dump(gen_batcher, timeout)]

      {partitions, _, _} ->
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

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec dump_partition(t(), partition_key()) :: list()
  @spec dump_partition(t(), partition_key(), keyword()) :: list()
  def dump_partition(gen_batcher, partition_key, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _, _} ->
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

    * `:async?` - A `t:boolean/0` denoting whether or not a partition should be
      flushed asynchronously. This field is optional and defaults to `true`.

    * `:blocking?` - A `t:boolean/0` denoting whether or not partitions should
      be blocked while flushing. This field is optional and defaults to the
      behavior specified by the `blocking_flush?` field provided to
      `start_link/2`.

    * `:max_concurrency` - A `t:pos_integer/0` denoting the maximum number of
      partitions that can flush simultaneously. This field is optional and
      defaults to `1`.

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec flush(t()) :: :ok
  @spec flush(t(), keyword()) :: :ok
  def flush(gen_batcher, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)
    {partitions, _, blocking?} = fetch_meta(gen_batcher)
    {blocking?, opts} = Keyword.pop(opts, :blocking?, blocking?)
    {async?, opts} = Keyword.pop(opts, :async?, true)
    flush_fun = flush_function(async?, blocking?, timeout)
    max_concurrency = Keyword.get(opts, :max_concurrency, 1)

    case {partitions, max_concurrency} do
      {1, _} ->
        flush_fun.(gen_batcher)

      {partitions, 1} ->
        Enum.each(0..(partitions - 1), fn partition_key ->
          gen_batcher
          |> partition_name(partition_key)
          |> flush_fun.()
        end)

      {partitions, max_concurrency} ->
        max_concurrency = min(max_concurrency, partitions)
        opts = [max_concurrency: max_concurrency, ordered: false, timeout: :infinity]

        0..(partitions - 1)
        |> Task.async_stream(
          fn partition_key ->
            gen_batcher
            |> partition_name(partition_key)
            |> flush_fun.()
          end,
          opts
        )
        |> Stream.run()
    end
  end

  @doc """
  Flushes the given `GenBatcher` partition.

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  A `GenBatcher` partition can be flushed with the following options:

    * `:async?` - A `t:boolean/0` denoting whether or not a partition should be
      flushed asynchronously. This field is optional and defaults to `true`.

    * `:blocking?` - A `t:boolean/0` denoting whether or not partitions should
      be blocked while flushing. This field is optional and defaults to the
      behavior specified by the `blocking_flush?` field provided to
      `start_link/2`.

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec flush_partition(t(), partition_key()) :: :ok
  @spec flush_partition(t(), partition_key(), keyword()) :: :ok
  def flush_partition(gen_batcher, partition_key, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)
    {partitions, _, blocking?} = fetch_meta(gen_batcher)
    {blocking?, opts} = Keyword.pop(opts, :blocking?, blocking?)

    flush_fun =
      opts
      |> Keyword.get(:async?, true)
      |> flush_function(blocking?, timeout)

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

  While this functionality may occasionally be desriable in a production
  environment, it is intended to be used primarily for testing and debugging.

  ## Options

  Information about a `GenBatcher` can be retrieved with the following options:

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec info(t()) :: [partition_info()]
  @spec info(t(), keyword()) :: [partition_info()]
  def info(gen_batcher, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _, _} ->
        [Partition.info(gen_batcher, timeout)]

      {partitions, _, _} ->
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

    * `:partition_key` - A `t:partition_key/0` denoting which partition to
      insert the item into. This field is optional and, if not included, the
      partition is decided by the round-robin partitioner.
      TODO(Gordon) - link to partition doc

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec insert(t(), term()) :: :ok
  @spec insert(t(), term(), keyword()) :: :ok
  def insert(gen_batcher, item, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _, _} ->
        Partition.insert(gen_batcher, item, timeout)

      {_, partitioner, _} ->
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
  > far more performant than calling `insert/2` for each one. As such, this
  > function should be preferred.

  ## Options

  Items can be inserted into a `GenBatcher` with the following options:

    * `:blocking_flush?` - A `t:boolean/0` denoting whether or not partitions
      should be blocked while flushing. This field is only relevant if a flush
      would be triggered before all items are inserted. This field is optional
      and defaults to the behavior specified by the `blocking_flush?` field
      provided to `start_link/2`.

    * `:partition_key` - A `t:partition_key/0` denoting which partition to
      insert the item into. This field is optional and, if not included, the
      partition is decided by the round-robin partitioner.
      TODO(Gordon) - link to partition doc

    * `:safe?` - A `t:boolean/0` denoting whether or not to flush immediately
      after a flush condition is met. If `false` and a flush condition is met
      before all items have been inserted, the remaining items are inserted
      before a flush is triggered. This can improve throughput at the cost of
      allowing otherwise-impossible batches. This field is optional and
      defaults to `true`.

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec insert_all(t(), Enumerable.t()) :: non_neg_integer()
  @spec insert_all(t(), Enumerable.t(), keyword()) :: non_neg_integer()
  def insert_all(gen_batcher, items, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, @default_timeout)
    {partitions, partitioner, blocking_flush?} = fetch_meta(gen_batcher)
    {blocking_flush?, opts} = Keyword.pop(opts, :blocking_flush?, blocking_flush?)

    insert_all_fun =
      opts
      |> Keyword.get(:safe?, true)
      |> insert_all_function(blocking_flush?, items, timeout)

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

    * `:timeout` - A `t:timeout/0` (in ms) for this operation. For more
      information, see [Operation Timeouts](README.md#operation-timeouts). This
      field is optional and defaults to `5000`.
  """
  @spec partition_info(t(), partition_key()) :: partition_info()
  @spec partition_info(t(), partition_key(), keyword()) :: partition_info()
  def partition_info(gen_batcher, partition_key, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case fetch_meta(gen_batcher) do
      {1, _, _} ->
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
    blocking_flush? = Keyword.fetch!(opts, :blocking_flush?)
    partitioner = build_partitioner(partitions)

    opts
    |> Keyword.fetch!(:name)
    |> gen_batcher_key()
    |> :persistent_term.put({partitions, partitioner, blocking_flush?})
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

  defp flush_function(true, false, timeout), do: &Partition.flush_async(&1, timeout)
  defp flush_function(true, true, timeout), do: &Partition.flush_async_block(&1, timeout)
  defp flush_function(false, _, timeout), do: &Partition.flush_sync(&1, timeout)

  defp insert_all_function(true, false, items, timeout) do
    &Partition.insert_all_safe(&1, items, timeout)
  end

  defp insert_all_function(true, true, items, timeout) do
    &Partition.insert_all_safe_block(&1, items, timeout)
  end

  defp insert_all_function(false, _, items, timeout) do
    &Partition.insert_all_unsafe(&1, items, timeout)
  end

  defp partition_name(gen_batcher, partition_key) do
    {:via, PartitionSupervisor, {gen_batcher, partition_key}}
  end
end

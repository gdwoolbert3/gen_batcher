defmodule GenBatcher.TestBatcher do
  @moduledoc """
  A `GenBatcher` implementation designed for use in tests.

  This module implements all of the `GenBatcher` callbacks in a way that mimics
  a `{:size, 3}` flush trigger.

  The flush operation sends a message to the pid stored under the `:flush_meta`
  key. For more information on the shape of the message, see `flush/3`.
  """

  use GenBatcher

  @max_size 3

  ################################
  # Public API
  ################################

  @doc """
  Starts the `TestBatcher` process.
  """
  @spec start_link :: Supervisor.on_start()
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    GenBatcher.start_link(__MODULE__, opts)
  end

  @doc """
  Sends a message to the given destination.

  The message shape is `{items, info, deferred?, partition, flusher}`:

    * `items` - the given `t:list/0` of items.

    * `info` - the given `t:GenBatcher.partition_info/0`.

    * `deferred?` - a `t:boolean/0` denoting whether or not the current flush
      was deferred.

    * `partition` - the `t:pid/0` of the flushing partition.

    * `flusher` - the `t:pid/0` of the flushing process.
  """
  @spec flush(list(), GenBatcher.partition_info(), pid()) :: :ok
  def flush(items, info, destination) do
    flusher = self()
    partition = partition_pid(flusher)
    deferred? = deferred_flush?(info, partition)
    send(destination, {items, info, deferred?, partition, flusher})
    :ok
  end

  ################################
  # GenBatcher Callbacks
  ################################

  @doc false
  @impl GenBatcher
  @spec handle_flush(list(), GenBatcher.partition_info()) :: :ok
  def handle_flush(items, info) do
    flush(items, info, info.flush_meta)
  end

  @doc false
  @impl GenBatcher
  @spec handle_insert(term(), non_neg_integer()) :: {:cont, non_neg_integer()} | :flush
  def handle_insert(_, 1), do: :flush
  def handle_insert(_, acc), do: {:cont, acc - 1}

  @doc false
  @impl GenBatcher
  @spec initial_acc :: non_neg_integer()
  def initial_acc, do: @max_size

  ################################
  # Private API
  ################################

  defp partition_pid(flusher) do
    case Process.get(:"$callers") do
      [partition | _] -> partition
      _ -> flusher
    end
  end

  defp deferred_flush?(info, partition) do
    last_deferred_flush(partition) == info.flush_ref
  end

  defp last_deferred_flush(partition) when partition == self() do
    Process.get(:last_deferred_flush)
  end

  defp last_deferred_flush(partition) do
    {:status, _, _, [pdict | _]} = :sys.get_status(partition, :infinity)
    Keyword.get(pdict, :last_deferred_flush)
  catch
    :exit, _ -> nil
  end
end

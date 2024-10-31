defmodule GenBatcher.TestBatcher do
  @moduledoc """
  TODO(Gordon) - Add this
  """

  use GenBatcher

  @max_byte_size 9

  ################################
  # Public API
  ################################

  @doc """
  TODO(Gordon) - Add this
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    GenBatcher.start_link(__MODULE__, opts)
  end

  ################################
  # GenBatcher Callbacks
  ################################

  @doc false
  @impl GenBatcher
  @spec handle_flush([binary()], GenBatcher.partition_info()) :: :ok
  def handle_flush(items, info) do
    flusher = self()
    partition = partition_pid(flusher)
    send(info.flush_meta, {items, info, partition, flusher})
    :ok
  end

  @doc false
  @impl GenBatcher
  @spec handle_insert(binary(), non_neg_integer()) :: {:cont, non_neg_integer()} | :flush
  def handle_insert(item, acc) do
    size = acc + byte_size(item)
    if size >= @max_byte_size, do: :flush, else: {:cont, size}
  end

  @doc false
  @impl GenBatcher
  @spec initial_acc :: non_neg_integer()
  def initial_acc, do: 0

  ################################
  # Private API
  ################################

  defp partition_pid(flusher) do
    case Process.get(:"$callers") do
      [partition | _] -> partition
      _ -> flusher
    end
  end
end

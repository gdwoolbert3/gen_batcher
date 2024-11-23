defmodule GenBatcher.TestBatcher do
  @moduledoc """
  TODO(Gordon) - Add this
  """

  use GenBatcher

  @max_size 3

  ################################
  # Public API
  ################################

  @doc """
  TODO(Gordon) - Add this
  """
  @spec start_link :: Supervisor.on_start()
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    GenBatcher.start_link(__MODULE__, opts)
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec flush(list(), GenBatcher.partition_info(), pid()) :: :ok
  def flush(items, info, destination) do
    flusher = self()
    partition = partition_pid(flusher)
    deferred? = deferred_flush?(partition)
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

  defp deferred_flush?(partition) when partition == self() do
    not is_nil(Process.get(:last_deferred_flush))
  end

  defp deferred_flush?(partition) do
    {:status, _, _, [pdict | _]} = :sys.get_status(partition, :infinity)
    Keyword.has_key?(pdict, :last_deferred_flush)
  catch
    :exit, _ -> false
  end
end

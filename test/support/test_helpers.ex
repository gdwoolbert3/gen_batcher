defmodule GenBatcher.TestHelpers do
  @moduledoc """
  TODO(Gordon) - Add this
  """

  import ExUnit.Callbacks, only: [start_supervised: 1]

  ################################
  # Public API
  ################################

  @doc """
  TODO(Gordon) - Add this
  """
  @spec empty?(GenBatcher.t()) :: boolean()
  def empty?(gen_batcher) do
    gen_batcher
    |> GenBatcher.info()
    |> Enum.all?(&(&1.batch_size == 0))
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec partition_empty?(GenBatcher.t(), GenBatcher.partition_key()) :: boolean()
  def partition_empty?(gen_batcher, partition_key) do
    partition_size(gen_batcher, partition_key) == 0
  end

  @doc """
  TODO(Gordon) - add this
  """
  @spec partition_size(GenBatcher.t(), GenBatcher.partition_key()) :: non_neg_integer()
  def partition_size(gen_batcher, partition_key) do
    info = GenBatcher.partition_info(gen_batcher, partition_key)
    info.batch_size
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec seed_gen_batcher(GenBatcher.t()) :: :ok
  def seed_gen_batcher(gen_batcher) do
    Enum.each(["foo", "bar", "baz"], &GenBatcher.insert(gen_batcher, &1))
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec start_and_seed_gen_batcher :: {:ok, GenBatcher.t()} | {:error, term()}
  @spec start_and_seed_gen_batcher(keyword()) :: {:ok, GenBatcher.t()} | {:error, term()}
  def start_and_seed_gen_batcher(opts \\ []) do
    with {:ok, gen_batcher} <- start_gen_batcher(opts) do
      seed_gen_batcher(gen_batcher)
      {:ok, gen_batcher}
    end
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec start_gen_batcher :: {:ok, GenBatcher.t()} | {:error, term()}
  @spec start_gen_batcher(keyword()) :: {:ok, GenBatcher.t()} | {:error, term()}
  def start_gen_batcher(opts \\ []) do
    opts = Keyword.put_new_lazy(opts, :handle_flush, &flush_callback/0)
    do_start_gen_batcher({GenBatcher, opts})
  end

  ################################
  # Private API
  ################################

  defp flush_callback do
    destination = self()

    fn items, info ->
      flusher = self()
      partition = partition_pid(flusher)
      send(destination, {items, info, partition, flusher})
    end
  end

  defp partition_pid(flusher) do
    case Process.get(:"$callers") do
      [partition | _] -> partition
      _ -> flusher
    end
  end

  defp do_start_gen_batcher(child_spec) do
    case start_supervised(child_spec) do
      {:ok, pid} -> process_name(pid)
      {:error, {{_, {_, _, reason}}, _}} -> {:error, reason}
      {:error, {reason, _}} -> {:error, reason}
    end
  end

  defp process_name(pid) do
    # TODO(Gordon) - handle case where process doesn't have a registered name
    # Access.get/2 maybe?
    pid
    |> Process.info()
    |> case do
      nil -> {:error, :not_found}
      info -> {:ok, Keyword.get(info, :registered_name)}
    end
  end
end

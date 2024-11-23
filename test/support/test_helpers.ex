defmodule GenBatcher.TestHelpers do
  @moduledoc """
  TODO(Gordon) - Add this
  """

  import ExUnit.Callbacks, only: [start_supervised: 1]

  alias GenBatcher.TestBatcher

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
    opts = Keyword.put(opts, :flush_meta, self())

    case start_supervised({TestBatcher, opts}) do
      {:ok, pid} -> process_name(pid)
      {:error, {{_, {_, _, reason}}, _}} -> {:error, reason}
      {:error, {reason, _}} -> {:error, reason}
    end
  end

  ################################
  # Private API
  ################################

  defp process_name(pid) when is_pid(pid) do
    pid
    |> Process.info()
    |> case do
      nil -> {:error, :not_found}
      info -> process_name(info)
    end
  end

  defp process_name(info) do
    case Keyword.get(info, :registered_name) do
      nil -> {:error, :no_name}
      name -> {:ok, name}
    end
  end
end

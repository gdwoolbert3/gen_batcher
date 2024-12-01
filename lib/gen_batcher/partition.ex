defmodule GenBatcher.Partition do
  @moduledoc false

  use GenServer

  alias GenBatcher.Partition.Info
  alias GenBatcher.Partition.State

  ################################
  # Public API
  ################################

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {start_link_opts, init_arg} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_arg, start_link_opts)
  end

  @doc false
  @spec dump(GenServer.server(), timeout()) :: list()
  def dump(partition, timeout) do
    GenServer.call(partition, :dump, timeout)
  end

  @doc false
  @spec flush_async(GenServer.server(), timeout()) :: :ok
  def flush_async(partition, timeout) do
    GenServer.call(partition, :flush_async, timeout)
  end

  @doc false
  @spec flush_sync(GenServer.server(), timeout()) :: :ok
  def flush_sync(partition, timeout) do
    with {:ok, pid} <- GenServer.call(partition, :flush_sync, timeout) do
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      end
    end
  end

  @doc false
  @spec info(GenServer.server(), timeout()) :: Info.t()
  def info(partition, timeout) do
    GenServer.call(partition, :info, timeout)
  end

  @doc false
  @spec insert(GenServer.server(), term(), timeout()) :: :ok
  def insert(partition, item, timeout) do
    GenServer.call(partition, {:insert, item}, timeout)
  end

  @doc false
  @spec insert_all_safe(GenServer.server(), Enumerable.t(), timeout()) :: non_neg_integer()
  def insert_all_safe(partition, items, timeout) do
    GenServer.call(partition, {:insert_all_safe, items}, timeout)
  end

  @doc false
  @spec insert_all_unsafe(GenServer.server(), Enumerable.t(), timeout()) :: non_neg_integer()
  def insert_all_unsafe(partition, items, timeout) do
    GenServer.call(partition, {:insert_all_unsafe, items}, timeout)
  end

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, State.t(), {:continue, :refresh}}
  def init(opts) do
    Process.flag(:trap_exit, true)
    state = struct!(State, opts)
    {:ok, state, {:continue, :refresh}}
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
          | {:reply, term(), State.t(), {:continue, :flush | :refresh}}
  def handle_call(:dump, _from, state) do
    items = items(state)
    {:reply, items, state, {:continue, :refresh}}
  end

  def handle_call(:flush_async, _from, state) do
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call(:flush_sync, _from, state) do
    result = do_flush(state)
    {:reply, result, state, {:continue, :refresh}}
  end

  def handle_call(:info, _from, state) do
    info = do_info(state)
    {:reply, info, state}
  end

  def handle_call({:insert, item}, _from, state) do
    case do_insert(state, item) do
      {:cont, state} -> {:reply, :ok, state}
      {:flush, state} -> {:reply, :ok, state, {:continue, :flush}}
    end
  end

  def handle_call({:insert_all_safe, items}, _from, state) do
    {count, result} =
      Enum.reduce(items, {0, {:cont, state}}, fn
        item, {count, {:cont, state}} ->
          {count + 1, do_insert(state, item)}

        item, {count, {:flush, state}} ->
          do_flush(state)
          state = do_refresh(state)
          {count + 1, do_insert(state, item)}
      end)

    case result do
      {:cont, state} -> {:reply, count, state}
      {:flush, state} -> {:reply, count, state, {:continue, :flush}}
    end
  end

  def handle_call({:insert_all_unsafe, items}, _from, state) do
    # This function purposely avoids the `do_insert/2` helper since checking
    # flush conditions is unnecessary once the conditions have been reached.
    {count, items, result} =
      Enum.reduce(items, {0, state.items, {:cont, state.acc}}, fn
        item, {count, items, {:cont, acc}} ->
          {count + 1, [item | items], state.handle_insert.(item, acc)}

        item, {count, items, :flush} ->
          {count + 1, [item | items], :flush}
      end)

    case result do
      {:cont, acc} ->
        state = %{state | size: state.size + count, items: items, acc: acc}
        {:reply, count, state}

      :flush ->
        state = %{state | size: state.size + count, items: items}
        {:reply, count, state, {:continue, :flush}}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_continue(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :refresh}}
  def handle_continue(:flush, state) do
    # Stores the flush ref of the last deferred flush. This is done exclusively
    # for testing, hence the usage of the process dictionary.
    Process.put(:last_deferred_flush, state.flush_ref)
    do_flush(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state) do
    state = do_refresh(state)
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  @spec handle_info(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, state) when timer == state.timer do
    {:noreply, state, {:continue, :flush}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @doc false
  @impl GenServer
  @spec terminate(term(), State.t()) :: :ok
  def terminate(_, %State{items: [], flush_empty?: false}), do: :ok
  def terminate(_, state), do: do_blocking_flush(state)

  ################################
  # Private API
  ################################

  defp do_refresh(state) do
    refresh = %{
      timer: refresh_timer(state),
      acc: refresh_acc(state),
      flush_ref: make_ref(),
      batch_start: now(),
      size: 0,
      items: []
    }

    Map.merge(state, refresh)
  end

  defp refresh_timer(%State{batch_timeout: :infinity}), do: nil
  defp refresh_timer(state) when is_nil(state.timer), do: schedule_next_flush(state)

  defp refresh_timer(state) do
    Process.cancel_timer(state.timer)
    schedule_next_flush(state)
  end

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message.
    # This is important for handling race conditions from near-simultaneous
    # flush triggers.
    :erlang.start_timer(state.batch_timeout, self(), :flush)
  end

  defp refresh_acc(state) do
    case state.initial_acc do
      {:static, acc} -> acc
      {:dynamic, fun} -> fun.()
    end
  end

  defp do_insert(state, item) do
    case state.handle_insert.(item, state.acc) do
      {:cont, acc} ->
        state = %{state | items: [item | state.items], size: state.size + 1, acc: acc}
        {:cont, state}

      :flush ->
        state = %{state | items: [item | state.items], size: state.size + 1}
        {:flush, state}
    end
  end

  defp do_flush(%State{items: [], flush_empty?: false}), do: :ok

  defp do_flush(state) when not state.blocking_flush? do
    do_non_blocking_flush(state)
  end

  defp do_flush(state), do: do_blocking_flush(state)

  defp do_non_blocking_flush(state) do
    fun = fn ->
      Process.flag(:trap_exit, true)
      do_blocking_flush(state)
    end

    opts = [shutdown: state.shutdown]
    Task.Supervisor.start_child(GenBatcher.TaskSupervisor, fun, opts)
  end

  defp do_blocking_flush(state) do
    items = items(state)
    info = do_info(state)
    state.handle_flush.(items, info)
    :ok
  end

  defp do_info(state) do
    %Info{
      batch_duration: now() - state.batch_start,
      flush_meta: state.flush_meta,
      flush_ref: state.flush_ref,
      partition: state.partition,
      batch_size: state.size
    }
  end

  defp items(state), do: Enum.reverse(state.items)

  defp now, do: System.monotonic_time(:millisecond)
end

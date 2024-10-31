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
  @spec flush_async_block(GenServer.server(), timeout()) :: :ok
  def flush_async_block(partition, timeout) do
    GenServer.call(partition, :flush_async_block, timeout)
  end

  @doc false
  @spec flush_sync(GenServer.server(), timeout()) :: :ok
  def flush_sync(partition, timeout) do
    GenServer.call(partition, :flush_sync, timeout)
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
  @spec insert_all_safe_block(GenServer.server(), Enumerable.t(), timeout()) :: non_neg_integer()
  def insert_all_safe_block(partition, items, timeout) do
    GenServer.call(partition, {:insert_all_safe_block, items}, timeout)
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
          | {:reply, term(), State.t(), {:continue, :flush_async | :refresh}}
  def handle_call(:dump, _from, state) do
    items = items(state)
    {:reply, items, state, {:continue, :refresh}}
  end

  def handle_call(:flush_async, _from, state) do
    {:reply, :ok, state, {:continue, :flush_async}}
  end

  def handle_call(:flush_async_block, _from, state) do
    {:reply, :ok, state, {:continue, :flush_sync}}
  end

  def handle_call(:flush_sync, _from, state) do
    do_flush_sync(state)
    {:reply, :ok, state, {:continue, :refresh}}
  end

  def handle_call(:info, _from, state) do
    info = do_info(state)
    {:reply, info, state}
  end

  def handle_call({:insert, item}, _from, state) do
    case {do_insert(state, item), state.blocking_flush?} do
      {{:cont, state}, _} -> {:reply, :ok, state}
      {{:flush, state}, false} -> {:reply, :ok, state, {:continue, :flush_async}}
      {{:flush, state}, true} -> {:reply, :ok, state, {:continue, :flush_sync}}
    end
  end

  def handle_call({:insert_all_safe, items}, _from, state) do
    {count, result} = do_insert_all_safe(state, items, &do_flush_async/1)

    case {result, state.blocking_flush?} do
      {{:cont, state}, _} -> {:reply, count, state}
      {{:flush, state}, false} -> {:reply, count, state, {:continue, :flush_async}}
      {{:flush, state}, true} -> {:reply, count, state, {:continue, :flush_sync}}
    end
  end

  def handle_call({:insert_all_safe_block, items}, _from, state) do
    {count, result} = do_insert_all_safe(state, items, &do_flush_sync/1)

    case {result, state.blocking_flush?} do
      {{:cont, state}, _} -> {:reply, count, state}
      {{:flush, state}, false} -> {:reply, count, state, {:continue, :flush_async}}
      {{:flush, state}, true} -> {:reply, count, state, {:continue, :flush_sync}}
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

    case {result, state.blocking_flush?} do
      {{:cont, acc}, _} ->
        state = %{state | size: state.size + count, items: items, acc: acc}
        {:reply, count, state}

      {:flush, false} ->
        state = %{state | size: state.size + count, items: items}
        {:reply, count, state, {:continue, :flush_async}}

      {:flush, true} ->
        state = %{state | size: state.size + count, items: items}
        {:reply, count, state, {:continue, :flush_sync}}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_continue(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :refresh}}
  def handle_continue(:flush_async, state) do
    do_flush_async(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:flush_sync, state) do
    do_flush_sync(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state) do
    state = do_refresh(state)
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  @spec handle_info(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :flush_async}}
  def handle_info({:timeout, timer, :flush}, state) when timer == state.timer do
    if state.blocking_flush? do
      {:noreply, state, {:continue, :flush_sync}}
    else
      {:noreply, state, {:continue, :flush_async}}
    end
  end

  def handle_info(_, state), do: {:noreply, state}

  @doc false
  @impl GenServer
  @spec terminate(term(), State.t()) :: term()
  def terminate(_, state), do: do_flush_sync(state)

  ################################
  # Private API
  ################################

  defp do_refresh(state) do
    refresh = %{
      timer: refresh_timer(state),
      acc: refresh_acc(state),
      batch_start: now(),
      size: 0,
      items: []
    }

    Map.merge(state, refresh)
  end

  defp refresh_timer(%State{batch_timeout: :infinity}), do: nil
  defp refresh_timer(%State{timer: nil} = state), do: schedule_next_flush(state)

  defp refresh_timer(state) do
    Process.cancel_timer(state.timer)
    schedule_next_flush(state)
  end

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message.
    # This is important for handling race conditions from near-simultaneous
    # flush conditions.
    :erlang.start_timer(state.batch_timeout, self(), :flush)
  end

  defp refresh_acc(state) do
    case state.initial_acc do
      {:static, acc} -> acc
      {:dynamic, fun} -> fun.()
    end
  end

  defp do_insert_all_safe(state, items, flush_callback) do
    Enum.reduce(items, {0, {:cont, state}}, fn
      item, {count, {:cont, state}} ->
        {count + 1, do_insert(state, item)}

      item, {count, {:flush, state}} ->
        flush_callback.(state)
        state = do_refresh(state)
        {count + 1, do_insert(state, item)}
    end)
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

  defp do_flush_async(%State{items: [], flush_empty?: false}), do: :ok

  defp do_flush_async(state) do
    fun = fn ->
      Process.flag(:trap_exit, true)
      do_flush_sync(state)
    end

    Task.Supervisor.start_child(GenBatcher.TaskSupervisor, fun, shutdown: state.shutdown)
    :ok
  end

  defp do_flush_sync(%State{items: [], flush_empty?: false}), do: :ok

  defp do_flush_sync(state) do
    items = items(state)
    info = do_info(state)
    state.handle_flush.(items, info)
    :ok
  end

  defp do_info(state) do
    %Info{
      batch_duration: now() - state.batch_start,
      batch_timer: next_flush(state),
      flush_meta: state.flush_meta,
      partition: state.partition,
      batch_size: state.size
    }
  end

  defp items(state), do: Enum.reverse(state.items)

  defp now, do: System.monotonic_time(:millisecond)

  defp next_flush(%{timer: nil}), do: nil

  defp next_flush(state) do
    with false <- Process.read_timer(state.timer), do: 0
  end
end

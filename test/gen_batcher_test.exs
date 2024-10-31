defmodule GenBatcherTest do
  use ExUnit.Case, async: true
  # TODO(Gordon) - doctest GenBatcher

  import GenBatcher.TestHelpers

  alias GenBatcher.Partition.Info
  alias GenBatcher.TestBatcher

  # setup %{test_type: test_type} do
  #   if test_type == :doctest do
  #     opts = [flush_callback: fn _, _ -> :ok end, partitions: 2]
  #     if start_ex_buffer(opts) == {:ok, ExBuffer}, do: :ok
  #   else
  #     :ok
  #   end
  # end

  # TODO(Gordon) - document general test workflow (ie sending test process a message from flusher)
  # :erlang.process_info(self(), :messages)

  describe "start_link/2" do
    test "will start a GenBatcher" do
      assert start_gen_batcher() == {:ok, GenBatcher}
    end

    test "will start a partitioned GenBatcher" do
      assert start_gen_batcher(partitions: 2) == {:ok, GenBatcher}
    end

    test "will start a GenBatcher with the provided name" do
      assert start_gen_batcher(name: :gen_batcher) == {:ok, :gen_batcher}
    end

    test "will flush a GenBatcher when conditions are met" do
      opts = [flush_trigger: {:size, 2}, partitions: 2]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)
      assert_receive {["foo", "baz"], %Info{}, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      refute partition == flusher

      # Ensures that partitions flush independently
      assert partition_size(gen_batcher, 1) == 1
    end

    test "will start a GenBatcher with a static custom flush trigger" do
      handle_insert = fn item, acc ->
        size = acc + byte_size(item)
        if size >= 9, do: :flush, else: {:cont, size}
      end

      opts = [flush_trigger: {:static_custom, 0, handle_insert}]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)
      assert_receive {["foo", "bar", "baz"], %Info{}, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      refute partition == flusher
    end

    test "will start a GenBatcher with a dynamic custom trigger" do
      handle_insert = fn
        _, 1 -> :flush
        _, acc -> {:cont, acc - 1}
      end

      # Creates a deterministic function to generate an initial acc
      atomics_ref = :atomics.new(1, signed: false)
      initial_acc = fn -> :atomics.add_get(atomics_ref, 1, 1) end
      opts = [flush_trigger: {:dynamic_custom, initial_acc, handle_insert}]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)
      assert_receive {["foo"], %Info{}, partition, flusher}
      refute partition == flusher
      assert_receive {["bar", "baz"], %Info{}, partition, flusher}
      refute partition == flusher
      assert partition_empty?(gen_batcher, 0)
    end

    test "will start a GenBatcher from an implementation module" do
      assert {:ok, _} = start_supervised({TestBatcher, flush_meta: self()})

      seed_gen_batcher(TestBatcher)

      assert_receive {["foo", "bar", "baz"], %Info{}, partition, flusher}
      refute partition == flusher
    end

    test "will trigger a flush when batch timeout is exceeded" do
      opts = [batch_timeout: 50, flush_empty?: true]

      assert {:ok, gen_batcher} = start_gen_batcher(opts)
      assert_receive {[], %Info{}, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      refute partition == flusher
    end

    test "will trigger a blocking flush" do
      opts = [flush_trigger: {:size, 3}, blocking_flush?: true]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)
      assert_receive {["foo", "bar", "baz"], %Info{}, partition, partition}
      assert partition_empty?(gen_batcher, 0)
    end

    test "will not flush an empty batch" do
      assert start_gen_batcher(batch_timeout: 50) == {:ok, GenBatcher}
      refute_receive _, 150
    end

    test "will flush a GenBatcher on termination" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()

      GenServer.stop(gen_batcher)

      assert_received {["foo", "bar", "baz"], %Info{}, partition, partition}
    end

    test "will flush a partitioned GenBatcher on termination" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)

      GenServer.stop(gen_batcher)

      assert_received {["foo", "baz"], _, partition, partition}
      assert_received {["bar"], _, partition, partition}
    end
  end

  describe "dump/2" do
    test "will dump a GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.dump(gen_batcher) == [["foo", "bar", "baz"]]
      assert empty?(gen_batcher)
    end

    test "will dump a partitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.dump(gen_batcher) == [["foo", "baz"], ["bar"]]
      assert empty?(gen_batcher)
    end
  end

  describe "dump_partition/3" do
    test "will dump an unpartitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.dump_partition(gen_batcher, 0) == ["foo", "bar", "baz"]
      assert partition_empty?(gen_batcher, 0)
    end

    test "will dump a partitioned GenBatcher's partition" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.dump_partition(gen_batcher, 0) == ["foo", "baz"]
      assert partition_empty?(gen_batcher, 0)
      assert partition_size(gen_batcher, 1) == 1
    end
  end

  describe "flush/2" do
    test "will synchronously flush a GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.flush(gen_batcher, async?: false) == :ok
      assert_received {["foo", "bar", "baz"], _, partition, partition}
      assert empty?(gen_batcher)
    end

    test "will synchronously flush a partitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush(gen_batcher, async?: false) == :ok
      assert_received {["foo", "baz"], _, partition, partition}
      assert_received {["bar"], _, partition, partition}
      assert empty?(gen_batcher)
    end

    test "will synchronously flush a partitioned GenBatcher in parallel" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush(gen_batcher, async?: false, max_concurrency: 2) == :ok
      assert_received {["foo", "baz"], _, partition, partition}
      assert_received {["bar"], _, partition, partition}
      assert empty?(gen_batcher)
    end

    test "will asynchronously flush a GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.flush(gen_batcher) == :ok
      assert_receive {["foo", "bar", "baz"], _, partition, flusher}
      refute partition == flusher
      assert empty?(gen_batcher)
    end

    test "will asynchronously flush a partitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush(gen_batcher) == :ok
      assert_receive {["foo", "baz"], _, partition, flusher}
      refute partition == flusher
      assert_receive {["bar"], _, partition, flusher}
      refute partition == flusher
      assert empty?(gen_batcher)
    end

    test "will asynchronously flush a GenBatcher in a blocking manner" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.flush(gen_batcher, blocking?: true) == :ok
      assert_receive {["foo", "bar", "baz"], _, partition, partition}
      assert empty?(gen_batcher)
    end

    test "will asynchronously flush a partitioned GenBatcher in a blocking manner" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush(gen_batcher, blocking?: true) == :ok
      assert_receive {["foo", "baz"], _, partition, partition}
      assert_receive {["bar"], _, partition, partition}
      assert empty?(gen_batcher)
    end
  end

  describe "flush_partition/3" do
    test "will synchronously flush an unpartitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.flush_partition(gen_batcher, 0, async?: false) == :ok
      assert_received {["foo", "bar", "baz"], _, partition, partition}
      assert partition_empty?(gen_batcher, 0)
    end

    test "will synchronously flush a partitioned GenBatcher's partition" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush_partition(gen_batcher, 0, async?: false) == :ok
      assert_received {["foo", "baz"], _, partition, partition}
      assert partition_empty?(gen_batcher, 0)
      assert partition_size(gen_batcher, 1) == 1
    end

    test "will asynchronously flush an unpartitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.flush_partition(gen_batcher, 0) == :ok
      assert_receive {["foo", "bar", "baz"], _, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      refute partition == flusher
    end

    test "will asynchronously flush a partitioned GenBatcher's partition" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush_partition(gen_batcher, 0) == :ok
      assert_receive {["foo", "baz"], _, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      assert partition_size(gen_batcher, 1) == 1
      refute partition == flusher
    end

    test "will asynchronously flush an unpartitioned GenBatcher in a blocking manner" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher()
      assert GenBatcher.flush_partition(gen_batcher, 0, blocking?: true) == :ok
      assert_receive {["foo", "bar", "baz"], _, partition, partition}
      assert partition_empty?(gen_batcher, 0)
    end

    test "will asynchronously flush a partitioned GenBatcher's partition in a blocking manner" do
      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(partitions: 2)
      assert GenBatcher.flush_partition(gen_batcher, 0, blocking?: true) == :ok
      assert_receive {["foo", "baz"], _, partition, partition}
      assert partition_empty?(gen_batcher, 0)
      assert partition_size(gen_batcher, 1) == 1
    end
  end

  describe "info/2" do
    test "will return information about a GenBatcher" do
      opts = [batch_timeout: 1_000, flush_meta: "meta"]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)
      assert [%Info{} = info] = GenBatcher.info(gen_batcher)
      assert info.flush_meta == "meta"
      assert info.batch_timer <= 1_000
      assert info.batch_duration >= 0
      assert info.batch_size == 3
      assert info.partition == 0
    end

    test "will return information about a partitioned GenBatcher" do
      opts = [batch_timeout: 1_000, flush_meta: "meta", partitions: 2]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)

      gen_batcher
      |> GenBatcher.info()
      |> Enum.zip([2, 1])
      |> Enum.with_index(fn {info, size}, part -> {info, size, part} end)
      |> Enum.each(fn {info, size, partition} ->
        assert %Info{} = info
        assert info.partition == partition
        assert info.flush_meta == "meta"
        assert info.batch_timer <= 1_000
        assert info.batch_duration >= 0
        assert info.batch_size == size
      end)
    end
  end

  describe "insert/3" do
    test "will insert an item into a GenBatcher" do
      assert {:ok, gen_batcher} = start_gen_batcher()
      assert GenBatcher.insert(gen_batcher, "foo") == :ok
      assert partition_size(gen_batcher, 0) == 1
    end

    test "will insert an item into a partitioned GenBatcher" do
      assert {:ok, gen_batcher} = start_gen_batcher(partitions: 2)
      assert GenBatcher.insert(gen_batcher, "foo") == :ok
      assert partition_size(gen_batcher, 0) == 1
      assert partition_empty?(gen_batcher, 1)

      # Ensure partitioner works as expected
      assert GenBatcher.insert(gen_batcher, "bar") == :ok
      assert partition_size(gen_batcher, 0) == 1
      assert partition_size(gen_batcher, 1) == 1
    end

    test "will insert an item into a specific GenBatcher partition" do
      assert {:ok, gen_batcher} = start_gen_batcher(partitions: 2)
      assert GenBatcher.insert(gen_batcher, "foo", partition_key: 1) == :ok
      assert partition_empty?(gen_batcher, 0)
      assert partition_size(gen_batcher, 1) == 1

      # Ensure passing partition key doesn't increment partitioner
      assert GenBatcher.insert(gen_batcher, "bar") == :ok
      assert partition_size(gen_batcher, 0) == 1
      assert partition_size(gen_batcher, 1) == 1
    end
  end

  describe "insert_all/3" do
    test "will insert items into a GenBatcher" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher()
      assert GenBatcher.insert_all(gen_batcher, items) == 3
      assert partition_size(gen_batcher, 0) == 3
    end

    test "will insert items into a partitioned GenBatcher" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher(partitions: 2)
      assert GenBatcher.insert_all(gen_batcher, items) == 3
      assert partition_size(gen_batcher, 0) == 3
      assert partition_empty?(gen_batcher, 1)

      # Ensure partitioner works as expected
      assert GenBatcher.insert_all(gen_batcher, items) == 3
      assert partition_size(gen_batcher, 0) == 3
      assert partition_size(gen_batcher, 1) == 3
    end

    test "will insert items into a specific GenBatcher partition" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher(partitions: 2)
      assert GenBatcher.insert_all(gen_batcher, items, partition_key: 1) == 3
      assert partition_empty?(gen_batcher, 0)
      assert partition_size(gen_batcher, 1) == 3

      # Ensure passing partition key doesn't increment partitioner
      assert GenBatcher.insert_all(gen_batcher, items) == 3
      assert partition_size(gen_batcher, 0) == 3
      assert partition_size(gen_batcher, 1) == 3
    end

    test "will trigger a flush if conditions are met while inserting items" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher(flush_trigger: {:size, 2})
      assert GenBatcher.insert_all(gen_batcher, items) == 3
      assert_receive {["foo", "bar"], _, partition, flusher}
      assert partition_size(gen_batcher, 0) == 1
      refute partition == flusher
    end

    test "will trigger a blocking flush if conditions are met while inserting items" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher(flush_trigger: {:size, 2})
      assert GenBatcher.insert_all(gen_batcher, items, blocking_flush?: true) == 3
      assert_received {["foo", "bar"], _, partition, partition}
      assert partition_size(gen_batcher, 0) == 1
    end

    test "will trigger an asynchronous flush if conditions are met after inserting items" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher(flush_trigger: {:size, 3})

      # More specifically, ensure that the GenBatcher's flush configuration is
      # respected when the last item in the given enumerable triggers a flush
      assert GenBatcher.insert_all(gen_batcher, items, blocking_flush?: true) == 3
      assert_receive {["foo", "bar", "baz"], _, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      refute partition == flusher
    end

    test "will not trigger a flush if conditions are met while inserting items" do
      items = ["foo", "bar", "baz"]

      assert {:ok, gen_batcher} = start_gen_batcher(flush_trigger: {:size, 2})
      assert GenBatcher.insert_all(gen_batcher, items, safe?: false) == 3
      assert_receive {["foo", "bar", "baz"], _, partition, flusher}
      assert partition_empty?(gen_batcher, 0)
      refute partition == flusher
    end
  end

  describe "partition_info/3" do
    test "will return information about an unpartitioned GenBatcher" do
      opts = [batch_timeout: 500, flush_meta: "meta"]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)

      info = GenBatcher.partition_info(gen_batcher, 0)

      assert %Info{} = info
      assert info.flush_meta == "meta"
      assert info.batch_duration >= 0
      assert info.batch_timer <= 500
      assert info.batch_size == 3
      assert info.partition == 0
    end

    test "will return information about a partitioned GenBatcher's partition" do
      opts = [batch_timeout: 500, flush_meta: "meta", partitions: 2]

      assert {:ok, gen_batcher} = start_and_seed_gen_batcher(opts)

      info = GenBatcher.partition_info(gen_batcher, 0)

      assert %Info{} = info
      assert info.flush_meta == "meta"
      assert info.batch_duration >= 0
      assert info.batch_timer <= 500
      assert info.batch_size == 2
      assert info.partition == 0
    end
  end
end

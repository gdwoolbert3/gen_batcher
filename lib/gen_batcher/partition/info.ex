defmodule GenBatcher.Partition.Info do
  @moduledoc """
  Defines a `GenBatcher.Partition.Info` struct that is returned and used by
  various `GenBatcher` functions and callbacks.

  The following fields are public:

    * `batch_duration` - The time (in ms) that the partition's current batch has
      been accumulating items.

    * `batch_size` - The number of items in the partition's current batch.

    * `flush_meta` - The `GenBatcher` `:flush_meta`.

    * `partition` - The partition's index.
  """

  defstruct [:batch_duration, :batch_size, :flush_meta, :flush_ref, :partition]

  ################################
  # Types
  ################################

  @typedoc "Information about a `GenBatcher` partition."
  @type t :: %__MODULE__{
          batch_duration: non_neg_integer(),
          batch_size: non_neg_integer(),
          flush_meta: term(),
          partition: non_neg_integer()
        }
end

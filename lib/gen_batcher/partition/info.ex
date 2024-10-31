defmodule GenBatcher.Partition.Info do
  @moduledoc """
  Defines a `GenBatcher.Partition.Info` struct that is returned and used by
  various `GenBatcher` functions and callbacks.

  The following fields are public:

    * `batch_duration` - The time (in ms) that the partition's current batch has
      been accumulating items.

    * `batch_size` - The number of items in the partition's current batch.

    * `batch_timer` - The time (in ms) until the partition's next time-based
      flush is triggered or `nil` if the `GenBatcher` does not have a
      `batch_timeout`.

    * `flush_meta` - The `GenBatcher` `:flush_meta`.

    * `partition` - The partition's index.
  """

  defstruct [:batch_duration, :batch_size, :batch_timer, :flush_meta, :partition]

  ################################
  # Types
  ################################

  @typedoc "Information about a `GenBatcher` partition."
  @type t :: %__MODULE__{
          batch_duration: non_neg_integer(),
          batch_size: non_neg_integer(),
          batch_timer: non_neg_integer() | nil,
          flush_meta: term(),
          partition: non_neg_integer()
        }
end

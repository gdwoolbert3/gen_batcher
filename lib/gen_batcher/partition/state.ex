defmodule GenBatcher.Partition.State do
  @moduledoc false

  alias GenBatcher.Partition.Info

  defstruct [
    :acc,
    :batch_start,
    :batch_timeout,
    :flush_empty?,
    :blocking_flush?,
    :flush_meta,
    :flush_ref,
    :handle_insert,
    :handle_flush,
    :initial_acc,
    :partition,
    :shutdown,
    :timer,
    items: [],
    size: 0
  ]

  ################################
  # Types
  ################################

  @typedoc false
  @type t :: %__MODULE__{
          acc: term(),
          batch_start: integer(),
          batch_timeout: timeout(),
          flush_empty?: boolean(),
          blocking_flush?: boolean(),
          flush_meta: term(),
          handle_insert: (term(), term() -> {:cont, term()} | :flush),
          handle_flush: (list(), Info.t() -> term()),
          initial_acc: {:static, term()} | {:dynamic, (-> term())},
          items: list(),
          partition: non_neg_integer(),
          shutdown: timeout() | :brutal_kill,
          size: non_neg_integer(),
          timer: reference() | nil
        }
end

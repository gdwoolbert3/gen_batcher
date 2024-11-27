defmodule GenBatcher.Application do
  @moduledoc false

  use Application

  ################################
  # Application Callbacks
  ################################

  @doc false
  @impl Application
  @spec start(Application.start_type(), keyword()) :: Supervisor.on_start()
  def start(_type, _opts) do
    children = [
      {Task.Supervisor, name: GenBatcher.TaskSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

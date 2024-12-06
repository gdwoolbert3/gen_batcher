defmodule GenBatcher.MixProject do
  @moduledoc false

  use Mix.Project

  @version "1.0.0"

  ################################
  # Public API
  ################################

  def application do
    [
      mod: {GenBatcher.Application, []},
      extra_applications: [:logger]
    ]
  end

  def project do
    [
      aliases: aliases(),
      app: :gen_batcher,
      deps: deps(),
      description: description(),
      dialyzer: dialyzer(),
      docs: docs(),
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      name: "GenBatcher",
      package: package(),
      preferred_cli_env: preferred_cli_env(),
      start_permanent: Mix.env() == :prod,
      test_coverage: test_coverage(),
      version: @version
    ]
  end

  ################################
  # Private API
  ################################

  defp aliases do
    [
      ci: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "test --cover --export-coverage default",
        "dialyzer --format github"
      ]
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.35.1", only: :dev, runtime: false},
      {:credo, "~> 1.7.10", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.3", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.2.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    """
    A simple and lightweight batching utility for Elixir.
    """
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "dialyzer/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix]
    ]
  end

  defp docs do
    [
      extras: ["README.md", "CHANGELOG.md"],
      main: "readme",
      source_url: "https://github.com/gdwoolbert3/gen_batcher",
      authors: ["Gordon Woolbert"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      files: ["lib", "mix.exs", "README*", "LICENSE"],
      maintainers: ["Gordon Woolbert"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/gdwoolbert3/gen_batcher"}
    ]
  end

  defp preferred_cli_env do
    [
      ci: :test
    ]
  end

  defp test_coverage do
    [
      ignore_modules: [
        GenBatcher.Application,
        GenBatcher.Partition,
        GenBatcher.Partition.Info,
        GenBatcher.Partition.State,
        GenBatcher.TestBatcher,
        GenBatcher.TestHelpers
      ]
    ]
  end
end

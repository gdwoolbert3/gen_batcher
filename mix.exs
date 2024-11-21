defmodule GenBatcher.MixProject do
  @moduledoc false

  use Mix.Project

  @version "1.0.0"

  # TODO(Gordon) - Test coverage (see nimble_pool)
  # TODO(Gordon) - configure docs
  # TODO(Gordon) - configure package
  # TODO(Gordon) - relax elixir version requirement

  def project do
    [
      app: :gen_batcher,
      name: "GenBatcher",
      version: @version,
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      description: description(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {GenBatcher.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp description do
    """
    A simple and lightweight batching utility for Elixir.
    """
  end

  defp docs do
    [
      extras: ["README.md", "CHANGELOG.md"],
      main: "readme",
      source_url: "https://github.com/gdwoolbert3/gen_batcher",
      authors: ["Gordon Woolbert"]
    ]
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README*", "LICENSE"],
      maintainers: ["Gordon Woolbert"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/gdwoolbert3/gen_batcher"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:benchee, "~> 1.3.1", only: :dev, runtime: false},
      {:ex_doc, "~> 0.34.2", only: :dev, runtime: false},
      {:credo, "~> 1.7.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.3", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13.0", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.0.0", only: [:dev, :test], runtime: false}
    ]
  end
end

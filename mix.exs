defmodule Vax.MixProject do
  use Mix.Project

  def project do
    [
      app: :vax,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      test_paths: ["test", "integration_test"]
    ] ++ docs()
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.7"},
      {:antidotec_pb, github: "vaxine-io/antidote-erlang-client", branch: "master"},
      {:nimble_pool, "~> 0.2.6"}
    ]
  end

  defp docs do
    [
      name: "Vax",
      description: "Data access library for the Vaxine database platform.",
      source_url: "https://github.com/vaxine-io/vax",
      homepage_url: "https://vaxine.io"
    ]
  end

  defp package do
    [
      name: "vax",
      maintainers: ["James Arthur"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/vaxine-io/vax",
        "Vaxine" => "https://vaxine.io"
      }
    ]
  end
end

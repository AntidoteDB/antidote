defmodule Vaxine.MixProject do
  use Mix.Project

  def project do
    [
      app: :vaxine,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
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
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end

  defp docs do
    [
      name: "Vaxine",
      description: "The Vaxine database platform.",
      source_url: "https://github.com/vaxine-io/vaxine",
      homepage_url: "https://vaxine.io"
    ]
  end

  defp package do
    [
      name: "vaxine",
      maintainers: ["James Arthur"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/vaxine-io/vaxine",
        "Vaxine" => "https://vaxine.io"
      }
    ]
  end
end

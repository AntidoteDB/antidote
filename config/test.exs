import Config

config :vax, hostname: System.get_env("VAXINE_HOST", "localhost")
config :vax, port: System.get_env("VAXINE_PORT", "8087") |> String.to_integer()

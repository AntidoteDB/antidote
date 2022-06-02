import Config

if config_env() == :test, do: import_config("test.exs")

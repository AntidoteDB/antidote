defmodule Vax.Adapter do
  @moduledoc """
  Ecto adapter for Vaxine
  """

  @behaviour Ecto.Adapter

  @impl true
  def loaders(:binary_id, type), do: [type, Ecto.UUID]
  def loaders(_primitive_type, ecto_type), do: [ecto_type]

  @impl true
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_primitive_type, ecto_type), do: [ecto_type]

  @impl true
  def init(config) do
    IO.inspect(config, label: :config)
    {:ok, []}
  end

  @impl true
  def ensure_all_started(_config, _type) do
    {:ok, []}
  end

  @impl true
  def checkout(_adapter_meta, _config, function) do
    Process.put(:checked_out, true)
    result = function.()
    Process.put(:checked_out, false)

    result
  end

  @impl true
  def checked_out?(_adapter_meta) do
    Process.get(:checked_out, false)
  end

  @impl true
  defmacro __before_compile__(env) do
    env
  end
end

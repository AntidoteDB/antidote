defmodule Vax.Type do
  defmacro __using__(opts) do
    quote do
      use Ecto.Type, unquote(opts)

      @behaviour Vax.Type
    end
  end

  def crdt_type(ecto_type) do
    if base_or_composite?(ecto_type) do
      :antidote_crdt_register_lww
    else
      ecto_type.antidote_crdt_type()
    end
  end

  # TODO: ?
  def base_or_composite?(type) when is_atom(type), do: Ecto.Type.base?(type)
  def base_or_composite?(type), do: is_tuple(type)

  @callback antidote_crdt_type() :: atom()
  @callback compute_change(antidotec_type :: term(), change :: term()) :: term()
end

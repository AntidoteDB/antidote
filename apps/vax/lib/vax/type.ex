defmodule Vax.Type do
  defmacro __using__(opts) do
    quote do
      use Ecto.Type, unquote(opts)

      @behaviour Vax.Type
    end
  end

  def crdt_type(ecto_type) do
    case ecto_type do
      {:parameterized, type, param} ->
        type.antidote_crdt_type(param)

      _ ->
        if base_or_composite?(ecto_type) do
          :antidote_crdt_register_lww
        else
          ecto_type.antidote_crdt_type()
        end
    end
  end

  # TODO: ?
  def base_or_composite?({:parameterized, _, _}), do: false
  def base_or_composite?(type) when is_atom(type), do: Ecto.Type.base?(type)
  def base_or_composite?(_), do: true

  def compute_change(type, antidotec_value, new_value) do
    case type do
      {:parameterized, type, param} ->
        type.compute_change(param, antidotec_value, new_value)

      type when is_atom(type) ->
        if base_or_composite?(type) do
          {:ok, dumped_value} = Ecto.Type.adapter_dump(Vax.Adapter, type, new_value)
          :antidotec_reg.assign(antidotec_value, dumped_value)
        else
          type.compute_change(antidotec_value, new_value)
        end
    end
  end

  def client_dump(ecto_type, default \\ nil) do
    if base_or_composite?(ecto_type) do
      :antidotec_reg.new(default)
    else
      case ecto_type do
        {:parameterized, type, params} -> type.client_dump(params, default)
        type -> type.client_dump(default)
      end
    end
  end

  def client_load(value) when is_binary(value) do
    {:ok, value}
  end

  def client_load(crdt) do
    case :antidotec_datatype.module_for_term(crdt) do
      :undefined -> raise "unkown antidotec crdt: #{inspect(crdt)}"
      mod -> {:ok, mod.value(crdt)}
    end
  end

  @callback antidote_crdt_type() :: atom()
  @callback client_dump(value :: term()) :: term()
  @callback compute_change(antidotec_type :: term(), change :: term()) :: term()
end

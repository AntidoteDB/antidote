defmodule Vax.Types.Set do
  @moduledoc """
  Type for CRDT sets
  """

  alias Ecto.Type

  use Vax.ParameterizedType

  @impl Ecto.ParameterizedType
  def type(_), do: :set

  @impl Ecto.ParameterizedType
  def init(params) do
    Keyword.fetch!(params, :type)
  end

  @impl Ecto.ParameterizedType
  def load(nil, _loader, _inner_type) do
    {:ok, MapSet.new([])}
  end

  def load(value, _loader, inner_type) do
    value
    |> :antidotec_set.value()
    |> map_while_into_set(&Ecto.Type.adapter_load(Vax.Adapter, inner_type, &1))
  end

  @impl Ecto.ParameterizedType
  def dump(nil, _loader, _inner_type) do
    {:ok, MapSet.new([])}
  end

  def dump(value, dumper, inner_type) do
    map_while_into_set(value, &Type.dump(&1, dumper, inner_type))
  end

  @impl Ecto.ParameterizedType
  def cast(data, inner_type) do
    map_while_into_set(data, &Type.cast(inner_type, &1))
  end

  @impl Vax.ParameterizedType
  def compute_change(inner_type, antidotec_set, set) do
    {:ok, set} = map_while_into_set(set, &Ecto.Type.adapter_dump(Vax.Adapter, inner_type, &1))

    removes_set =
      antidotec_set
      |> :antidotec_set.value()
      |> MapSet.new()
      |> MapSet.difference(set)

    antidotec_set_with_adds = Enum.reduce(set, antidotec_set, &:antidotec_set.add/2)
    Enum.reduce(removes_set, antidotec_set_with_adds, &:antidotec_set.remove/2)
  end

  @impl Vax.ParameterizedType
  def antidote_crdt_type(_params), do: :antidote_crdt_set_aw

  @impl Vax.ParameterizedType
  def client_dump(_inner_type, nil) do
    :antidotec_set.new()
  end

  def client_dump(inner_type, enumerable) do
    {:ok, set} =
      map_while_into_set(enumerable, &Ecto.Type.adapter_dump(Vax.Adapter, inner_type, &1))

    set
    |> Enum.to_list()
    |> :antidotec_set.new()
  end

  defp map_while_into_set(enum, fun) do
    enum
    |> Enum.reduce_while([], fn el, acc ->
      case fun.(el) do
        {:ok, res} -> {:cont, [res | acc]}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      list -> {:ok, MapSet.new(list)}
    end
  end
end

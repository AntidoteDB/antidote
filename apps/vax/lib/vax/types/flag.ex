defmodule Vax.Types.Flag do
  @moduledoc """
  Type for CRDT Flag
  """

  use Vax.ParameterizedType

  alias Ecto.Changeset

  @conflict_resolution_strategies [:enable_wins, :disable_wins]

  @impl Ecto.ParameterizedType
  def type(_), do: :flag

  @impl Ecto.ParameterizedType
  def init(params) do
    case Keyword.get(params, :conflict_resolution) do
      nil ->
        :enable_wins

      strategy when strategy in @conflict_resolution_strategies ->
        strategy

      invalid ->
        raise "Invalid `conflict_resolution` option, use one of: " <>
                Enum.map_join(@conflict_resolution_strategies, ",", &"`#{inspect(&1)}`") <>
                ". Got #{inspect(invalid)}."
    end
  end

  @impl Ecto.ParameterizedType
  def load(nil, _loader, _strategy) do
    {:ok, false}
  end

  def load(value, _loader, _strategy) do
    {:ok, :antidotec_flag.value(value)}
  end

  @impl Ecto.ParameterizedType
  def dump(value, _dumper, _strategy) do
    {:ok, value}
  end

  @impl Ecto.ParameterizedType
  def cast(data, _strategy) do
    Ecto.Type.cast(:boolean, data)
  end

  @impl Vax.ParameterizedType
  def compute_change(_strategy, antidotec_flag, value) do
    :antidotec_flag.assign(antidotec_flag, value)
  end

  @impl Vax.ParameterizedType
  def antidote_crdt_type(:enable_wins), do: :antidote_crdt_flag_ew
  def antidote_crdt_type(:disable_wins), do: :antidote_crdt_flag_dw

  @impl Vax.ParameterizedType
  def client_dump(_strategy, nil) do
    :antidotec_flag.new()
  end

  def client_dump(_strategy, value) do
    :antidotec_flag.new(value)
  end

  @spec enable(Changeset.t(), atom()) :: Changeset.t()
  def enable(changeset, field) do
    Changeset.put_change(changeset, field, true)
  end

  @spec disable(Changeset.t(), atom()) :: Changeset.t()
  def disable(changeset, field) do
    Changeset.put_change(changeset, field, false)
  end

  @spec toggle(Changeset.t(), atom()) :: Changeset.t()
  def toggle(changeset, field) do
    value = Changeset.get_field(changeset, field) || false
    Changeset.put_change(changeset, field, !value)
  end
end

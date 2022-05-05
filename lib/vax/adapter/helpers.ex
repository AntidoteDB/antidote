defmodule Vax.Adapter.Helpers do
  @moduledoc false
  # TODO: split by purpose (?)

  @spec schema_primary_key!(schema :: atom()) :: atom()
  def schema_primary_key!(schema) do
    case schema.__schema__(:primary_key) do
      [primary_key] ->
        primary_key

      [] ->
        raise "Vax requires all schemas to have a primary key, found none for schema #{schema}"

      keys ->
        raise "Vax requires all schemas to have no more than one primary key. Found #{keys} for schema #{schema}"
    end
  end

  @spec object_key(schema_source :: binary(), primary_key :: binary()) :: binary()
  def object_key(schema_source, primary_key) do
    schema_source <> ":" <> primary_key
  end

  @spec build_object(schema_source :: binary(), primary_key :: binary(), bucket :: binary()) ::
          {binary(), :antidote_crdt_map_rr, binary()}
  def build_object(schema_source, primary_key, bucket) do
    {object_key(schema_source, primary_key), :antidote_crdt_map_rr, bucket}
  end

  @spec load_map(
          repo :: atom(),
          schema :: Ecto.Schema.t(),
          antidote_map :: :antidotec_map.antidotec_map()
        ) :: struct() | nil
  def load_map(repo, schema, map) do
    map
    |> :antidotec_map.value()
    |> Enum.map(fn {{k, _t}, v} -> {String.to_atom(k), v} end)
    |> case do
      [] -> nil
      fields -> repo.load(schema, fields)
    end
  end

  @spec build_update_map(repo :: atom(), schema :: Ecto.Schema.t(), values :: Enumerable.t()) ::
          :antidotec_map.antidote_map()
  def build_update_map(_repo, _schema, fields) do
    Enum.reduce(fields, :antidotec_map.new(), fn {key, value}, map ->
      # TODO: handle non-reg types
      reg = :antidotec_reg.new() |> :antidotec_reg.assign(value)
      :antidotec_map.add_or_update(map, {Atom.to_string(key), :antidote_crdt_register_lww}, reg)
    end)
  end
end

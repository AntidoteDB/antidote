defmodule Vax.Adapter.Query do
  alias Vax.Adapter.Helpers

  def query_to_objs(query, params, bucket) do
    {source, schema} = query.from.source
    primary_key = Helpers.schema_primary_key!(schema)

    case query.wheres do
      [%{expr: expr}] ->
        where_expr_to_key(expr, source, primary_key, bucket, params) |> List.flatten()

      wheres ->
        raise "Vax only supports a single `where` expression per query. Received #{Enum.count(wheres)}"
    end
  end

  def select_schema(query) do
    {_source, schema} = query.from.source
    schema
  end

  def select_fields(
        %{select: %{from: {:any, {:source, _source_schema, _, fields_and_types}}}} = _query_meta
      ) do
    Keyword.keys(fields_and_types)
  end

  defp where_expr_to_key({:or, _, exprs}, source, primary_key, bucket, params) do
    Enum.map(exprs, &where_expr_to_key(&1, source, primary_key, bucket, params))
  end

  defp where_expr_to_key(
         {:==, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, ix_or_literal]},
         source,
         primary_key,
         bucket,
         params
       ) do
    assert_primary_key!(field, primary_key)

    param = maybe_fetch_param(ix_or_literal, params)

    [Helpers.build_object(source, param, bucket)]
  end

  defp where_expr_to_key(
         {:in, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, ix_or_literal]} = _expr,
         source,
         primary_key,
         bucket,
         params
       ) do
    assert_primary_key!(field, primary_key)

    param = maybe_fetch_param(ix_or_literal, params)

    Enum.map(param, &Helpers.build_object(source, &1, bucket))
  end

  defp where_expr_to_key(
         expr,
         _source,
         _primary_key,
         _bucket,
         _params
       ) do
    raise "Unsupported expression #{Macro.to_string(expr)}"
  end

  defp assert_primary_key!(field, primary_key) do
    if field != primary_key,
      do: raise("Vax only supports filtering by primary key in where expressions")

    :ok
  end

  defp maybe_fetch_param(ix_or_literal, params) do
    case ix_or_literal do
      {:^, [], [ix | _]} -> Enum.at(params, ix)
      literal -> literal
    end
  end
end

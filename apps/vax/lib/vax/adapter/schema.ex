defmodule Vax.Adapter.Schema do
  @moduledoc false

  @bucket "vax"

  alias Vax.Adapter
  alias Vax.Adapter.AntidoteClient
  alias Vax.Adapter.Helpers

  def insert(_repo, %Ecto.Changeset{valid?: false} = changeset, _opts) do
    {:error, changeset}
  end

  def insert(repo, schema_or_changeset, _opts) do
    %schema_module{} =
      schema =
      case schema_or_changeset do
        %Ecto.Changeset{} = changeset -> Ecto.Changeset.apply_changes(changeset)
        schema -> schema
      end

    schema_primary_key = Helpers.schema_primary_key!(schema_module)

    schema =
      Map.update!(schema, schema_primary_key, fn
        nil -> Ecto.UUID.generate()
        term -> term
      end)

    primary_key = Map.get(schema, schema_primary_key)

    object = Helpers.build_object(schema_module.__schema__(:source), primary_key, @bucket)
    map = Helpers.build_insert_map(repo, schema)
    compute_ops_and_update(repo, object, schema, map)
  end

  def insert!(repo, schema_or_changeset, opts) do
    case insert(repo, schema_or_changeset, opts) do
      {:ok, result} ->
        result

      {:error, changeset} ->
        raise Ecto.InvalidChangesetError, action: :insert, changeset: changeset
    end
  end

  def update(_repo, %Ecto.Changeset{valid?: false} = changeset, _opts) do
    {:error, changeset}
  end

  def update(repo, changeset, _opts) do
    schema = changeset.data
    schema_primary_key = Helpers.schema_primary_key!(schema.__struct__)
    primary_key = Map.get(schema, schema_primary_key)
    object = Helpers.build_object(schema.__struct__.__schema__(:source), primary_key, @bucket)
    map = Helpers.build_update_map(repo, schema, changeset)
    compute_ops_and_update(repo, object, schema, map)
  end

  def update!(repo, changeset, opts) do
    case update(repo, changeset, opts) do
      {:ok, result} ->
        result

      {:error, changeset} ->
        raise Ecto.InvalidChangesetError, action: :update, changeset: changeset
    end
  end

  def delete(repo, schema, _options) do
    Adapter.execute_static_transaction(repo, fn conn, tx_id ->
      schema_primary_key = Helpers.schema_primary_key!(schema.__struct__)
      primary_key = Map.get(schema, schema_primary_key)
      object = Helpers.build_object(schema.__meta__.source, primary_key, @bucket)

      :ok = AntidoteClient.update_objects(conn, [{object, :reset, {}}], tx_id)

      {:ok, schema}
    end)
  end

  def delete!(repo, schema, opts) do
    case delete(repo, schema, opts) do
      {:ok, result} ->
        result

      {:error, changeset} ->
        raise Ecto.InvalidChangesetError, action: :delete, changeset: changeset
    end
  end

  def insert_or_update(repo, changeset, opts) do
    case get_state(changeset) do
      :built ->
        insert(repo, changeset, opts)

      :loaded ->
        update(repo, changeset, opts)

      state ->
        raise ArgumentError,
              "the changeset has an invalid state " <>
                "for Repo.insert_or_update/2: #{state}"
    end
  end

  def insert_or_update!(repo, changeset, opts) do
    case get_state(changeset) do
      :built ->
        insert!(repo, changeset, opts)

      :loaded ->
        update!(repo, changeset, opts)

      state ->
        raise ArgumentError,
              "the changeset has an invalid state " <>
                "for Repo.insert_or_update/2: #{state}"
    end
  end

  defp get_state(%Ecto.Changeset{data: %{__meta__: %{state: state}}}), do: state

  defp get_state(%{__struct__: _}) do
    raise ArgumentError,
          "giving a struct to Repo.insert_or_update/2 or " <>
            "Repo.insert_or_update!/2 is not supported. " <>
            "Please use an Ecto.Changeset"
  end

  defp compute_ops_and_update(repo, object, schema, map) do
    ops = :antidotec_map.to_ops(object, map)

    Adapter.execute_static_transaction(repo, fn conn, tx_id ->
      :ok = AntidoteClient.update_objects(conn, ops, tx_id)
      {:ok, repo.reload(schema)}
    end)
  end
end

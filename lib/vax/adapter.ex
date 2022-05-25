defmodule Vax.Adapter do
  @moduledoc """
  Ecto adapter for Vaxine
  """

  alias Vax.ConnectionPool
  alias Vax.Adapter.AntidoteClient
  alias Vax.Adapter.Query

  @bucket "vax"

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Storage

  @impl Ecto.Adapter.Queryable
  def stream(_adapter_meta, _query_meta, _query_cache, _params, _options) do
    raise "Not implemented"
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:update_all, _query), do: raise("Not implemented")
  def prepare(:delete_all, _query), do: raise("Not implemented")

  def prepare(:all, query) do
    {:nocache, query}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, {:nocache, query}, params, _options) do
    objs = Query.query_to_objs(query, params, @bucket)
    fields = Query.select_fields(query_meta)
    defaults = query |> Query.select_schema() |> struct()

    execute_static_transaction(adapter_meta.repo, fn conn, tx_id ->
      {:ok, results} = AntidoteClient.read_objects(conn, objs, tx_id)

      results =
        for result <- results,
            result_value = :antidotec_map.value(result),
            result_value != %{} do
          map = Map.new(result_value, fn {{k, _t}, v} -> {String.to_atom(k), v} end)
          Enum.map(fields, &Map.get_lazy(map, &1, fn -> Map.get(defaults, &1) end))
        end

      {Enum.count(results), results}
    end)
  end

  @impl Ecto.Adapter
  def loaders(:binary_id, type), do: [type]
  def loaders(:string, :string), do: [:string]

  def loaders(_primitive_type, ecto_type) do
    if Vax.Type.base_or_composite?(ecto_type) do
      [&binary_to_term/1, ecto_type]
    else
      [ecto_type]
    end
  end

  @impl Ecto.Adapter
  def dumpers(:binary_id, type), do: [type]
  def dumpers(:string, :string), do: [:string]
  def dumpers({:in, _primitive_type}, {:in, ecto_type}), do: [&dump_inner(&1, ecto_type)]
  def dumpers(_primitive_type, ecto_type), do: [ecto_type, &term_to_binary/1]

  defp term_to_binary(term), do: {:ok, :erlang.term_to_binary(term)}
  defp binary_to_term(binary), do: {:ok, :erlang.binary_to_term(binary)}

  defp dump_inner(values, ecto_type) do
    values
    |> Enum.reduce_while([], fn v, acc ->
      case Ecto.Type.adapter_dump(__MODULE__, ecto_type, v) do
        {:ok, v} -> {:cont, [v | acc]}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      list -> {:ok, Enum.reverse(list)}
    end
  end

  @impl Ecto.Adapter
  def init(config) do
    hostname = Keyword.fetch!(config, :hostname) |> String.to_charlist()
    port = Keyword.get(config, :port, 8087)
    pool_size = Keyword.get(config, :pool_size, 10)
    log? = Keyword.get(config, :log, true)

    child_spec = %{
      id: ConnectionPool,
      start:
        {NimblePool, :start_link,
         [[worker: {ConnectionPool, [hostname: hostname, port: port]}, size: pool_size]]}
    }

    if log?, do: Vax.Adapter.Logger.attach()

    {:ok, child_spec, %{}}
  end

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type) do
    {:ok, []}
  end

  @impl Ecto.Adapter
  def checkout(%{pid: pool}, _config, function) do
    if Process.get(:vax_checked_out_conn) do
      function.()
    else
      ConnectionPool.checkout(pool, fn {_pid, _ref}, pid ->
        try do
          Process.put(:vax_checked_out_conn, pid)
          result = function.()

          {result, pid}
        after
          Process.put(:vax_checked_out_conn, nil)
        end
      end)
    end
  end

  @impl Ecto.Adapter
  def checked_out?(_adapter_meta) do
    not is_nil(Process.get(:vax_checked_out_conn))
  end

  @impl Ecto.Adapter
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      Increments a counter

      See `Vax.Adapter.increment_counter/3` for more information
      """
      @spec increment_counter(key :: binary(), amount :: integer()) :: :ok
      def increment_counter(key, amount) do
        Vax.Adapter.increment_counter(__MODULE__, key, amount)
      end

      @doc """
      Reads a counter

      See `Vax.Adapter.read_counter/2` for more information
      """
      @spec read_counter(key :: binary()) :: integer()
      def read_counter(key) do
        Vax.Adapter.read_counter(__MODULE__, key)
      end

      def insert(changeset_or_struct, opts \\ []) do
        Vax.Adapter.Schema.insert(__MODULE__, changeset_or_struct, opts)
      end

      def update(changeset, opts \\ []) do
        Vax.Adapter.Schema.update(__MODULE__, changeset, opts)
      end

      def insert_or_update(changeset, opts \\ []) do
        Vax.Adapter.Schema.insert_or_update(__MODULE__, changeset, opts)
      end

      def delete(schema, opts \\ []) do
        Vax.Adapter.Schema.delete(__MODULE__, schema, opts)
      end

      def insert!(changeset_or_struct, opts \\ []) do
        Vax.Adapter.Schema.insert!(__MODULE__, changeset_or_struct, opts)
      end

      def update!(changeset, opts \\ []) do
        Vax.Adapter.Schema.update!(__MODULE__, changeset, opts)
      end

      def insert_or_update!(changeset, opts \\ []) do
        Vax.Adapter.Schema.insert_or_update!(__MODULE__, changeset, opts)
      end

      def delete!(schema, opts \\ []) do
        Vax.Adapter.Schema.delete!(__MODULE__, schema, opts)
      end

      @doc """
      Executes a static transaction
      """
      @spec execute_static_transaction((conn :: pid(), tx_id :: term() -> result :: term())) ::
              term()
      def execute_static_transaction(fun) do
        Vax.Adapter.execute_static_transaction(__MODULE__, fun)
      end
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_up(_) do
    :ok
  end

  @impl Ecto.Adapter.Storage
  def storage_down(_) do
    :ok
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_) do
    :ok
  end

  @doc """
  Reads a counter
  """
  @spec read_counter(repo :: atom() | pid(), key :: binary()) :: integer()
  def read_counter(repo, key) do
    execute_static_transaction(repo, fn conn, tx_id ->
      obj = {key, :antidote_crdt_counter_pn, @bucket}
      {:ok, [result]} = AntidoteClient.read_objects(conn, [obj], tx_id)

      :antidotec_counter.value(result)
    end)
  end

  @doc """
  Increases a counter
  """
  @spec increment_counter(repo :: atom() | pid(), key :: binary(), amount :: integer()) :: :ok
  def increment_counter(repo, key, amount) do
    execute_static_transaction(repo, fn conn, tx_id ->
      obj = {key, :antidote_crdt_counter_pn, @bucket}
      counter = :antidotec_counter.increment(amount, :antidotec_counter.new())
      counter_update_ops = :antidotec_counter.to_ops(obj, counter)

      AntidoteClient.update_objects(conn, counter_update_ops, tx_id)
    end)
  end

  defp get_conn(), do: Process.get(:vax_checked_out_conn) || raise("Missing connection")

  def execute_static_transaction(repo, fun) when is_atom(repo) or is_pid(repo) do
    meta = Ecto.Adapter.lookup_meta(repo)
    execute_static_transaction(meta, fun)
  end

  def execute_static_transaction(meta, fun) do
    checkout(meta, [], fn ->
      conn = get_conn()

      {:ok, tx_id} = AntidoteClient.start_transaction(conn, :ignore, static: true)

      fun.(conn, tx_id)
    end)
  end
end

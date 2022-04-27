defmodule Vax.Adapter do
  @moduledoc """
  Ecto adapter for Vaxine
  """

  alias Vax.ConnectionPool

  @behaviour Ecto.Adapter

  @impl true
  def loaders(:binary_id, type), do: [type, Ecto.UUID]
  def loaders(_primitive_type, ecto_type), do: [ecto_type]

  @impl true
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_primitive_type, ecto_type), do: [ecto_type]

  @impl true
  def init(config) do
    address = Keyword.fetch!(config, :address) |> String.to_charlist()
    port = Keyword.get(config, :port, 8087)
    pool_size = Keyword.get(config, :pool_size, 10)

    child_spec = %{
      id: ConnectionPool,
      start:
        {NimblePool, :start_link,
         [[worker: {ConnectionPool, [address: address, port: port]}, size: pool_size]]}
    }

    {:ok, child_spec, %{}}
  end

  @impl true
  def ensure_all_started(_config, _type) do
    {:ok, []}
  end

  @impl true
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

  @impl true
  def checked_out?(_adapter_meta) do
    Process.get(:vax_checked_out_conn, nil)
  end

  def get_conn(), do: Process.get(:vax_checked_out_conn) || raise("Missing connection")

  def execute_static_transaction(repo, fun) do
    meta = Ecto.Adapter.lookup_meta(repo)

    checkout(meta, [], fn ->
      conn = get_conn()

      {:ok, tx_id} = :antidotec_pb.start_transaction(conn, :ignore, static: true)

      fun.(conn, tx_id)
    end)
  end

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      Increments a counter

      See `Vax.Adapter.inc_counter/3` for more information
      """
      @spec inc_counter(key :: binary(), amount :: integer()) :: :ok
      def inc_counter(key, amount) do
        Vax.Adapter.inc_counter(__MODULE__, key, amount)
      end

      @doc """
      Reads a counter

      See `Vax.Adapter.read_counter/2` for more information
      """
      @spec read_counter(key :: binary()) :: integer()
      def read_counter(key) do
        Vax.Adapter.read_counter(__MODULE__, key)
      end
    end
  end

  @doc """
  Reads a counter
  """
  @spec read_counter(repo :: atom() | pid(), key :: binary()) :: integer()
  def read_counter(repo, key) do
    execute_static_transaction(repo, fn conn, tx_id ->
      obj = {key, :antidote_crdt_counter_pn, "my_bucket"}
      {:ok, [result]} = :antidotec_pb.read_objects(conn, [obj], tx_id)

      :antidotec_counter.value(result)
    end)
  end

  @doc """
  Increases a counter
  """
  @spec inc_counter(repo :: atom() | pid(), key :: binary(), amount :: integer()) :: :ok
  def inc_counter(repo, key, amount) do
    execute_static_transaction(repo, fn conn, tx_id ->
      obj = {key, :antidote_crdt_counter_pn, "my_bucket"}
      counter = :antidotec_counter.increment(amount, :antidotec_counter.new())
      counter_update_ops = :antidotec_counter.to_ops(obj, counter)

      :antidotec_pb.update_objects(conn, counter_update_ops, tx_id)
    end)
  end
end

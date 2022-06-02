defmodule Vax.ConnectionPool do
  @behaviour NimblePool

  @spec checkout(pid() | atom(), ({pid(), reference()}, pid() -> {term(), pid()})) :: term()
  def checkout(pool, fun, _opts \\ []) do
    NimblePool.checkout!(pool, :checkout, fun)
  end

  @impl true
  def init_worker(pool_state) do
    hostname = Keyword.fetch!(pool_state, :hostname)
    port = Keyword.fetch!(pool_state, :port)

    # Starting the protobuf socket is blocking, so we use NimblePool async
    # initialization
    init_fun = fn ->
      {:ok, socket_pid} = :antidotec_pb_socket.start_link(hostname, port)
      %{pb_socket: socket_pid}
    end

    {:async, init_fun, pool_state}
  end

  @impl true
  def handle_checkout(_maybe_wrapped_command, _from, worker_state, pool_state) do
    {:ok, worker_state.pb_socket, worker_state, pool_state}
  end
end

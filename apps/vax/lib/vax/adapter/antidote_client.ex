defmodule Vax.Adapter.AntidoteClient do
  @moduledoc false

  @spec read_objects(
          conn :: pid(),
          objects :: [{key :: binary(), type :: atom(), bucket :: binary()}],
          tx_id :: term()
        ) :: {:ok, results :: [term]}
  def read_objects(conn, objects, tx_id) do
    metadata = %{objects: objects, tx_id: tx_id}

    :telemetry.span([:vax, :read_objects], metadata, fn ->
      {:antidotec_pb.read_objects(conn, objects, tx_id), metadata}
    end)
  end

  @spec update_objects(conn :: pid(), ops :: list(), tx_id :: term()) :: :ok
  def update_objects(conn, ops, tx_id) do
    metadata = %{ops: ops, tx_id: tx_id}

    :telemetry.span([:vax, :update_objects], metadata, fn ->
      {:antidotec_pb.update_objects(conn, ops, tx_id), metadata}
    end)
  end

  @spec start_transaction(conn :: pid(), last_tx_id :: binary() | :ignore, opts :: Keyword.t()) ::
          {:ok, tx_id :: term()}
  def start_transaction(conn, last_tx_id \\ :ignore, opts \\ []) do
    :telemetry.span([:vax, :start_transaction], start_transaction_metadata(last_tx_id), fn ->
      result = :antidotec_pb.start_transaction(conn, last_tx_id, opts)
      {result, start_transaction_metadata(last_tx_id, result)}
    end)
  end

  defp start_transaction_metadata(last_tx_id, result \\ nil)

  defp start_transaction_metadata(last_tx_id, {:ok, tx_id}),
    do: %{last_tx_id: last_tx_id, tx_id: tx_id}

  defp start_transaction_metadata(last_tx_id, _), do: %{last_tx_id: last_tx_id}
end

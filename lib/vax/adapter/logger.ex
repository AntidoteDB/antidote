defmodule Vax.Adapter.Logger do
  @moduledoc """
  Logger for Vax side-effects. Enabled by default, can be disabled by
  """

  require Logger

  def attach do
    :telemetry.attach_many(
      "vax-logger",
      [
        [:vax, :read_objects, :stop],
        [:vax, :update_objects, :stop],
        [:vax, :start_transaction, :stop]
      ],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  def handle_event([:vax, :read_objects, :stop], measurements, metadata, _handler_meta) do
    case metadata.objects do
      [] ->
        :ok

      objects ->
        [
          "Vax",
          " - ",
          "READ OBJECTS - ",
          "Took #{duration_to_binary(measurements.duration)} ",
          "in transaction #{tx_id_to_binary(metadata.tx_id)}.\n",
          "Read keys:",
          objects_to_iolist(objects)
        ]
        |> Logger.debug()
    end
  end

  def handle_event([:vax, :update_objects, :stop], measurements, metadata, _handler_meta) do
    case metadata.ops do
      [] ->
        :ok

      ops ->
        [
          "Vax",
          " - ",
          "UPDATE OBJECTS - ",
          "Took #{duration_to_binary(measurements.duration)} ",
          "in transaction #{tx_id_to_binary(metadata.tx_id)}.\n",
          "Update OPS:",
          ops_to_iolist(ops)
        ]
        |> Logger.debug()
    end
  end

  def handle_event([:vax, :start_transaction, :stop], measurements, metadata, _handler_meta) do
    [
      "Vax",
      " - ",
      "START TRANSACTION",
      " - ",
      "Took #{duration_to_binary(measurements.duration)} ",
      " - Transaction id: ",
      tx_id_to_binary(metadata.tx_id)
    ]
    |> Logger.debug()
  end

  defp objects_to_iolist(objects) do
    Enum.map(objects, fn {key, _type, _bucket} -> "\n- " <> key end)
  end

  defp ops_to_iolist(ops) do
    Enum.map(ops, fn op ->
      [
        "\n- ",
        op |> elem(0) |> object_to_binary(),
        " - ",
        inspect(Tuple.delete_at(op, 0))
      ]
    end)
  end

  defp object_to_binary({key, _type, _bucket}), do: key

  defp duration_to_binary(duration) do
    duration_in_us = System.convert_time_unit(duration, :native, :microsecond)
    "#{duration_in_us} μs"
  end

  defp tx_id_to_binary({_kind, {tx_vector_clock, _opts}}), do: inspect(tx_vector_clock)
end

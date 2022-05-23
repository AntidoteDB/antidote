defmodule Vax.ParameterizedType do
  defmacro __using__(opts) do
    quote do
      use Ecto.ParameterizedType, unquote(opts)

      @behaviour Vax.ParameterizedType
    end
  end

  @callback antidote_crdt_type(params :: term()) :: atom()
  @callback client_dump(params :: term, value :: term()) :: term()
  @callback compute_change(params :: term(), antidotec_type :: term(), change :: term()) :: term()
end

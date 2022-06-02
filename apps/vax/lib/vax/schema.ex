defmodule Vax.Schema do
  @moduledoc """
  Wrapper over Ecto.Schema to be used with Vaxine schemas

  Uses `:binary_id` as primary key.
  """

  defmacro __using__(_opts) do
    quote do
      use Ecto.Schema

      @primary_key {:id, :binary_id, autogenerate: true}
      @foreign_key_type :binary_id
    end
  end
end

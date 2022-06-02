defmodule Vax.Types.SetTest do
  use ExUnit.Case, async: true

  alias Vax.Types.Set

  setup_all do
    integer_set = Ecto.ParameterizedType.init(Set, type: :integer)
    string_set = Ecto.ParameterizedType.init(Set, type: :string)

    {:ok, integer_set: integer_set, string_set: string_set}
  end

  describe "cast/2" do
    test "properly casts inner_type", %{integer_set: int_set, string_set: string_set} do
      assert {:ok, MapSet.new([10, 20])} == Ecto.Type.cast(int_set, [10, 20])
      assert {:ok, MapSet.new([10, 20])} == Ecto.Type.cast(int_set, ["10", "20"])
      assert {:ok, MapSet.new([10])} == Ecto.Type.cast(int_set, [10, 10])

      assert {:ok, MapSet.new(["a", "b"])} == Ecto.Type.cast(string_set, ["a", "b"])
      assert {:ok, MapSet.new(["a"])} == Ecto.Type.cast(string_set, ["a", "a"])
    end
  end

  describe "compute_changes/3" do
    test "properly adds items to set", %{integer_set: int_set} do
      base_set = Vax.Type.client_dump(int_set, [])
      s1 = MapSet.new([10])

      assert {:antidote_set, _set, adds, _removes} =
               Vax.Type.compute_change(int_set, base_set, s1)

      assert MapSet.to_list(s1) == :sets.to_list(adds) |> Enum.map(&:erlang.binary_to_term/1)
    end

    test "properly removes items from set", %{integer_set: int_set} do
      base_set = Vax.Type.client_dump(int_set, [10, 20])

      assert {:antidote_set, _set, _adds, removes} =
               Vax.Type.compute_change(int_set, base_set, MapSet.new([10]))

      assert [20] == :sets.to_list(removes) |> Enum.map(&:erlang.binary_to_term/1)
    end
  end
end

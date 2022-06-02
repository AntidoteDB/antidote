defmodule Vax.Types.CounterTest do
  use ExUnit.Case, async: true

  alias Vax.Types.Counter

  describe "cast/1" do
    test "properly casts integers" do
      assert {:ok, 10} = Counter.cast(10)
      assert {:ok, 20} = Counter.cast(20)
    end

    test "properly casts strings" do
      assert {:ok, 10} = Counter.cast("10")
      assert {:ok, 20} = Counter.cast("20")
      assert :error = Counter.cast("a")
      assert :error = Counter.cast("2a")
    end

    test "errors for floats" do
      assert :error = Counter.cast(10.0)
    end
  end

  describe "compute_change/2" do
    test "properly computes change to a crdt type" do
      old_counter = :antidotec_counter.new(10)
      assert {:counter, 10, 15} = Counter.compute_change(old_counter, 25)
    end
  end

  describe "cast_increment/3" do
    test "properly increments the value of a counter" do
      changeset =
        {%{counter: 3}, %{counter: Counter}}
        |> Counter.cast_increment(:counter, 2)

      assert 5 = Ecto.Changeset.get_field(changeset, :counter)
    end
  end
end

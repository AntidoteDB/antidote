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
end

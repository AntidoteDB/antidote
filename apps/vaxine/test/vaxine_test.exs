defmodule VaxineTest do
  use ExUnit.Case
  doctest Vaxine

  test "greets the world" do
    assert Vaxine.hello() == :world
  end
end

defmodule VaxTest do
  use ExUnit.Case
  doctest Vax

  test "greets the world" do
    assert Vax.hello() == :world
  end
end

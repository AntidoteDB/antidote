defmodule Vax.AdapterIntegrationTest do
  use ExUnit.Case, async: true

  defmodule TestRepo do
    use Ecto.Repo, adapter: Vax.Adapter, otp_app: :vax
  end

  setup_all %{} do
    start_supervised!({TestRepo, address: "localhost", port: 8087})

    :ok
  end

  test "checking out a connection" do
    refute TestRepo.checked_out?()

    TestRepo.checkout(fn ->
      assert TestRepo.checked_out?()
    end)

    refute TestRepo.checked_out?()
  end

  test "performing actions on checked out connection" do
    TestRepo.checkout(fn ->
      assert 0 = TestRepo.read_counter("bar")
      assert :ok = TestRepo.inc_counter("bar", 10)
      assert 10 = TestRepo.read_counter("bar")
      assert :ok = TestRepo.inc_counter("bar", 20)
      assert 30 = TestRepo.read_counter("bar")
    end)
  end

  test "reading and increasing a counter" do
    assert 0 = TestRepo.read_counter("foo")
    assert :ok = TestRepo.inc_counter("foo", 10)
    assert 10 = TestRepo.read_counter("foo")
    assert :ok = TestRepo.inc_counter("foo", 20)
    assert 30 = TestRepo.read_counter("foo")
  end
end

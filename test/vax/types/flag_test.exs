defmodule Vax.Types.FlagTest do
  use ExUnit.Case, async: true

  alias Vax.Types.Flag

  setup_all do
    {:ok, flag: Ecto.ParameterizedType.init(Flag, conflict_resolution: :enable_wins)}
  end

  describe "cast/2" do
    test "properly casts to boolean", %{flag: flag} do
      assert {:ok, true} == Ecto.Type.cast(flag, true)
      assert {:ok, false} == Ecto.Type.cast(flag, false)
      assert {:ok, true} == Ecto.Type.cast(flag, "true")
      assert {:ok, false} == Ecto.Type.cast(flag, "false")
    end
  end

  describe "compute_changes/3" do
    test "properly changes flag value", %{flag: flag} do
      base_flag = Vax.Type.client_dump(flag, false)

      assert {:antidote_flag, _, true} = Vax.Type.compute_change(flag, base_flag, true)
      assert {:antidote_flag, _, false} = Vax.Type.compute_change(flag, base_flag, false)
    end
  end

  test "antidote_crdt_type/1 changes according to param" do
    enable_wins_flag = Ecto.ParameterizedType.init(Flag, conflict_resolution: :enable_wins)
    disable_wins_flag = Ecto.ParameterizedType.init(Flag, conflict_resolution: :disable_wins)

    assert Vax.Type.crdt_type(enable_wins_flag) == :antidote_crdt_flag_ew
    assert Vax.Type.crdt_type(disable_wins_flag) == :antidote_crdt_flag_dw
  end

  test "enable/2 enables the flag", %{flag: flag} do
    changeset =
      {%{}, %{flag: flag}}
      |> Ecto.Changeset.change()
      |> Flag.enable(:flag)

    assert Ecto.Changeset.get_field(changeset, :flag) == true
  end

  test "disable/2 disables the flag", %{flag: flag} do
    changeset =
      {%{}, %{flag: flag}}
      |> Ecto.Changeset.change()
      |> Flag.disable(:flag)

    assert Ecto.Changeset.get_field(changeset, :flag) == false
  end

  test "toggle/2 toggles the flag", %{flag: flag} do
    changeset =
      {%{}, %{flag: flag}}
      |> Ecto.Changeset.change()
      |> Flag.toggle(:flag)

    assert Ecto.Changeset.get_field(changeset, :flag)

    changeset = Flag.toggle(changeset, :flag)
    refute Ecto.Changeset.get_field(changeset, :flag)

    changeset = Flag.toggle(changeset, :flag)
    assert Ecto.Changeset.get_field(changeset, :flag)
  end
end

from coffee_cocoa_platform import ProjectPaths


def test_explicit_root_takes_precedence_without_creating_directories(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("COFFEE_COCOA_HOME", str(tmp_path / "other"))
    paths = ProjectPaths.from_root(tmp_path / "project")

    assert paths.raw == tmp_path / "project/data/raw"
    assert paths.parquet == tmp_path / "project/data/parquet"
    assert paths.warehouse == tmp_path / "project/warehouse"
    assert paths.state == tmp_path / "project/.state"
    assert not paths.root.exists()


def test_environment_root_is_used_from_another_working_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("COFFEE_COCOA_HOME", str(tmp_path / "project"))
    monkeypatch.chdir(tmp_path)

    assert ProjectPaths.from_root().root == tmp_path / "project"

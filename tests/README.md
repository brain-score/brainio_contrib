# Unit Tests
## Markers
Unit tests have various markers that denote possible issues in the travis build:

* **private_access**: tests that require access to a private ressource, such as assemblies on S3 (travis pull request builds can not have private access)

Use the following syntax to mark a test:
```
@pytest.mark.private_access
def test_something(...):
    assert False
```

To skip a specific marker, run e.g. `pytest -m "not private_access"`.
To skip multiple markers, run e.g. `pytest -m "not private_access and not memory_intense"`.

# BDD Tests

The BDD tests are written in Python and make use of the pytest-bdd library.

The tests utilise auto-generated code from the OpenApi spec. To generate the client code run:
```bash
./../../scripts/generate-openapi-bindings-bdd.sh
```

The feature files in the `features` directory define the behaviour expected of the control plane. These behaviours are described using the [Gherkin](https://cucumber.io/docs/gherkin/) syntax.

The feature files can be used to auto-generate the test file. For example running `pytest-bdd generate features/volume/replicas.feature > test_volume_replicas.py`
generates the `test_volume_replicas.py` test file from the `replicas.feature` file.

**:warning: Note: Running pytest-bdd generate will overwrite any existing files with the same name**

## Running the Tests
Before running any tests run the `setup.sh` script. This sets up the necessary environment to run the tests:
```bash
source ./setup.sh
```

To run all the tests:
```bash
pytest .
```

To run individual test files:
```bash
pytest --tc-file=test_config.ini test_volume_observability.py
```
# BDD Tests

The BDD tests are written in Python and make use of the pytest-bdd library.

The feature files in the `features` directory define the behaviour expected of the control plane. These behaviours are described using the [Gherkin](https://cucumber.io/docs/gherkin/) syntax.

The feature files can be used to auto-generate the test file. For example running `pytest-bdd generate replicas.feature > test_volume_replicas.py`
generates the `test_volume_replicas.py` test file from the `replicas.feature` file.

**:warning: Note: Running pytest-bdd generate will overwrite any existing files with the same name**

## Running the Tests by entering the python virtual environment
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
pytest features/volume/create/test_feature.py
```

To run an individual test within a test file use the `-k` option followed by the test name:
```bash
pytest features/volume/create/test_feature.py -k test_sufficient_suitable_pools
```

## Running the Tests
The script in `../../scripts/python/test.sh` can be used to run the tests without entering the venv.
This script will implicitly enter and exit the venv during test execution.

To run all the tests:
```bash
../../scripts/python/test.sh
```

Arguments will be passed directly to pytest. Example running individual tests:
```bash
../../scripts/python/test.sh features/volume/create/test_feature.py -k test_sufficient_suitable_pools
```

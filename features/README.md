# Component tests

This directory contains [Godog](https://github.com/cucumber/godog) (Cucumber style) component tests. Each `.feature` file tests one HTTP endpoint against a running instance of the Dataset API using real Mongo/Kafka containers.

## Folder structure

Feature files are organised like URL routing, mirroring the endpoint path, then split by web/publishing mode and dataset
type:

```text
features/
  steps/                            # Go step definitions shared by all .feature files
  compose/                          # Docker Compose files used by `make test-component`
  <resource>/                       # e.g. datasets, instances, dataset-editions
    web/                            # API running in web mode (ENABLE_PRIVATE_ENDPOINTS=false)
    publishing/                     # API running in publishing mode (ENABLE_PRIVATE_ENDPOINTS=true)
      static/                       # scenarios specific to type: "static" datasets
      other/                        # scenarios for non-static (CMD/Cantabular) datasets
    [id]/                           # nested resource, e.g. a specific dataset
      editions/[edition]/versions/[version]/...
```

To find the tests for an endpoint: start from the resource name, then narrow by mode (`web`/`publishing`) and
dataset type if relevant. For example, tests for `PUT /datasets/{id}` on a static dataset are found in
`features/datasets/[id]/publishing/static/put.feature`.

## Naming conventions

**Feature titles** — one per file, naming the action, resource, and mode:

```text
{Action} [static] {resource} in {web|publishing} mode
```

- `Action` — `List`, `Get`, `Create`, `Update`, `Delete`, or `Query`, matching the HTTP method.
- `static` — included only for files under a `static/` folder. *This may change to `dataset type` in the future.*
- `resource` — `dataset`, `edition`, `version`, `instance`, ...
- `web|publishing` — the API mode the file's folder is under.

Example: `Feature: Get static version in web mode`

## Running tests

### Running all component tests

Run the `make test-component` command from the root of the repository. (See [Makefile](../Makefile))

### Running a specific test file or folder

To run only a specific file or folder, add its path as an extra argument to the `command:` list in
[compose/dp-dataset-api.yml](compose/dp-dataset-api.yml), after `-component`, then run `make test-component` as
normal.

#### Specific test file

```yaml
    command:
      # ...unchanged flags...
      - -component
      - features/datasets/[id]/publishing/static/put.feature
      # add more specific test files
```

#### Specific test folder

```yaml
    command:
      # ...unchanged flags...
      - -component
      - features/datasets/[id]/publishing/static/
      # add more specific test folders
```

#### Specific files and folders

```yaml
    command:
      # ...unchanged flags...
      - -component
      - features/datasets/[id]/publishing/static/
      - features/datasets/[id]/publishing/other/get.feature
      # add more specific test files and/or folders
```

> **Note:** a folder path always includes its subfolders. To run only the files directly inside it, list those `.feature` files individually instead.

## Formatting

- Indentation:
  - `Feature` at the start of the line
  - `Background`/`Scenario` at 2 spaces
  - Steps at 4 spaces
  - Docstrings and tables at 6 spaces
- JSON docstrings are pretty printed with 2 space indentation.
- Data tables use single space padding inside each `|` delimiter, with columns aligned.
- One blank line between the feature description/Background and the first scenario, and between scenarios
- No blank lines within a single scenario's steps.
- New line at the end of the file.

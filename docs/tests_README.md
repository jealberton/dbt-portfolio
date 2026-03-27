## DBT Tests

There are two kinds of tests in DBT:
- **Generic tests**
- **Singular tests**

### Generic Tests

The `generic` tests are configured like a macro. They follow this pattern:

```sql
{% test <test_name>(model[, column_name, other_parameters=value]) %}
<The SQL code>
{% endtest %}
```

As a requirement, they must have at least the `model`. Most tests also include the `column_name`.
`model` and `column_name` are reserved keywords in tests.

As we are using the Elementary package, using `column_name` will populate the field `test_column_name` in the `dbt_test` table and the `column_name` field in the `elementary_test_results` table.

#### Features
- To perform a generic test, you must define it in the `properties.yml` file of the model.
- It can be set at the **model level** or **column level**.

#### Example of a Generic Test

Below is an example configuration for a generic test in a `schema.yml` file:

```yaml
version: 2

models:
  - name: slv_meetings__meetings_dim
    description: "Dimensions and attributes for Calendly meeting types."
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [meeting_type_id]
        warn_unenforced: false
    columns:
      - name: meeting_type_id
        data_type: string
        description: "Unique identifier for the meeting type"
        data_tests:
          - unique
          - not_null

# Compliance validations
    data_tests:
      - cpl_generic__program_code:
          name: cpl_slv_meetings__program_code # model_level
          column_name: program_id # Required by the generic test: 'cpl_generic__program_code'
          context_columns: # Optional arguments in the test: 'cpl_generic__program_code'
            - calendly_created_at_ts
            - meeting_name
            - meeting_type_id
            - internal_note
          config:
            description: "Check that the program code is valid"
            alias: 'cpl_generic__program_code_alias' # This will create the alias of this test.
            limit: 50 # This will limit the result set that is retrived. Set whatever limit you feel is appropriate for the test.
            # TODO: Review wit Owen if we want to limit the results.
            severity: warn # All tests are configured as 'warn' by default in the `dbt_project.yml` file.
            tags: [compliance, cpl_meetings] # This is configured in the `dbt_project.yml` file. We can use it here to leverage more tags.
            meta:
              quality_dimension: 'completeness' # quality_dimension: "completeness", "uniqueness", "validity", "accuracy", and "consistency".
              slack_channel: var('compliance_slack_channel')
              cpl_id: 'CPL-SVC-001'
              service: 'calendly'
              description: "Violation: Program code missing"
              gs_url: 'https://docs.google.com/spreadsheets/d/to-be-defined/'
            store_failures: true # This will create a table with the name of the test.
            schema: var('compliance_failures_dataset')
            where: "DATE(calendly_created_at_ts) >= '2025-01-01'" # Where clause can be added in generic tests only.
```

### Singular Tests

Singular tests function similarly to models; you write SQL queries as if they were standalone models.

#### Example of a Singular Test

```sql
{#-
    This test validates whether `program_id` values in the `slv_meetings__meetings_dim` model
    conform to the expected compliance program code format: `XXXX-XXXX-000`
    (where `X` is an uppercase letter and `0` is a digit).

    Configuration:
        - `limit`: Limits the number of failing records stored (default: 500).
        - `severity`: Defines the severity level (`error` by default).
        - `error_if`: Fails the test if the error percentage is `>=70%`.
        - `warn_if`: Raises a warning if the error percentage is `>10%`.
        - `store_failures`: Stores failing records for debugging.

    Returns:
        A query that retrieves records where `program_id`:
        - Is NULL.
        - Does not match the required format (`XXXX-XXXX-000`).
        - Contains specific format issues (e.g., missing hyphens, incorrect character types).

    Example:
        ```yaml
        version: 2

        models:
          - name: slv_meetings__meetings_dim
            tests:
              - name: cpl_program_code
                severity: error
        ```
-#}

{# Most of the configurations in the generic test can also be set for singular test #}
{{
  config(
    limit = 500,
    severity = "error",
    error_if = ">=70",
    warn_if = ">10",
    store_failures = true,
    )
}}
WITH all_values AS (
    SELECT
        calendly_created_at_ts,
        meeting_name,
        meeting_type_id,
        internal_note,
        program_id AS program_code
    FROM {{ ref('slv_meetings__meetings_dim') }}
),

invalid_format AS (
    SELECT
        calendly_created_at_ts,
        meeting_name,
        meeting_type_id,
        internal_note,
        program_code,
        -- Add a descriptive reason for the failure
        CASE
            WHEN program_code IS NULL THEN 'NULL value'
            WHEN NOT regexp_contains(program_code, r'([A-Z]{4}-[A-Z]{4}-\d{3})') THEN
                CASE
                    WHEN program_code NOT LIKE '%-%-%' THEN 'Missing hyphens'
                    WHEN NOT regexp_contains(program_code, r'([A-Z]{4}-)') THEN 'First segment not 4 uppercase letters'
                    WHEN NOT regexp_contains(program_code, r'(-[A-Z]{4}-)') THEN 'Second segment not 4 uppercase letters'
                    WHEN NOT regexp_contains(program_code, r'(-\d{3})') THEN 'Third segment not 3 digits'
                    ELSE 'Unknown format issue'
                END
            ELSE 'Unknown reason'
        END as failure_reason
    FROM all_values
    WHERE
        program_code IS NULL
        OR NOT regexp_contains(program_code, r'([A-Z]{4}-[A-Z]{4}-\d{3})')
)

SELECT * FROM invalid_format

```

### Data Tests Configuration

Please refer to the [DBT Official Documentation](https://docs.getdbt.com/reference/data-test-configs) to understand the various ways data tests can be configured.

## Project Tests Configuration

**Prefixes:**
- `tst_`: General tests prefix.
- `cpl_`: Compliance tests prefix.

```
models/tests
├── generic
│   └── compliance
│   │   └── cpl_generic__<column_name or test_name>.sql
│   └── tst_generic__<column_name or test_name>.sql
├── singular
│   ├── meetings
│   │   ├── cpl_meetings__<column_name or test_name>.sql
│   │   └── tst_meetings__<column_name or test_name>.sql
│   └── ops
│       └── cpl_ops__<column_name or test_name>.sql
└── tests_README.md
```

### Elementary

On your branch, you need to make the following changes, but they should **not** be committed to the repo:

#### `dbt_project.yml` Configuration

You need to add Elementary to your DEV environment but remember not to commit the following line:

```yaml
models:
  elementary:
    +dataset: "elementary"
    # To disable elementary for developers' environments
    enabled: "{{ target.name in ['prod','nprd','dev'] }}"
```

#### Running Elementary in Your Dev Environment

Run the following command in your terminal inside your `.venv` environment:

```sh
dbt run --select package:elementary
```

This will create the Elementary tables in your `dev_<developer_name>` dataset.

Every `run`, `build`, `test`, etc., will now be logged in your Elementary tables.

### Cleaning Your Elementary Environment

If you want to validate only your most recent run, you can truncate the Elementary tables. This ensures that they contain only the latest test results.

To clean your dev Elementary tables, call the following procedure or replicate it in your dataset:

```sql
CALL `proj-nprd-003.dev_user.truncate_dev_elementary_tables`('your_project_id', 'your_dataset_name');
```

### Running Tests

There are two ways to test a model:

1. **Building the model**: This means DBT will execute `dbt run && dbt test`. To build your model, run:

```sh
dbt build --select <your_model>
```

2. **Running only the test**:

```sh
dbt test --select <your_model>
```

### Why stored failures currently lack column descriptions

So DBT converts your `select` statement for a generic or singular test into in a `create and replace as` statement during when `stored_failures = true` on a data_test. Sadly according to the official docs and our own testing, these are not considered dbt `models` and do not get the advantages in terms of configurations or contracts. See this [documentation on tests](https://docs.getdbt.com/reference/data-test-configs).

One work around attempted was to `post-hook` and run the SQL statement analogous to this:

`ALTER COLUMN id SET OPTIONS (description='Unique identifier for each row')`

But since a data_test is not a `model`, it does not get the ability to use a post-hook as seen [here](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook).

So for now, we are accepting our stored test failures will have no column level documentation unless we find a work around.
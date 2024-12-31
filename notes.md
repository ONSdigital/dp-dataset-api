# Issues

| Endpoint                                                                                     | Function               | Issue                               | Example                                                                                     |
|---------------------------------------------------------------------------------------------|------------------------|-------------------------------------|---------------------------------------------------------------------------------------------|
| `/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions`                   | `getDimensions`        | `code_list` contains port 22400    | `{{base_url}}/datasets/cpih01/editions/time-series/versions/1/dimensions`                  |
| `/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options` | `getDimensionOptions` | `code` and `code_list` contain port 22400 | `{{base_url}}/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options` |
|                                                                                             |                        |                                     |                                                                                             |
|                                                                                             |                        |                                     |                                                                                             |

## download links

| Endpoint | Function | Example | Extra |
|----------|----------|---------|-------|
|`/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata`|`getMetadata`|`/datasets/cpih01/editions/time-series/versions/1/metadata`| |
|`/datasets/{dataset_id}/editions/{edition}/versions`|`getVersions`| `/datasets/cpih01/editions/time-series/versions` | more links in dimensions|
| `/datasets/{dataset_id}/editions/{edition}/versions/{version}` | `getVersion` | `/datasets/cpih01/editions/time-series/versions/1` | More links in dimensions |
|`/instances`|`getList`|`/instances`|More links in dimensions|
|`/instances/{instance_id}`|`Get`|`/instances/c7f2b593-5927-4efb-afeb-3037949f06f2`|More links in dimensions|
|``|``|``||

| test | complete |
|----------|----------|
| dataset_test    | yes   |
| Row 2    | Data 2   |
| Row 3    | Data 3   |
| Row 4    | Data 4   |
| Row 5    | Data 5   |

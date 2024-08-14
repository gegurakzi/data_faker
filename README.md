### Example conf

```json
{
  "sources": [
    {
      "name": "source1",
      "type": "jdbc",
      "url": "<JDBC url>",
      "user": "<DB user>",
      "password": "<DB password>",
      "tables": [
        {
          "name": "table1",
          "sparsity": "100",
          "columns": [
            {
              "name": "column1",
              "type": "long",
              "constraint": "primary"
            },
            {
              "name": "column2",
              "type": "string",
              "kind": "address"
            }
          ]
        },
        {
          "name": "table2",
          "columns": [
            {
              "name": "column1",
              "type": "long",
              "constraint": "primary"
            },
            {
              "name": "column2",
              "type": "long",
              "constraint": "foreign",
              "refers": "source1.table1.column1"
            },
            {
              "name": "column3",
              "type": "string",
              "kind": "phone"
            },
            {
              "name": "column4",
              "type": "string",
              "kind": "internet.uuid"
            }
          ]
        },
        {
          "name": "table3",
          "columns": [
            {
              "name": "column1",
              "type": "long",
              "constraint": "primary"
            },
            {
              "name": "column2",
              "type": "long",
              "constraint": "foreign",
              "refers": "source1.table1.column1"
            },
            {
              "name": "column3",
              "type": "long",
              "constraint": "foreign",
              "refers": "source1.table2.column1"
            },
            {
              "name": "column4",
              "type": "string",
              "kind": "job.position"
            }
          ]
        }
      ]
    }
  ]
}

```

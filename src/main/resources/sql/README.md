# SQL Files Directory

This directory contains SQL query files that can be referenced from `sql-statements.yml` configuration.

## Benefits of Using SQL Files

1. **No Escaping Required**: Write SQL naturally without worrying about YAML escaping rules
2. **Better IDE Support**: Get syntax highlighting and SQL formatting in `.sql` files
3. **Easier Maintenance**: Complex queries are easier to read and modify in separate files
4. **Version Control**: Track SQL changes more clearly in git

## Usage

In your `sql-statements.yml`, reference SQL files using the `sql-file` property:

```yaml
my_sync_task:
  sql-file: "classpath:sql/my_query.sql"  # Load from classpath
  # or
  sql-file: "sql/my_query.sql"           # Load from file system
  index: my_index
  id-column: ID
```

## File Resolution Order

1. File system (relative to working directory)
2. Classpath resources
3. For classpath resources, also checks for file overrides in the working directory

## Examples

See the provided example files:
- `t_jw_rycw_query.sql` - Simple incremental query
- `ids_str_query.sql` - Complex query with multiple conditions

## Parameters

SQL files support the same parameters as inline SQL:
- `?` - JDBC parameter placeholder for cursor value
- `:chunkSize` - Named parameter for chunk size limit

## Best Practices

1. Use meaningful file names that match the sync task name
2. Add comments in SQL files to document the query purpose
3. Format SQL for readability (multiple lines, proper indentation)
4. Keep related queries in subdirectories if you have many files
# SqlQueryTool
C# class which makes SQL Server query operations more streamlined

Usage:
```cs
class Person
{
  public string FirstName { get; set; }
  public string LastName { get; set; }
  public int Age { get; set; }
  public decimal HeightInCentimeters { get; set; }
}
```

```cs
List<Person> People = new List<Person>();
// Provide your connection string to the SqlQueryTool constructor
SqlQueryTool sqlQueryTool = new SqlQueryTool(connectionString);
// The YourDatabase..Person table has the same schema and field names as the Person class
var dictResult = sqlQueryTool.Select("SELECT * FROM YourDatabase..Person");

dictResult.ForEach(x => People.Add(sqlQueryTool.GetObjectFromDictionary<Person>(x)));
```

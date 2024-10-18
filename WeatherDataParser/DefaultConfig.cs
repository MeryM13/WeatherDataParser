namespace WeatherDataParser
{
    internal class DefaultConfig
    {
        public static readonly string MSSQLDatabaseConnectionString = @"Data Source=localhost;Initial Catalog=WeatherDatabase;Integrated Security=True";
        public static readonly string SQLITEDatabaseConnectionString = @"Data Source=WeatherDatabase; Foreign Keys=True";
        public static readonly DateTime StartingDate = new(2020, 01, 01);
        public static readonly bool ReplaceStationDataIfDiffersFromArchive = true;
        public static readonly string ExcelFilesPath = @"C:\WeatherDataSheets\";
    }
}

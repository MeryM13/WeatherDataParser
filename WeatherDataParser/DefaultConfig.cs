namespace WeatherDataParser
{
    internal class DefaultConfig
    {
        public static readonly string MSSQLDatabaseConnectionString = @"Data Source=localhost;Initial Catalog=WeatherDatabase;Integrated Security=True";
        public static readonly string SQLITEDatabaseConnectionString = "";
        public static readonly DateTime StartingDate = new(2020, 01, 01);
        public static readonly bool ReplaceStationDataIfDiffersFromArchive = true;
    }
}

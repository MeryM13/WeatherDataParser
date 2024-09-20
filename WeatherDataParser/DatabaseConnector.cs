using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using WeatherDataParser.CLASSES;

namespace WeatherDataParser
{
    internal abstract class DatabaseConnector
    {
        internal string _connectionString;

        public abstract void DatabaseConnectionAvailable();
        public abstract DateTime FindLastDate(int stationID);
        public abstract bool StationExists(int stationID);
        public abstract bool DataLineExists(WeatherData weatherData);
        public abstract List<int> GetStationIDList();
        public abstract void AddStation(Station station);
        public abstract void InsertData(WeatherData weatherData);
        public abstract int GetCount(decimal? direction, DateTime from, DateTime to, int stationID);
        public abstract int GetLowSpeedCount(DateTime from, DateTime to, int stationID);
        public abstract decimal GetAll(DateTime from, DateTime to, int stationID);
        public abstract decimal GetAverage(Parameter parameter, DateTime from, DateTime to, int stationID);
        public abstract DataTable GetRaw(string parameters, DateTime from, DateTime to, int stationID);
        public abstract string GetStationName(int stationID);
        public abstract string GetStationDescription(int stationID);
    }
}

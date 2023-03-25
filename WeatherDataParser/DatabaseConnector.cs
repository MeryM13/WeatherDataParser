using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WeatherDataParser.CLASSES;
using static System.Collections.Specialized.BitVector32;

namespace WeatherDataParser
{
    internal class DatabaseConnector
    {
        string _connectionString;
        public DatabaseConnector() 
        {
            _connectionString = DefaultConfig.DatabaseConnectionString;
        }

        public DatabaseConnector(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DateTime FindLastDate(int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select top 1 [Date] from [Data] where Station = @Station order by [Date] desc";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                SqlDataReader reader = cmd.ExecuteReader();
                List<int> list = new();
                if (reader.Read())
                {
                    return reader.GetDateTime(0);
                }
                throw new Exception();
            }
        }

        public bool StationExists(int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select * from Station where ID = @ID";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", stationID);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return true;
                }
                return false;
            }
        }

        public bool DataLineExists(WeatherData weatherData)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select * from [Data] where Station = @Station and Date = @Date";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", weatherData.StationID);
                cmd.Parameters.AddWithValue("Date", weatherData.Date);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return true;
                }
                return false;
            }
        }

        public List<int> GetStationIDList()
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select ID from [Station]";
                SqlCommand cmd = new(sql, conn);
                SqlDataReader reader = cmd.ExecuteReader();
                List<int> list = new();
                while (reader.Read())
                {
                    list.Add(reader.GetInt32(0));
                }
                return list;
            }
        }

        public void AddStation(Station station)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "insert into Station values (@ID, @Name, @Location, @Latitude, @Longitude, @Height)";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", station.ID);
                cmd.Parameters.AddWithValue("Name", station.Name);
                cmd.Parameters.AddWithValue("Location", station.Location);
                cmd.Parameters.AddWithValue("Latitude", station.Latitude);
                cmd.Parameters.AddWithValue("Longitude", station.Longitude);
                cmd.Parameters.AddWithValue("Height", station.Height);
                cmd.ExecuteNonQuery();
            }
        }

        public void InsertData(WeatherData weatherData)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "insert into [Data](Station, [Date], Wind_Direction, Wind_Speed, Temperature, Humidity, Pressure, Snow_Height) " +
                    "values (@Station, @Date, @WindDirection, @WindSpeed, @Temperature, @Humidity, @Pressure, @SnowHeight)";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", weatherData.StationID);
                cmd.Parameters.AddWithValue("Date", weatherData.Date);
                cmd.Parameters.AddWithValue("WindDirection", weatherData.WindDirection == null ? DBNull.Value : weatherData.WindDirection);
                cmd.Parameters.AddWithValue("WindSpeed", weatherData.WindSpeed);
                cmd.Parameters.AddWithValue("Temperature", weatherData.Temperature);
                cmd.Parameters.AddWithValue("Humidity", weatherData.Humidity);
                cmd.Parameters.AddWithValue("Pressure", weatherData.Pressure);
                cmd.Parameters.AddWithValue("SnowHeight", weatherData.SnowHeight == null ? DBNull.Value : weatherData.SnowHeight);
                cmd.ExecuteNonQuery();
            }
        }
    }
}

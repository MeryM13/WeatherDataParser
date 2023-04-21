using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using WeatherDataParser.CLASSES;

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

        public void AddStation(int ID, string name, string location, decimal latitude, decimal longitude, decimal height)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "insert into Station values (@ID, @Name, @Location, @Latitude, @Longitude, @Height)";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", ID);
                cmd.Parameters.AddWithValue("Name", name);
                cmd.Parameters.AddWithValue("Location", location);
                cmd.Parameters.AddWithValue("Latitude", latitude);
                cmd.Parameters.AddWithValue("Longitude", longitude);
                cmd.Parameters.AddWithValue("Height", height);
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

        public int GetCount(decimal? direction, DateTime from, DateTime to, int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql;
                if (direction == null)
                    sql = "select count(*) from [Data] where Station = @Station and Wind_Direction is null and Date between @From and @To";
                else
                    sql = "select count(*) from [Data] where Station = @Station and Wind_Direction = @Direction and Date between @From and @To";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to);
                if (direction != null)
                {
                    cmd.Parameters.AddWithValue("Direction", direction);
                }
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetInt32(0);
                }
                throw new Exception("Couldn't get count");
            }
        }

        public decimal GetAll(DateTime from, DateTime to, int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select count(*) from [Data] where Station = @Station and Date between @From and @To";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetInt32(0);
                }
                throw new Exception("Couldn't get count");
            }
        }

        public decimal GetAverage(CLASSES.Parameter parameter, DateTime from, DateTime to, int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = $"select avg({parameter}) from [Data] where Station = @Station and Date between @From and @To";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    if (parameter == CLASSES.Parameter.Wind_Speed || parameter == CLASSES.Parameter.Humidity || parameter == CLASSES.Parameter.Snow_Height)
                    {
                        try
                        {
                            return reader.GetInt32(0);
                        }
                        catch (SqlNullValueException) { return 0; }
                    }
                    else
                    {
                        try
                        {
                            return reader.GetDecimal(0);
                        }
                        catch (SqlNullValueException) { return 0; }
                    }
                }
                throw new Exception();
            }
        }

        public DataTable GetRaw(string parameters, DateTime from, DateTime to, int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = $"select {parameters} from [Data] where Station = @Station and Date between @From and @To";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to);
                SqlDataAdapter adapter = new SqlDataAdapter(sql, conn);
                adapter.SelectCommand = cmd;
                DataSet ds = new();
                adapter.Fill(ds);
                return ds.Tables[0];
                throw new Exception("Couldn't get count");
            }
        }

        public string GetStationName(int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select Name from Station where ID = @ID";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", stationID);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return $"{reader.GetString(0)} ({stationID})";
                }
                throw new Exception();
            }
        }

        public string GetStationDescription(int stationID)
        {
            using (SqlConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select Location, Latitude, Longitude, Height from Station where ID = @ID";
                SqlCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", stationID);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return $"Расположение: {reader.GetString(0)}; Географические координаты: широта: {reader.GetDecimal(1)}, " +
                        $"долгота: {reader.GetDecimal(2)}, высота над уровнем моря: {reader.GetDecimal(3)}";
                }
                throw new Exception();
            }
        }
    }
}

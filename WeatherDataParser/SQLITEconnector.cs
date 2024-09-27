using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data.SqlTypes;
using System.Data;
using WeatherDataParser.CLASSES;


namespace WeatherDataParser
{
    internal class SQLITEconnector : DatabaseConnector
    {
        public SQLITEconnector() 
        {
            _connectionString = DefaultConfig.SQLITEDatabaseConnectionString;
        }

        public SQLITEconnector(string connectionString)
        {
            _connectionString = connectionString;
        }

        public override void CreateDatabase()
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "CREATE TABLE Station (ID integer NOT NULL PRIMARY KEY, " +
                                                                "[Name] text NOT NULL, " +
                                                                "[Location] text NOT NULL, " +
                                                                "Latitude decimal(5,2) NOT NULL, " +
                                                                "Longitude decimal(5,2) NOT NULL, " +
                                                                "Height decimal(5,2) NOT NULL); " +
                            "CREATE TABLE Data (Station integer NOT NULL REFERENCES Station(ID), " +
                                                            "[Date] date NOT NULL, " +
                                                            "Wind_Direction decimal(6,2), " +
                                                            "Wind_Speed integer NOT NULL, " +
                                                            "Temperature decimal(6,2) NOT NULL, " +
                                                            "Humidity integer NOT NULL, " +
                                                            "Pressure decimal(6,2) NOT NULL, " +
                                                            "Snow_Height integer, " +
                                                            "PRIMARY KEY (Station, [Date]));";
                SQLiteCommand cmd = new(sql, conn);
                cmd.ExecuteNonQuery();
                sql = "INSERT INTO Station VALUES" +
                    "(30309,'Братск','Иркутская область, Россия',56.28,101.7,416), " +
                    "(30509,'Саянск','Иркутская область, Россия',54.05,102.1,550), " +
                    "(30603,'Зима','Иркутская область, Россия',53.93,102,458), " +
                    "(30617,'Черемхово','Иркутская область, Россия',53.18,103,598), " +
                    "(30710,'Иркутск','Иркутская область, Россия',52.27,104.3,469), " +
                    "(30711,'Шелехов','Иркутская область, Россия',52.2,104,458), " +
                    "(30715,'Ангарск','Иркутская область, Россия',52.48,103.8,437), " +
                    "(30716,'Хомутово','Иркутская область, Россия',52.46,104.3,454), " +
                    "(30758,'Чита','Забайкальский край, Россия',52.08,113.4,671), " +
                    "(30818,'Байкальск','Иркутская область, Россия',51.65,104.1,478)";
                cmd = new(sql, conn);
                cmd.ExecuteNonQuery();
            }
        }

        public override void DatabaseConnectionAvailable()
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                try
                {
                    conn.Open();
                    CreateDatabase();
                }
                catch (SQLiteException)
                {

                }
            }
        }

        public override DateTime FindLastDate(int stationID)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select [Date] from [Data] where Station = @Station order by [Date] desc limit 1";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                SQLiteDataReader reader = cmd.ExecuteReader();
                List<int> list = new();
                if (reader.Read())
                {
                    return reader.GetDateTime(0);
                }
                throw new Exception("Не удалось обнаружить дату последней записи данных выбранной метеостанции");
            }
        }

        public override bool StationExists(int stationID)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select * from Station where ID = @ID";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", stationID);
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return true;
                }
                return false;
            }
        }

        public override bool DataLineExists(WeatherData weatherData)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {

                conn.Open();
                string sql = "select * from [Data] where Station = @Station and Date = @Date";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", weatherData.StationID);
                cmd.Parameters.AddWithValue("Date", weatherData.Date);
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return true;
                }
                return false;
            }
        }

        public override List<int> GetStationIDList()
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select ID from [Station]";
                SQLiteCommand cmd = new(sql, conn);
                SQLiteDataReader reader = cmd.ExecuteReader();
                List<int> list = new();
                while (reader.Read())
                {
                    list.Add(reader.GetInt32(0));
                }
                return list;
            }
        }

        public override void AddStation(Station station)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "insert into Station values (@ID, @Name, @Location, @Latitude, @Longitude, @Height)";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", station.ID);
                cmd.Parameters.AddWithValue("Name", station.Name);
                cmd.Parameters.AddWithValue("Location", station.Location);
                cmd.Parameters.AddWithValue("Latitude", station.Latitude);
                cmd.Parameters.AddWithValue("Longitude", station.Longitude);
                cmd.Parameters.AddWithValue("Height", station.Height);
                cmd.ExecuteNonQuery();
            }
        }

        public override void InsertData(WeatherData weatherData)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "insert into [Data](Station, [Date], Wind_Direction, Wind_Speed, Temperature, Humidity, Pressure, Snow_Height) " +
                    "values (@Station, @Date, @WindDirection, @WindSpeed, @Temperature, @Humidity, @Pressure, @SnowHeight)";
                SQLiteCommand cmd = new(sql, conn);
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

        public override int GetCount(decimal? direction, DateTime from, DateTime to, int stationID)
        {
            int count = GetAll(from, to, stationID);
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql;
                if (direction == null)
                    sql = $"select count(*) from [Data] where Station = @Station and Wind_Direction is null and Date between @From and @To limit {count}";
                else
                    sql = $"select count(*) from [Data] where Station = @Station and Wind_Direction = @Direction and Date between @From and @To limit {count}";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to + TimeSpan.FromDays(1));
                if (direction != null)
                {
                    cmd.Parameters.AddWithValue("Direction", direction);
                }
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetInt32(0);
                }
                throw new Exception("Не получилось подсчитать количество записей");
            }
        }

        public override int GetCount(decimal? direction, DateTime from, DateTime to, int stationID, int maxSpeed)
        {
            int count = GetAll(from, to, stationID);
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql;
                if (direction == null)
                    sql = $"select count(*) from [Data] where Station = @Station and Wind_Direction is null and Date between @From and @To and Wind_Speed < @MaxSpeed limit {count}";
                else
                    sql = $"select count(*) from [Data] where Station = @Station and Wind_Direction = @Direction and Date between @From and @To and Wind_Speed < @MaxSpeed limit {count}";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to + TimeSpan.FromDays(1));
                cmd.Parameters.AddWithValue("MaxSpeed", maxSpeed);
                if (direction != null)
                {
                    cmd.Parameters.AddWithValue("Direction", direction);
                }
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetInt32(0);
                }
                throw new Exception("Не получилось подсчитать количество записей");
            }
        }

        public override int GetLowSpeedCount(DateTime from, DateTime to, int stationID)
        {
            int count = GetAll(from, to, stationID);
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = $"select count(*) from [Data] where Station = @Station and Wind_Speed < 3 and Date between @From and @To limit {count}";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to + TimeSpan.FromDays(1));
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetInt32(0);
                }
                throw new Exception("Не получилось подсчитать количество записей");
            }
        }

        public override int GetAll(DateTime from, DateTime to, int stationID)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select count(*) from [Data] where Station = @Station and Date between @From and @To";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to + TimeSpan.FromDays(1));
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetInt32(0) - 1;
                }
                throw new Exception("Не получилось подсчитать общее количество записей");
            }
        }

        public override decimal GetAverage(Parameter parameter, DateTime from, DateTime to, int stationID)
        {
            int count = GetAll(from, to, stationID);
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = $"select avg({parameter}) from [Data] where Station = @Station and Date between @From and @To limit {count}";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to + TimeSpan.FromDays(1));
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    if (parameter == Parameter.Wind_Speed || parameter == Parameter.Humidity || parameter == Parameter.Snow_Height)
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
                throw new Exception("Не получилось подсчитать среднее значение");
            }
        }

        public override DataTable GetRaw(string parameters, DateTime from, DateTime to, int stationID)
        {
            int count = GetAll(from, to, stationID);
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = $"select {parameters} from [Data] where Station = @Station and Date between @From and @To limit {count}";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("Station", stationID);
                cmd.Parameters.AddWithValue("From", from);
                cmd.Parameters.AddWithValue("To", to + TimeSpan.FromDays(1));
                SQLiteDataAdapter adapter = new(sql, conn)
                {
                    SelectCommand = cmd
                };
                DataSet ds = new();
                adapter.Fill(ds);
                return ds.Tables[0];
                throw new Exception("Данные не найдены");
            }
        }

        public override string GetStationName(int stationID)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select Name from Station where ID = @ID";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", stationID);
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return $"{reader.GetString(0)} ({stationID})";
                }
                throw new Exception("Метеостанция с таким индексом не найдена в базе данных");
            }
        }

        public override string GetStationDescription(int stationID)
        {
            using (SQLiteConnection conn = new(_connectionString))
            {
                conn.Open();
                string sql = "select Location, Latitude, Longitude, Height from Station where ID = @ID";
                SQLiteCommand cmd = new(sql, conn);
                cmd.Parameters.AddWithValue("ID", stationID);
                SQLiteDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    return $"Расположение: {reader.GetString(0)}; Географические координаты: широта: {reader.GetDecimal(1)}, " +
                        $"долгота: {reader.GetDecimal(2)}, высота над уровнем моря: {reader.GetDecimal(3)}";
                }
                throw new Exception("Метеостанция с таким индексом не найдена в базе данных");
            }
        }
    }
}

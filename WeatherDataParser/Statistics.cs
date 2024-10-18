using Microsoft.VisualBasic;
using System.Data;

namespace WeatherDataParser
{
    public class Statistics
    {
        readonly DateTime _from;
        readonly DateTime _to;
        readonly int _stationID;
        readonly DatabaseConnector _connector;
        public Statistics(DateTime from, DateTime to, int stationID)
        {
            _from = from;
            _to = to;
            _stationID = stationID;
            _connector = new SQLITEconnector();
            try
            {
                _connector.DatabaseConnectionAvailable();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        public Statistics(DateTime from, DateTime to, int stationID, string connectionString)
        {
            _from = from;
            _to = to;
            _stationID = stationID;
            _connector = new SQLITEconnector(connectionString);
            try
            {
                _connector.DatabaseConnectionAvailable();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        public Statistics(DateTime from, DateTime to, int stationID, int mode)
        {
            _from = from;
            _to = to;
            _stationID = stationID;
            switch (mode)
            {
                case 1:
                    {
                        _connector = new SQLITEconnector();
                        break;
                    }
                case 2:
                    {
                        _connector = new MSSQLconnector();
                        break;
                    }
                default: { throw new Exception("Unknown database mode"); }
            }
            try
            {
                _connector.DatabaseConnectionAvailable();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        public Statistics(DateTime from, DateTime to, int stationID, string connectionString, int mode)
        {
            _from = from;
            _to = to;
            _stationID = stationID;
            switch (mode)
            {
                case 1:
                    {
                        _connector = new SQLITEconnector(connectionString);
                        break;
                    }
                case 2:
                    {
                        _connector = new MSSQLconnector(connectionString);
                        break;
                    }
                default: { throw new Exception("Unknown database mode"); }
            }
            try
            {
                _connector.DatabaseConnectionAvailable();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        public Dictionary<decimal, decimal> GetWindRose(bool distributeCalm, int numberOfDirections)
        {
            Dictionary<decimal, decimal> windCount = new();
            if (numberOfDirections != 8 && numberOfDirections != 16)
            { throw new ArgumentException("Указано неверное количество румбов розы"); }

            List<int> windDirections = new() { 0, 45, 90, 135, 180, 225, 270, 315 };
            foreach (int direction in windDirections)
            {
                windCount.Add(direction, _connector.GetCount(direction, _from, _to, _stationID));
            }

            if (numberOfDirections == 16)
            {
                List<decimal> innerDirections = new() { 22.5m, 67.5m, 112.5m, 157.5m, 202.5m, 247.5m, 292.5m, 337.5m };
                foreach (decimal direction in innerDirections)
                    windCount.Add(direction, 0);

                int quarter = (int)Math.Round(windCount[windDirections[0]] / 4);
                windCount[innerDirections[0]] += quarter;
                windCount[innerDirections[7]] += quarter;
                windCount[windDirections[0]] -= quarter * 2;
                for (int i = 1; i < 8; i++)
                {
                    quarter = (int)Math.Round(windCount[windDirections[i]] / 4);
                    windCount[innerDirections[i - 1]] += quarter;
                    windCount[innerDirections[i]] += quarter;
                    windCount[windDirections[i]] -= quarter * 2;
                }
                windCount = windCount.OrderBy(obj => obj.Key).ToDictionary(obj => obj.Key, obj => obj.Value);
            }

            if (distributeCalm)
            {
                int avgCalmPerDirection = (int)Math.Round((decimal)(GetCalmCount() / numberOfDirections));
                foreach (var key in windCount.Keys)
                {
                    windCount[key] += avgCalmPerDirection;
                }
            }

            return windCount;
        }

        public Dictionary<decimal, decimal> GetWindRose(bool distributeCalm, int numberOfDirections, int maxSpeed)
        {
            Dictionary<decimal, decimal> windCount = new();
            if (numberOfDirections != 8 && numberOfDirections != 16)
            { throw new ArgumentException("Указано неверное количество румбов розы"); }

            List<int> windDirections = new() { 0, 45, 90, 135, 180, 225, 270, 315 };
            foreach (int direction in windDirections)
            {
                windCount.Add(direction, _connector.GetCount(direction, _from, _to, _stationID, maxSpeed));
            }

            if (numberOfDirections == 16)
            {
                List<decimal> innerDirections = new() { 22.5m, 67.5m, 112.5m, 157.5m, 202.5m, 247.5m, 292.5m, 337.5m };
                foreach (decimal direction in innerDirections)
                    windCount.Add(direction, 0);

                int quarter = (int)Math.Round(windCount[windDirections[0]] / 4);
                windCount[innerDirections[0]] += quarter;
                windCount[innerDirections[7]] += quarter;
                windCount[windDirections[0]] -= quarter * 2;
                for (int i = 1; i < 8; i++)
                {
                    quarter = (int)Math.Round(windCount[windDirections[i]] / 4);
                    windCount[innerDirections[i - 1]] += quarter;
                    windCount[innerDirections[i]] += quarter;
                    windCount[windDirections[i]] -= quarter * 2;
                }
                windCount = windCount.OrderBy(obj => obj.Key).ToDictionary(obj => obj.Key, obj => obj.Value);
            }

            if (distributeCalm)
            {
                int avgCalmPerDirection = (int)Math.Round((decimal)(GetCalmCount() / numberOfDirections));
                foreach (var key in windCount.Keys)
                {
                    windCount[key] += avgCalmPerDirection;
                }
            }

            return windCount;
        }

        public Dictionary<int, Dictionary<decimal, decimal>> GetDifferentiatedWindRose(bool distributeCalm, int numberOfDirections, List<int> maxSpeeds)
        {
            Dictionary<int, Dictionary<decimal, decimal>> differentiatedRose = new();

            foreach(int maxSpeed in maxSpeeds)
            {
                differentiatedRose.Add(maxSpeed, GetWindRose(distributeCalm,numberOfDirections,maxSpeed));
            }

            return differentiatedRose;
        }

        public Dictionary<int, Dictionary<decimal, decimal>> GetDifferentiatedPercentageWindRose(bool distributeCalm, int numberOfDirections, List<int> maxSpeeds)
        {
            Dictionary<int, Dictionary<decimal, decimal>> differentiatedRose = new();

            foreach (int maxSpeed in maxSpeeds)
            {
                differentiatedRose.Add(maxSpeed, GetPercentageWindRose(distributeCalm, numberOfDirections, maxSpeed));
            }

            return differentiatedRose;
        }

        public Dictionary<decimal, decimal> GetPercentageWindRose(bool distributeCalm, int numberOfDirections)
        {
            Dictionary<decimal, decimal> windCount = new();
            if (numberOfDirections != 8 && numberOfDirections != 16)
            { throw new ArgumentException("Указано неверное количество румбов розы"); }

            List<int> windDirections = new() { 0, 45, 90, 135, 180, 225, 270, 315 };
            foreach (int direction in windDirections)
            {
                windCount.Add(direction, _connector.GetCount(direction, _from, _to, _stationID));
            }

            if (numberOfDirections == 16)
            {
                List<decimal> innerDirections = new() { 22.5m, 67.5m, 112.5m, 157.5m, 202.5m, 247.5m, 292.5m, 337.5m };
                foreach (decimal direction in innerDirections)
                    windCount.Add(direction, 0);

                int quarter = (int)Math.Round(windCount[windDirections[0]] / 4);
                windCount[innerDirections[0]] += quarter;
                windCount[innerDirections[7]] += quarter;
                windCount[windDirections[0]] -= quarter * 2;
                for (int i = 1; i < 8; i++)
                {
                    quarter = (int)Math.Round(windCount[windDirections[i]] / 4);
                    windCount[innerDirections[i - 1]] += quarter;
                    windCount[innerDirections[i]] += quarter;
                    windCount[windDirections[i]] -= quarter * 2;
                }
                windCount = windCount.OrderBy(obj => obj.Key).ToDictionary(obj => obj.Key, obj => obj.Value);
            }

            if (distributeCalm)
            {
                int avgCalmPerDirection = (int)Math.Round((decimal)(GetCalmCount() / numberOfDirections));
                foreach (var key in windCount.Keys)
                {
                    windCount[key] += avgCalmPerDirection;
                }
            }

            int sum = GetAll();

            foreach (var key in windCount.Keys)
                windCount[key] = Math.Round(windCount[key] / sum, 3);

            return windCount;
        }

        public Dictionary<decimal, decimal> GetPercentageWindRose(bool distributeCalm, int numberOfDirections, int maxSpeed)
        {
            Dictionary<decimal, decimal> windCount = new();
            if (numberOfDirections != 8 && numberOfDirections != 16)
            { throw new ArgumentException("Указано неверное количество румбов розы"); }

            List<int> windDirections = new() { 0, 45, 90, 135, 180, 225, 270, 315 };
            foreach (int direction in windDirections)
            {
                windCount.Add(direction, _connector.GetCount(direction, _from, _to, _stationID, maxSpeed));
            }

            if (numberOfDirections == 16)
            {
                List<decimal> innerDirections = new() { 22.5m, 67.5m, 112.5m, 157.5m, 202.5m, 247.5m, 292.5m, 337.5m };
                foreach (decimal direction in innerDirections)
                    windCount.Add(direction, 0);

                int quarter = (int)Math.Round(windCount[windDirections[0]] / 4);
                windCount[innerDirections[0]] += quarter;
                windCount[innerDirections[7]] += quarter;
                windCount[windDirections[0]] -= quarter * 2;
                for (int i = 1; i < 8; i++)
                {
                    quarter = (int)Math.Round(windCount[windDirections[i]] / 4);
                    windCount[innerDirections[i - 1]] += quarter;
                    windCount[innerDirections[i]] += quarter;
                    windCount[windDirections[i]] -= quarter * 2;
                }
                windCount = windCount.OrderBy(obj => obj.Key).ToDictionary(obj => obj.Key, obj => obj.Value);
            }

            if (distributeCalm)
            {
                int avgCalmPerDirection = (int)Math.Round((decimal)(GetCalmCount() / numberOfDirections));
                foreach (var key in windCount.Keys)
                {
                    windCount[key] += avgCalmPerDirection;
                }
            }

            int sum = GetAll();

            foreach (var key in windCount.Keys)
                windCount[key] = Math.Round(windCount[key] / sum, 3);

            return windCount;
        }

        public int GetAll()
        {
            return _connector.GetAll(_from, _to, _stationID);
        }

        public int GetCalmCount()
        {
            return _connector.GetCount(null, _from, _to, _stationID);
        }

        public decimal GetCalmPeriodicity()
        {
            return GetCalmCount() / (decimal)GetAll();
        }

        public decimal GetCalmPeriodicity(int roundUp)
        {
            return Math.Round((decimal)(GetCalmCount() / GetAll()), roundUp);
        }

        public int GetLowSpeedCount()
        {
            return _connector.GetLowSpeedCount(_from, _to, _stationID);
        }

        public decimal GetWeakPeriodicity()
        {
            return GetLowSpeedCount() / (decimal)GetAll();
        }

        public decimal GetWeakPeriodicity(int roundUp)
        {
            return Math.Round((decimal)(GetLowSpeedCount() / GetAll()), roundUp);
        }

        public Dictionary<DateTime, decimal> GetWindPeriodicityChart(decimal? direction, DateInterval interval)
        {
            Dictionary<DateTime, decimal> chart = new();
            DateTime date = _from;
            while (date < _to)
            {
                DateTime getTo = DateAndTime.DateAdd(interval, 1, date);
                try
                {
                    chart.Add(date, _connector.GetCount(direction, date, getTo, _stationID) / (decimal)GetAll());
                }
                catch (DivideByZeroException)
                {
                    chart.Add(date, 0);
                }
                date = getTo;
            }
            return chart;
        }

        public Dictionary<DateTime, decimal> GetAveragesChart(Parameter parameter, DateInterval interval)
        {
            Dictionary<DateTime, decimal> chart = new();
            DateTime date = _from;
            while (date < _to)
            {
                DateTime getTo = DateAndTime.DateAdd(interval, 1, date);
                chart.Add(date, _connector.GetAverage(parameter, date, getTo, _stationID));
                date = getTo;
            }
            return chart;
        }

        public DataTable GetRawData(List<Parameter> parameters, bool useStationNameInsteadOfID, bool divideDateAndTime, bool onlyDate)
        {
            string includes = string.Join(", ", parameters.ToArray());
            DataTable rawTable = _connector.GetRaw(includes, _from, _to, _stationID);

            if (useStationNameInsteadOfID)
            {
                rawTable.Columns.Add("Station_Name", typeof(string));
                int index = rawTable.Columns.IndexOf("Station_Name");
                string name = _connector.GetStationName(_stationID);
                foreach (DataRow row in rawTable.Rows)
                {
                    row[index] = name;
                }
                if (rawTable.Columns.Contains("Station"))
                    rawTable.Columns.Remove("Station");
            }

            if (divideDateAndTime)
            {
                if (rawTable.Columns.Contains("Date"))
                {
                    rawTable.Columns.Add("Date_Only", typeof(string));
                    rawTable.Columns.Add("Time_Only", typeof(string));
                    foreach (DataRow row in rawTable.Rows)
                    {
                        row["Date_Only"] = ((DateTime)row["Date"]).ToString("d");
                        row["Time_Only"] = ((DateTime)row["Date"]).ToString("t");
                    }
                    rawTable.Columns.Remove("Date");
                }
            }

            if (onlyDate && !divideDateAndTime)
            {
                if (rawTable.Columns.Contains("Date"))
                {
                    rawTable.Columns.Add("Date_Only", typeof(string));
                    foreach (DataRow row in rawTable.Rows)
                    {
                        row["Date_Only"] = ((DateTime)row["Date"]).ToString("d");
                    }
                    rawTable.Columns.Remove("Date");
                }
            }

            if (onlyDate && divideDateAndTime)
            {
                rawTable.Columns.Remove("Time_Only");
            }

            string[] columnNames = new string[] { "Station", "Station_Name", "Date_Only", "Time_Only", "Date", "Wind_Direction", "Wind_Speed", "Temperature", "Humidity", "Pressure", "Snow_Height" };
            int i = 0;
            foreach (string name in columnNames)
            {
                if (rawTable.Columns.Contains(name))
                {
                    rawTable.Columns[name].SetOrdinal(i);
                    i++;
                    switch (name)
                        {
                        case "Station":
                            {
                                rawTable.Columns[name].ColumnName = "Индекс ВМО";
                                break;
                            }
                        case "Station_Name":
                            {
                                rawTable.Columns[name].ColumnName = "Название метеостанции";
                                break;
                            }
                        case "Date_Only":
                            {
                                rawTable.Columns[name].ColumnName = "Дата измерения";
                                break;
                            }
                        case "Time_Only":
                            {
                                rawTable.Columns[name].ColumnName = "Время измерения";
                                break;
                            }
                        case "Date":
                            {
                                rawTable.Columns[name].ColumnName = "Дата и время измерения";
                                break;
                            }
                        case "Wind_Direction":
                            {
                                rawTable.Columns[name].ColumnName = "Направление ветра, градусы";
                                break;
                            }
                        case "Wind_Speed":
                            {
                                rawTable.Columns[name].ColumnName = "Скорость ветра, мс";
                                break;
                            }
                        case "Temperature":
                            {
                                rawTable.Columns[name].ColumnName = "Температура воздуха, градусы С";
                                break;
                            }
                        case "Humidity":
                            {
                                rawTable.Columns[name].ColumnName = "Относительная влажность воздуха, %";
                                break;
                            }
                        case "Pressure":
                            {
                                rawTable.Columns[name].ColumnName = "Атмосферное давление, гПа";
                                break;
                            }
                        case "Snow_Height":
                            {
                                rawTable.Columns[name].ColumnName = "Высота снежного покрова, см";
                                break;
                            }
                    }
                }
            }
            return rawTable;
        }
    }
}

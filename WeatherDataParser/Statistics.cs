using Microsoft.VisualBasic;
using System.Data;

namespace WeatherDataParser
{
    public class Statistics
    {
        DateTime _from;
        DateTime _to;
        int _stationID;
        DatabaseConnector _connector;
        public Statistics(DateTime from, DateTime to, int stationID)
        {
            _from = from;
            _to = to;
            _stationID = stationID;
            _connector = new DatabaseConnector();
        }

        public Statistics(DateTime from, DateTime to, int stationID, string connectionString)
        {
            _from = from;
            _to = to;
            _stationID = stationID;
            _connector = new DatabaseConnector(connectionString);
        }

        public Dictionary<decimal, decimal> GetWindRose(bool distributeCalm, int numberOfDirections)
        {
            Dictionary<decimal, decimal> windCount = new();
            if (numberOfDirections != 8 && numberOfDirections != 16)
            { throw new ArgumentException(); }

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
                windCount[innerDirections[7]] = +quarter;
                windCount[windDirections[0]] -= quarter * 2;
                for (int i = 1; i < 8; i++)
                {
                    quarter = (int)Math.Round(windCount[windDirections[i]] / 4);
                    windCount[innerDirections[i - 1]] += quarter;
                    windCount[innerDirections[i]] = +quarter;
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

        public Dictionary<decimal, decimal> GetPercentageWindRose(bool distributeCalm, int numberOfDirections)
        {
            Dictionary<decimal, decimal> windCount = new();
            if (numberOfDirections != 8 && numberOfDirections != 16)
            { throw new ArgumentException(); }

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
                windCount[innerDirections[7]] = +quarter;
                windCount[windDirections[0]] -= quarter * 2;
                for (int i = 1; i < 8; i++)
                {
                    quarter = (int)Math.Round(windCount[windDirections[i]] / 4);
                    windCount[innerDirections[i - 1]] += quarter;
                    windCount[innerDirections[i]] = +quarter;
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

            int sum = 0;
            foreach (var key in windCount.Keys)
                sum += (int)windCount[key];

            foreach (var key in windCount.Keys)
                windCount[key] = Math.Round(windCount[key] / sum, 3);

            return windCount;
        }

        public int GetCalmCount()
        {
            return _connector.GetCount(null, _from, _to, _stationID);
        }

        public decimal GetCalmPeriodicity()
        {
            return GetCalmCount() / _connector.GetAll(_from, _to, _stationID);
        }

        public decimal GetCalmPeriodicity(int roundUp)
        {
            return Math.Round(GetCalmCount() / _connector.GetAll(_from, _to, _stationID), roundUp);
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
                    chart.Add(date, _connector.GetCount(direction, date, getTo, _stationID) / _connector.GetAll(date, getTo, _stationID));
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
                }
            }
            return rawTable;
        }
    }
}

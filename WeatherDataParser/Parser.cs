using System.Text;
using WeatherDataParser.CLASSES;
using HtmlAgilityPack;
using System.Net.NetworkInformation;

namespace WeatherDataParser
{
    public class Parser
    {
        readonly DateTime _startingDate;
        readonly DatabaseConnector _connector;

        public Parser()
        {
            _startingDate = DefaultConfig.StartingDate;
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

        public Parser(DateTime startingDate)
        {
            _startingDate = startingDate;
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

        public Parser(string connectionString)
        {
            _startingDate = DefaultConfig.StartingDate;
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

        public Parser(DateTime startingDate, string connectionString)
        {
            _startingDate = startingDate;
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

        public Parser(int mode)
        {
            _startingDate = DefaultConfig.StartingDate;
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

        public Parser(DateTime startingDate, int mode)
        {
            _startingDate = startingDate;
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

        public Parser(string connectionString, int mode)
        {
            _startingDate = DefaultConfig.StartingDate;
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

        public Parser(DateTime startingDate, string connectionString, int mode)
        {
            _startingDate = startingDate;
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

        public void UpdateStationData(int stationID)
        {
            if (!InternetConnectionAvailable())
            {
                throw new Exception("Нет подключения к сети Интернет");
            }

            if (_connector.StationExists(stationID))
            {
                try
                {
                    DateTime lastDate = _connector.FindLastDate(stationID);
                    ParseForStation(stationID, lastDate);
                }
                catch (Exception)
                {
                    ParseForStation(stationID, _startingDate);
                }
            }
            else
            {
                Console.WriteLine($"Unknown station {stationID}. Do you want to add it to the database? y/n");
                if (Console.ReadLine() == "y")
                {
                    AddStation(FindStation(stationID));
                }
            }
        }

        public Station FindStation(int stationID)
        {
            if (!InternetConnectionAvailable())
            {
                throw new Exception("Нет подключения к сети Интернет");
            }

            if (_connector.StationExists(stationID))
            {
                throw new Exception("Данная станция уже присутствует в базе данных");
            }

            string url = $@"http://www.pogodaiklimat.ru/weather.php?id={stationID}";
            var doc = GetDocument(url);

            var headline = doc.DocumentNode.SelectSingleNode("/html/body/div[1]/main/div/div/div/div/div/div[5]/div[2]/div/div[1]/h1");
            if (headline.InnerText.Contains("(, )"))
                throw new Exception("Метеостанция не найдена в архиве");

            var archiveText = doc.DocumentNode.SelectSingleNode("/html/body/div[1]/main/div/div/div/div/div/div[5]/div[2]/div/div[5]");
            return new()
            {
                ID = stationID,
                Name = archiveText.SelectSingleNode("span[1]").InnerText.Split('(')[0].Trim(),
                Location = archiveText.SelectSingleNode("span[1]").InnerText.Split('(', ')')[1].Trim(),
                Latitude = decimal.Parse(archiveText.SelectSingleNode("span[2]").InnerText.Replace(".", ",")),
                Longitude = decimal.Parse(archiveText.SelectSingleNode("span[3]").InnerText.Replace(".", ",")),
                Height = decimal.Parse(archiveText.SelectSingleNode("span[4]").InnerText.Split(' ')[0].Replace(".", ","))
            };
        }

        public void AddStation(Station newStation)
        {
            if (!InternetConnectionAvailable())
            {
                throw new Exception("Нет подключения к сети Интернет");
            }

            try
            {
                if (newStation != FindStation(newStation.ID))
                {
                    if (DefaultConfig.ReplaceStationDataIfDiffersFromArchive)
                    {
                        newStation = FindStation(newStation.ID);
                    }
                    else
                    {
                        throw new Exception("Введенные данные не совпадают с данными архива");
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }

            _connector.AddStation(newStation);
            ParseForStation(newStation.ID, _startingDate);
        }

        public void FullUpdate()
        {
            if (!InternetConnectionAvailable())
            {
                throw new Exception("Нет подключения к сети Интернет");
            }

            List<int> StationIDList = _connector.GetStationIDList();
            foreach (var station in StationIDList)
            {
                UpdateStationData(station);
            }
        }

        public List<string> GetStationNamesList()
        {
            List<string> list = new();
            foreach (int id in _connector.GetStationIDList())
            {
                list.Add(_connector.GetStationName(id));
            }
            return list;
        }

        public string GetStationInfo(int stationId)
        {
            return _connector.GetStationDescription(stationId);
        }

        void ParseForStation(int station, DateTime startDate)
        {
            DateTime currentDate = DateTime.Now;
            if (startDate.Year == currentDate.Year)
            {
                if (startDate.Month == currentDate.Month)
                {
                    GetData(station, startDate.Day, currentDate.Day, currentDate.Month, currentDate.Year);
                }
                else
                {
                    GetData(station, startDate.Day, GetLastDayOfMonth(startDate.Month, currentDate.Year), startDate.Month, currentDate.Year);
                    for (int month = startDate.Month + 1; month <= currentDate.Month - 1; month++)
                    {
                        GetData(station, 1, GetLastDayOfMonth(month, currentDate.Year), month, currentDate.Year);
                    }
                    GetData(station, 1, currentDate.Day, currentDate.Month, currentDate.Year);
                }
            }
            else
            {
                GetData(station, startDate.Day, GetLastDayOfMonth(startDate.Month, startDate.Year), startDate.Month, startDate.Year);

                for (int month = startDate.Month + 1; month <= 12; month++)
                {
                    GetData(station, 1, GetLastDayOfMonth(month, startDate.Year), month, startDate.Year);
                }

                for (int year = startDate.Year + 1; year <= currentDate.Year - 1; year++)
                {
                    for (int month = 1; month <= 12; month++)
                    {
                        GetData(station, 1, GetLastDayOfMonth(month, year), month, year);
                    }
                }
                for (int month = 1; month <= currentDate.Month - 1; month++)
                {
                    GetData(station, 1, GetLastDayOfMonth(month, currentDate.Year), month, currentDate.Year);
                }
                GetData(station, 1, currentDate.Day, currentDate.Month, currentDate.Year);
            }
        }

        void GetData(int station, int firstDay, int lastDay, int month, int year)
        {
            string url = $@"http://www.pogodaiklimat.ru/weather.php?id={station}&bday={firstDay}&fday={lastDay}&amonth={month}&ayear={year}&bot=2";
            var doc = GetDocument(url);
            var dateList = GetDatesList(doc);
            var valueList = GetValuesList(doc);
            if (dateList.Count != valueList.Count)
                throw new Exception("Полученные таблицы были не равной длины");

            for (int i = 0; i < dateList.Count; i++)
            {
                if (EmptyValueLine(valueList[i]))
                    continue;

                WeatherData weatherData = Converter.ConvertToWeatherData(dateList[i], valueList[i], year, station);
                if (_connector.DataLineExists(weatherData))
                    continue;
                _connector.InsertData(weatherData);
            }
        }

        static bool EmptyValueLine(ParsedValue value)
        {
            if (string.IsNullOrEmpty(value.windDirection) || string.IsNullOrEmpty(value.windSpeed)
                || string.IsNullOrEmpty(value.pressure) || string.IsNullOrEmpty(value.humidity)
                || string.IsNullOrEmpty(value.temperature))
                return true;
            return false;
        }

        static List<ParsedDate> GetDatesList(HtmlDocument htmlDoc)
        {
            var datesTable = htmlDoc.DocumentNode.SelectSingleNode(@"/html/body/div[1]/main/div/div/div/div/div/div[5]/div[2]/div/div[2]/div[1]/table");
            var rows = datesTable.SelectNodes("tr");
            List<ParsedDate> list = new();
            for (int i = 1; i < rows.Count; i++)
            {
                ParsedDate date = new()
                {
                    time = rows[i].SelectSingleNode("td[1]").InnerText,
                    date = rows[i].SelectSingleNode("td[2]").InnerText
                };
                list.Add(date);
            }
            return list;
        }

        static List<ParsedValue> GetValuesList(HtmlDocument htmlDoc)
        {
            var valuesTable =
                htmlDoc.DocumentNode.SelectSingleNode(
                    @"/html/body/div[1]/main/div/div/div/div/div/div[5]/div[2]/div/div[2]/div[2]/table");
            var rows = valuesTable.SelectNodes("tr");
            List<ParsedValue> list = new();
            for (int i = 1; i < rows.Count; i++)
            {
                ParsedValue value = new()
                {
                    windDirection = rows[i].SelectSingleNode("td[1]").InnerText,
                    windSpeed = rows[i].SelectSingleNode("td[2]").InnerText,
                    temperature = rows[i].SelectSingleNode("td[6]").InnerText,
                    humidity = rows[i].SelectSingleNode("td[8]").InnerText,
                    pressure = rows[i].SelectSingleNode("td[12]").InnerText,
                    snowHeight = rows[i].SelectSingleNode("td[18]").InnerText
                };
                list.Add(value);
            }
            return list;
        }

        static bool InternetConnectionAvailable()
        {
            try
            {
                Ping myPing = new();
                String host = "pogodaiklimat.ru";
                byte[] buffer = new byte[32];
                int timeout = 1000;
                PingOptions pingOptions = new();
                PingReply reply = myPing.Send(host, timeout, buffer, pingOptions);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        static HtmlDocument GetDocument(string url)
        {
            HtmlWeb web = new()
            {
                OverrideEncoding = Encoding.UTF8
            };
            HtmlDocument doc = web.Load(url);
            return doc;
        }

        static int GetLastDayOfMonth(int month, int year)
        {
            switch (month)
            {
                case 1:
                case 3:
                case 5:
                case 7:
                case 8:
                case 10:
                case 12:
                    {
                        return 31;
                    }
                case 4:
                case 6:
                case 9:
                case 11:
                    {
                        return 30;
                    }
                case 2:
                    {
                        if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
                            return 29;
                        else
                            return 28;
                    }
                default: return 0;
            }
        }
    }
}
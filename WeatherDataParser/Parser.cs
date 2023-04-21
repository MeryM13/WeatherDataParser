using System.Text;
using WeatherDataParser.CLASSES;
using HtmlAgilityPack;

namespace WeatherDataParser
{
    public class Parser
    {
        DateTime _startingDate;
        DatabaseConnector _connector;
        public Parser()
        {
            _startingDate = DefaultConfig.StartingDate;
            _connector = new DatabaseConnector();
        }

        public Parser(DateTime startingDate)
        {
            _startingDate = startingDate;
            _connector = new DatabaseConnector();
        }

        public Parser(string connectionString)
        {
            _startingDate = DefaultConfig.StartingDate;
            _connector = new DatabaseConnector(connectionString);
        }

        public Parser(DateTime startingDate, string connectionString)
        {
            _startingDate = startingDate;
            _connector = new DatabaseConnector(connectionString);
        }

        public void UpdateStationData(int stationID)
        {
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
                    AddStation(stationID);
                }
            }
        }

        public void AddStation(int stationID)
        {
            if (_connector.StationExists(stationID))
            {
                throw new Exception("Station already exists");
            }

            string url = $@"http://www.pogodaiklimat.ru/weather.php?id={stationID}";
            var doc = GetDocument(url);

            var headline = doc.DocumentNode.SelectSingleNode("/html/body/div[1]/main/div/div/div/div/div/div[5]/div[2]/div/div[1]/h1");
            if (headline.InnerText.Contains("(, )"))
                throw new Exception("Station not found");

            var archiveText = doc.DocumentNode.SelectSingleNode("/html/body/div[1]/main/div/div/div/div/div/div[5]/div[2]/div/div[5]");
            Station newStation = new()
            {
                ID = stationID,
                Name = archiveText.SelectSingleNode("span[1]").InnerText.Split('(')[0].Trim(),
                Location = archiveText.SelectSingleNode("span[1]").InnerText.Split('(', ')')[1].Trim(),
                Latitude = decimal.Parse(archiveText.SelectSingleNode("span[2]").InnerText.Replace(".", ",")),
                Longitude = decimal.Parse(archiveText.SelectSingleNode("span[3]").InnerText.Replace(".", ",")),
                Height = decimal.Parse(archiveText.SelectSingleNode("span[4]").InnerText.Split(' ')[0].Replace(".", ","))
            };
            Console.WriteLine($"{newStation.ID}, {newStation.Name}, {newStation.Location}, {newStation.Latitude}, {newStation.Longitude}, {newStation.Height}");
            _connector.AddStation(newStation);
            ParseForStation(stationID, _startingDate);
        }

        public void FullUpdate()
        {
            List<int> StationIDList = _connector.GetStationIDList();
            foreach (var station in StationIDList)
            {
                UpdateStationData(station);
            }
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
                throw new Exception("Unequal lists");

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

        bool EmptyValueLine(ParsedValue value)
        {
            if (string.IsNullOrEmpty(value.windDirection) || string.IsNullOrEmpty(value.windSpeed)
                || string.IsNullOrEmpty(value.pressure) || string.IsNullOrEmpty(value.humidity)
                || string.IsNullOrEmpty(value.temperature))
                return true;
            return false;
        }

        List<ParsedDate> GetDatesList(HtmlDocument htmlDoc)
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

        List<ParsedValue> GetValuesList(HtmlDocument htmlDoc)
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

        HtmlDocument GetDocument(string url)
        {
            HtmlWeb web = new()
            {
                OverrideEncoding = Encoding.UTF8
            };
            HtmlDocument doc = web.Load(url);
            return doc;
        }

        int GetLastDayOfMonth(int month, int year)
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
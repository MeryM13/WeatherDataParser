using WeatherDataParser.CLASSES;

namespace WeatherDataParser
{
    internal class Converter
    {
        public static WeatherData ConvertToWeatherData(ParsedDate _date, ParsedValue _value, int year, int station)
        {
            WeatherData weatherData = new()
            {
                StationID = station,
                Date = UniteDateAndTime(_date, year),
                WindDirection = ConvertWindDirectionToAngle(_value.windDirection),
                WindSpeed = ConvertWindSpeed(_value.windSpeed),
                Temperature = Convert.ToDecimal(_value.temperature.Replace('.', ',')),
                Humidity = int.Parse(_value.humidity),
                Pressure = Convert.ToDecimal(_value.pressure.Replace('.', ',')),
                SnowHeight = ConvertSnowHeight(_value.snowHeight)
            };
            return weatherData;
        }

        static DateTime UniteDateAndTime(ParsedDate _date, int year)
        {
            int hours = int.Parse(_date.time);
            int day = int.Parse(_date.date.Split('.')[0]);
            int month = int.Parse(_date.date.Split('.')[1]);
            return new DateTime(year, month, day, hours, 0, 0);
        }

        static int? ConvertWindDirectionToAngle(string windDirection)
        {
            switch (windDirection)
            {
                case "С":
                    {
                        return 0;
                    }
                case "СВ":
                    {
                        return 45;
                    }
                case "В":
                    {
                        return 90;
                    }
                case "ЮВ":
                    {
                        return 135;
                    }
                case "Ю":
                    {
                        return 180;
                    }
                case "ЮЗ":
                    {
                        return 225;
                    }
                case "З":
                    {
                        return 270;
                    }
                case "СЗ":
                    {
                        return 315;
                    }
                default:
                    {
                        return null;
                    }
            }
        }

        static int ConvertWindSpeed(string scrappedSpeed)
        {
            string speed = scrappedSpeed;
            if (scrappedSpeed.Contains('{'))
            {
                speed = speed.Split('{')[0];
            }
            if (scrappedSpeed.Contains('-'))
            {
                return (int.Parse(speed.Split('-')[0]) + int.Parse(speed.Split('-')[1])) / 2;
            }
            if (string.IsNullOrEmpty(speed))
            {
                return 0;
            }
            return int.Parse(speed);
        }

        static int? ConvertSnowHeight(string scrappedHeight)
        {
            if (int.TryParse(scrappedHeight, out int height))
                return height;
            else
                return null;
        }
    }
}

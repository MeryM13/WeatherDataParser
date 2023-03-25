using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WeatherDataParser.CLASSES
{
    internal class WeatherData
    {
        public int StationID { get; set; }
        public DateTime Date { get; set; }
        public decimal? WindDirection { get; set; }
        public int WindSpeed { get; set; }
        public decimal Temperature { get; set; }
        public int Humidity { get; set; }
        public decimal Pressure { get; set; }
        public int? SnowHeight { get; set; }
    }
}

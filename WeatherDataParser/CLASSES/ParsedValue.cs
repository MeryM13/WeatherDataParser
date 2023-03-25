using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WeatherDataParser.CLASSES
{
    internal class ParsedValue
    {
        public string? windDirection { get; set; }
        public string? windSpeed { get; set; }
        public string? temperature { get; set; }
        public string? humidity { get; set; }
        public string? pressure { get; set; }
        public string? snowHeight { get; set; }
    }
}

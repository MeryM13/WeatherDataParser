using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WeatherDataParser
{
    internal class DefaultConfig
    {
        public static readonly string DatabaseConnectionString = @"Data Source=localhost;Initial Catalog=WeatherDatabase;Integrated Security=True";
        public static readonly DateTime StartingDate = new(2012, 01, 01);
    }
}

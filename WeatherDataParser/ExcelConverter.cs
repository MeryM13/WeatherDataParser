using Microsoft.VisualBasic;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WeatherDataParser.CLASSES;

namespace WeatherDataParser
{
    public class ExcelConverter
    {
        readonly string FilePath;
        public ExcelConverter()
        {
            FilePath = DefaultConfig.ExcelFilesPath;
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        }

        public ExcelConverter(string path) 
        {
            FilePath = path;
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        }

        #region RawDataConversion
        public void Convert(string listName, DataTable data, DateInterval divider)
        {
            using (var package = new ExcelPackage())
            {
                switch (divider)
                {
                    case DateInterval.Day:
                        {
                            Dictionary<string, DataTable> dividedWeatherData = data.AsEnumerable().
                                GroupBy(row => new string($"{DateTime.Parse(row.Field<string>("Дата измерения")).Year}." +
                                                            $"{DateTime.Parse(row.Field<string>("Дата измерения")).Month}." +
                                                            $"{DateTime.Parse(row.Field<string>("Дата измерения")).Day}")).
                                ToDictionary(group => group.Key, group => group.CopyToDataTable());
                            foreach (var pair in dividedWeatherData)
                            {
                                Convert(pair.Key, pair.Value, package);
                            }
                            break;
                        }
                    case DateInterval.Month:
                        {
                            Dictionary<string, DataTable> dividedWeatherData = data.AsEnumerable().
                                GroupBy(row => new string($"{DateTime.Parse(row.Field<string>("Дата измерения")).Year}." +
                                                          $"{DateTime.Parse(row.Field<string>("Дата измерения")).Month}")).
                                ToDictionary(group => group.Key, group => group.CopyToDataTable());
                            foreach (var pair in dividedWeatherData)
                            {
                                Convert(pair.Key, pair.Value, package);
                            }
                            break;
                        }
                    case DateInterval.Year:
                        {
                            Dictionary<string, DataTable> dividedWeatherData = data.AsEnumerable().
                                GroupBy(row => new string($"{DateTime.Parse(row.Field<string>("Дата измерения")).Year}")).
                                ToDictionary(group => group.Key, group => group.CopyToDataTable());
                            foreach (var pair in dividedWeatherData)
                            {
                                Convert(pair.Key, pair.Value, package);
                            }
                            break;
                        }
                    default:
                        {
                            Convert(listName, data, package);
                            break;
                        }
                }
                package.SaveAs(new FileInfo(FilePath + $"{listName}.xlsx"));
            }
        }

        public void Convert(string listName, DataTable data)
        {
            using (var package = new ExcelPackage())
            {
                Convert(listName, data, package);
                package.SaveAs(new FileInfo(FilePath + $"{listName}.xlsx"));
            }
        }

        public void Convert(string listName, DataTable data, ExcelPackage package)
        {
            var sheet = package.Workbook.Worksheets.Add(listName);
            sheet.Cells[1, 1].LoadFromDataTable(data, c => c.PrintHeaders = true);
            sheet.Cells[sheet.Dimension.Address].AutoFitColumns();
        }
        #endregion

        #region WindRoseConversion
        public void Convert(string listName, List<Dictionary<decimal, decimal>> data, int[] speeds)
        {
            var insert = new List<Dictionary<string, object>>();
            foreach (var entry in data)
            {
                insert.Add(entry.ToDictionary(x => x.Key.ToString(), x => (object)x.Value));
            }

            using (var package = new ExcelPackage())
            {
                var sheet = package.Workbook.Worksheets.Add(listName);
                sheet.Cells[1, 1].Value = "Направление";
                sheet.Cells[1, 2].LoadFromArrays(new List<object[]>() { speeds.Cast<object>().ToArray() });
                sheet.Cells[2, 1].LoadFromDictionaries(insert, c => { c.PrintHeaders = true; c.Transpose = true; });
                sheet.Cells[sheet.Dimension.Address].AutoFitColumns();
                package.SaveAs(new FileInfo(FilePath + $"{listName} .xlsx"));
            }
        }
        public void Convert(string listName, Dictionary<decimal,decimal> data)
        {
            using (var package = new ExcelPackage())
            {
                var sheet = package.Workbook.Worksheets.Add(listName);
                sheet.Cells[1, 1].Value = "Направление";
                sheet.Cells[1, 2].Value = "Значение";
                sheet.Cells[2, 1].LoadFromDictionaries(new List<Dictionary<string, object>>() { data.ToDictionary(x => x.Key.ToString(), x => (object)x.Value) }, c => { c.PrintHeaders = true; c.Transpose = true; });
                sheet.Cells[sheet.Dimension.Address].AutoFitColumns();
                package.SaveAs(new FileInfo(FilePath + $"{listName}.xlsx"));
            }
        }
        #endregion

        #region ChartsConversion
        public void Convert(string listName, Dictionary<DateTime, decimal> data)
        {
            using (var package = new ExcelPackage())
            {
                var sheet = package.Workbook.Worksheets.Add(listName);
                sheet.Cells[1, 1].Value = "Дата";
                sheet.Cells[1, 2].Value = "Значение";
                sheet.Cells[2, 1].LoadFromDictionaries(new List<Dictionary<string, object>>() { data.ToDictionary(x => x.Key.ToShortDateString(), x => (object)x.Value) }, c => { c.PrintHeaders = true; c.Transpose = true; });
                sheet.Cells[sheet.Dimension.Address].AutoFitColumns();
                package.SaveAs(new FileInfo(FilePath + $"{listName}.xlsx"));
            }
        }
        #endregion
    }
}

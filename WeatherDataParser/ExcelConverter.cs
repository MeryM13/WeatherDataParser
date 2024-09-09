using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Excel = Microsoft.Office.Interop.Excel;

namespace WeatherDataParser
{
    internal class ExcelConverter
    {
        string SheetPath;
        public ExcelConverter(string path) 
        {
            SheetPath = path;
        }

        void OpenSheet()
        {
            //opens sheet
            //if no sheet detected
            CreateSheet();
        }

        void CreateSheet()
        {
            //create shhet with given path
            //then
            OpenSheet();
        }

        void CheckListExists(string name)
        {
            //checks if the list with given name
            //already exists in a sheet
        }

        void CreateList(string name)
        {
            //if not
            CheckListExists(name);
            //creates a list in a sheet with
            //generated name
        }

        public void Convert(string listName, DataSet data)
        {
            //puts dataset of meteodata into
            //a newly generated list
        }

        public void Convert(string listName, Dictionary<decimal,decimal> data)
        { }

        public void Convert(string listName, Dictionary<DateTime, decimal> data)
        { }
    }
}

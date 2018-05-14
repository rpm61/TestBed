using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestBed
{
    class Utilities
    {

        public static DataTable loadExcelSheetIntoDatatable(string filepath, string sheetname = "Sheet1")
        {

            string fileToConvert = filepath;
            dynamic connectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + fileToConvert + ";Extended Properties=Excel 12.0;";
            OleDbConnection dbConn = new OleDbConnection(connectionString);
            dbConn.Open();
            dynamic adapter = new OleDbDataAdapter("SELECT * from [" + sheetname + "$]", connectionString);
            dynamic dt = new DataTable();
            adapter.Fill(dt);
            dbConn.Close();

            return dt;
        }


    }
}

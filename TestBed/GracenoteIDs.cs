using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using API.ServiceModel.DTO;
using MoreLinq;
using ServiceStack;
using ServiceStack.OrmLite;

namespace TestBed
{
    public class GracenoteIDs
    {
        public void start()
        {

            var dbFactory = new OrmLiteConnectionFactory(System.Configuration.ConfigurationManager.AppSettings["connectionstring.entertainment"], SqlServer2012Dialect.Provider);
            var gnProgramMatches = new List<GracenoteIdMatchResponse>();

            // MOVIES
            var movies = new List<long>();

            using (IDbConnection db = dbFactory.Open())
            {
                movies = db.Select<long>("select id from movie where exists(select mv.id from movievideo mv where mv.movieid = movie.id) and not exists(select ma.id from moviealternateid ma where ma.movieid = movie.id and ma.alternateidtypeid = 5)");
            }
            int count = movies.Count;           
            

            foreach (var m in movies)
            {
                Console.Clear();
                Console.Write("Running movies count: " + count);
                using (var IvaClient = Program.GetClient(new TimeSpan(0, 10, 0)))
                {
                    try
                    {
                        var gnMovie = IvaClient.Get(new GetGracenoteIdForMovie { MovieId = m });
                        //if (gnMovie != null && gnMovie.GracenoteId != "" && gnMovie.Score > 2)
                        gnProgramMatches.Add(gnMovie);
                    }
                    catch (Exception e) { }
                }
                count--;
            }


            // SHOWS
            var shows = new List<long>();
            using (IDbConnection db = dbFactory.Open())
            {
                shows = db.Select<long>("select id from show where not exists(select sa.id from showalternateid sa where sa.showid = show.id and sa.alternateidtypeid = 8)");
            }
            count = shows.Count;

            foreach (var s in shows)
            {
                Console.Clear();
                Console.Write("Running shows count: " + count);
                using (var IvaClient = Program.GetClient(new TimeSpan(0, 10, 0)))
                {
                    try
                    {
                        var gnShow = IvaClient.Get(new GetGracenoteIdForShow { ShowId = s });
                        //if (gnShow != null && gnShow.GracenoteId != "" && gnShow.Score > 2)
                        if (gnShow != null)
                            gnProgramMatches.Add(gnShow);
                    }
                    catch (Exception e) { }
                    count--;
                }
            }


            //Start OUTPUT********************************************************
            DataTable gnMatches = new DataTable();
            gnMatches = gnProgramMatches.ToDataTable();

            string localDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            dynamic package = new OfficeOpenXml.ExcelPackage();
            dynamic worksheet1 = package.Workbook.Worksheets.Add("GRacenoteID_Matches");

            worksheet1.Cells["A1"].LoadFromDataTable(gnMatches, true);
            //worksheet1.Cells.AutoFitColumns()
            worksheet1.Cells.Style.WrapText = true;


            FileInfo f = new FileInfo(localDirectory + "\\Match Results.xlsx");
            if (f.Exists)
                f.Delete();
            package.SaveAs(f);


            Console.WriteLine("");
        }
    }
}

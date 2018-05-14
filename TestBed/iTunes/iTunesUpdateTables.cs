using API.ServiceModel.DTO;
using Availability_Collectors_WebJob.collectors;
using Availability_Collectors_WebJob.DataSources;
using Availability_Collectors_WebJob.Models.iTunes;
using MoreLinq;
using ServiceStack.OrmLite;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestBed.iTunes
{
    public class iTunesUpdateTables
    {

        #region Artist
        public static void UpdateiTunesArtist()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.artist;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            string fileDate = today.Year.ToString("0000") + today.Month.ToString("00") + today.AddDays(-1).Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\current\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\current\pricing" + fileDate + @"\";

            // Get IDs to check IF update
            List<int> artistIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                artistIds = Db.Select<int>("Select artistid from itunesartist");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesArtist> itList = new ConcurrentBag<iTunesArtist>();
                    ConcurrentBag<iTunesArtist> updateList = new ConcurrentBag<iTunesArtist>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 6)
                            return;
                        iTunesArtist item = new iTunesArtist();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.ArtistId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];
                        bool b;
                        item.IsActualArtist = Boolean.TryParse(currentRow[3], out b) ? b : false;
                        item.ViewUrl = currentRow[4];
                        item.ArtistTypeId = Convert.ToInt32(currentRow[5]);

                        if (artistIds.Contains(item.ArtistId))
                        {
                            updateList.Add(item);
                            using (var Db = dbFactory.Open())
                            {
                                Db.Update(item);
                            }
                        }
                        else
                            itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            // Using StoredProcedure
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesArtist> itList = new ConcurrentBag<iTunesArtist>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 6)
                        return;
                    iTunesArtist item = new iTunesArtist();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.ArtistId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];
                    bool b;
                    item.IsActualArtist = Boolean.TryParse(currentRow[3], out b) ? b : false;
                    item.ViewUrl = currentRow[4];
                    item.ArtistTypeId = Convert.ToInt32(currentRow[5]);

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesArtistCollection()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.artist_collection;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> collectionIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesArtistCollection");
                collectionIds = Db.Select<int>("Select collectionid from itunescollection");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesArtistCollection> itList = new ConcurrentBag<iTunesArtistCollection>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 5)
                            return;
                        iTunesArtistCollection item = new iTunesArtistCollection();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.ArtistId = Convert.ToInt32(currentRow[1]);
                        item.CollectionId = Convert.ToInt32(currentRow[2]);
                        if (!(collectionIds.Contains(item.CollectionId)))
                            return;
                        bool b;
                        item.IsPrimaryArtist = Boolean.TryParse(currentRow[3], out b) ? b : false;
                        item.RoleId = Convert.ToInt32(currentRow[4]);


                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesArtistCollection> itList = new ConcurrentBag<iTunesArtistCollection>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 5)
                        return;
                    iTunesArtistCollection item = new iTunesArtistCollection();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.ArtistId = Convert.ToInt32(currentRow[1]);
                    item.CollectionId = Convert.ToInt32(currentRow[2]);
                    if (!(collectionIds.Contains(item.CollectionId)))
                        return;
                    bool b;
                    item.IsPrimaryArtist = Boolean.TryParse(currentRow[3], out b) ? b : false;
                    item.RoleId = Convert.ToInt32(currentRow[4]);

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }



        public static void UpdateiTunesArtistTranslation()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.artist_translation;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild            
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesArtistTranslation");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesArtistTranslation> itList = new ConcurrentBag<iTunesArtistTranslation>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 6)
                            return;
                        iTunesArtistTranslation item = new iTunesArtistTranslation();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.ArtistId = Convert.ToInt32(currentRow[1]);
                        item.LanguageCode = currentRow[2];
                        bool b;
                        item.IsPronuciation = Boolean.TryParse(currentRow[3], out b) ? b : false;
                        item.Translation = currentRow[4];
                        item.TranslationTypeId = Convert.ToInt32(currentRow[5]);

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesArtistTranslation> itList = new ConcurrentBag<iTunesArtistTranslation>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 6)
                        return;
                    iTunesArtistTranslation item = new iTunesArtistTranslation();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.ArtistId = Convert.ToInt32(currentRow[1]);
                    item.LanguageCode = currentRow[2];
                    bool b;
                    item.IsPronuciation = Boolean.TryParse(currentRow[3], out b) ? b : false;
                    item.Translation = currentRow[4];
                    item.TranslationTypeId = Convert.ToInt32(currentRow[5]);

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesArtistType()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.artist_type;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild            
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesArtistType");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesArtistType> itList = new ConcurrentBag<iTunesArtistType>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 4)
                            return;
                        iTunesArtistType item = new iTunesArtistType();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.ArtistTypeId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];
                        item.PrimaryMediaTypeId = Convert.ToInt32(currentRow[3]);

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesArtistType> itList = new ConcurrentBag<iTunesArtistType>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 4)
                        return;
                    iTunesArtistType item = new iTunesArtistType();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.ArtistTypeId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];
                    item.PrimaryMediaTypeId = Convert.ToInt32(currentRow[3]);

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                       

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesArtistVideo()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.artist_video;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> videoIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesArtistVideo");
                videoIds = Db.Select<int>("Select videoid from itunesvideo");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesArtistVideo> itList = new ConcurrentBag<iTunesArtistVideo>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 5)
                            return;
                        iTunesArtistVideo item = new iTunesArtistVideo();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.ArtistId = Convert.ToInt32(currentRow[1]);
                        item.VideoId = Convert.ToInt32(currentRow[2]);
                        if (!(videoIds.Contains(item.VideoId)))
                            return;
                        bool b;
                        item.IsPrimaryArtist = Boolean.TryParse(currentRow[3], out b) ? b : false;
                        item.RoleId = Convert.ToInt32(currentRow[4]);

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesArtistVideo> itList = new ConcurrentBag<iTunesArtistVideo>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 5)
                        return;
                    iTunesArtistVideo item = new iTunesArtistVideo();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.ArtistId = Convert.ToInt32(currentRow[1]);
                    item.VideoId = Convert.ToInt32(currentRow[2]);
                    if (!(videoIds.Contains(item.VideoId)))
                        return;
                    bool b;
                    item.IsPrimaryArtist = Boolean.TryParse(currentRow[3], out b) ? b : false;
                    item.RoleId = Convert.ToInt32(currentRow[4]);

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesRole()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.role;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> videoIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesRole");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesRole> itList = new ConcurrentBag<iTunesRole>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 3)
                            return;
                        iTunesRole item = new iTunesRole();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.RoleId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesRole> itList = new ConcurrentBag<iTunesRole>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 3)
                        return;
                    iTunesRole item = new iTunesRole();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.RoleId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }

        #endregion Artist


        #region Collection
        public static void UpdateiTunesCollection()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.collection;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild                 
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesCollection");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesCollection> itList = new ConcurrentBag<iTunesCollection>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 18)
                            return;

                        int i;
                        iTunesCollection item = new iTunesCollection();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.CollectionId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];
                        item.TitleVersion = currentRow[3];
                        item.SearchTerms = currentRow[4];
                        item.ParentalAdvisoryId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;
                        item.ArtistDisplayName = currentRow[6];
                        item.ViewUrl = currentRow[7];
                        item.ArtworkUrl = currentRow[8];
                        DateTime d;
                        item.OriginalReleaseDate = DateTime.TryParse(currentRow[9], out d) ? (DateTime?)d : (DateTime?)null;
                        if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                            item.OriginalReleaseDate = null;
                        item.iTunesReleaseDate = DateTime.TryParse(currentRow[10], out d) ? (DateTime?)d : (DateTime?)null;
                        if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                            item.OriginalReleaseDate = null;
                        item.LabelStudio = currentRow[11];
                        item.ContentProviderName = currentRow[12];
                        item.Copyright = currentRow[13];
                        item.Pline = currentRow[14];
                        item.MediaTypeId = Int32.TryParse(currentRow[15], out i) ? (int?)i : (int?)null;
                        bool b;
                        item.IsCompilation = Boolean.TryParse(currentRow[16], out b) ? b : false;
                        item.CollectionTypeId = Int32.TryParse(currentRow[17], out i) ? (int?)i : (int?)null;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {
                        var filteredList = itList.Where(x => x.MediaTypeId == 4 || x.MediaTypeId == 6);

                        if (filteredList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, filteredList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesCollection> itList = new ConcurrentBag<iTunesCollection>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 18)
                        return;

                    int i;
                    iTunesCollection item = new iTunesCollection();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.CollectionId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];
                    item.TitleVersion = currentRow[3];
                    item.SearchTerms = currentRow[4];
                    item.ParentalAdvisoryId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;
                    item.ArtistDisplayName = currentRow[6];
                    item.ViewUrl = currentRow[7];
                    item.ArtworkUrl = currentRow[8];
                    DateTime d;
                    item.OriginalReleaseDate = DateTime.TryParse(currentRow[9], out d) ? (DateTime?)d : (DateTime?)null;
                    if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                        item.OriginalReleaseDate = null;
                    item.iTunesReleaseDate = DateTime.TryParse(currentRow[10], out d) ? (DateTime?)d : (DateTime?)null;
                    if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                        item.OriginalReleaseDate = null;
                    item.LabelStudio = currentRow[11];
                    item.ContentProviderName = currentRow[12];
                    item.Copyright = currentRow[13];
                    item.Pline = currentRow[14];
                    item.MediaTypeId = Int32.TryParse(currentRow[15], out i) ? (int?)i : (int?)null;
                    bool b;
                    item.IsCompilation = Boolean.TryParse(currentRow[16], out b) ? b : false;
                    item.CollectionTypeId = Int32.TryParse(currentRow[17], out i) ? (int?)i : (int?)null;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {
                    var filteredList = itList.Where(x => x.MediaTypeId == 4 || x.MediaTypeId == 6);

                    if (filteredList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, filteredList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        public static void UpdateiTunesCollectionPrice()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.collection_price;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            //string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> collectionIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesCollectionPrice");
                collectionIds = Db.Select<int>("Select collectionid from itunescollection");
            }


            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(pricingFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesCollectionPrice> itList = new ConcurrentBag<iTunesCollectionPrice>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 8)
                            return;
                        Decimal dl;
                        iTunesCollectionPrice item = new iTunesCollectionPrice();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.CollectionId = Convert.ToInt32(currentRow[1]);
                        if (!(collectionIds.Contains(item.CollectionId)))
                            return;
                        item.RetailPrice = Decimal.TryParse(currentRow[2], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.CurrencyCode = currentRow[3];
                        item.StorefrontId = Convert.ToInt32(currentRow[4]);
                        DateTime d;
                        item.AvailabilityDate = DateTime.TryParse(currentRow[5], out d) ? (DateTime?)d : (DateTime?)null;
                        if (!(item.AvailabilityDate is null) && item.AvailabilityDate.Value.Year < 1755)
                            item.AvailabilityDate = null;
                        item.HQPrice = Decimal.TryParse(currentRow[6], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.PreorderPrice = Decimal.TryParse(currentRow[7], out dl) ? (Decimal?)dl : (Decimal?)null;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesCollectionPrice> itList = new ConcurrentBag<iTunesCollectionPrice>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 8)
                        return;

                    Decimal dl;
                    iTunesCollectionPrice item = new iTunesCollectionPrice();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.CollectionId = Convert.ToInt32(currentRow[1]);
                    if (!(collectionIds.Contains(item.CollectionId)))
                        return;
                    item.RetailPrice = Decimal.TryParse(currentRow[2], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.CurrencyCode = currentRow[3];
                    item.StorefrontId = Convert.ToInt32(currentRow[4]);
                    DateTime d;
                    item.AvailabilityDate = DateTime.TryParse(currentRow[5], out d) ? (DateTime?)d : (DateTime?)null;
                    if (!(item.AvailabilityDate is null) && item.AvailabilityDate.Value.Year < 1755)
                        item.AvailabilityDate = null;
                    item.HQPrice = Decimal.TryParse(currentRow[6], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.PreorderPrice = Decimal.TryParse(currentRow[7], out dl) ? (Decimal?)dl : (Decimal?)null;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesCollectionTranslation()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.collection_translation;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> collectionIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesCollectionTranslation");
                collectionIds = Db.Select<int>("Select collectionid from itunescollection");
            }


            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesCollectionTranslation> itList = new ConcurrentBag<iTunesCollectionTranslation>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 6)
                            return;
                        iTunesCollectionTranslation item = new iTunesCollectionTranslation();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.CollectionId = Convert.ToInt32(currentRow[1]);
                        if (!(collectionIds.Contains(item.CollectionId)))
                            return;
                        item.LanguageCode = currentRow[2];
                        bool b;
                        item.IsPronunciation = Boolean.TryParse(currentRow[3], out b) ? b : false;
                        item.Translation = currentRow[4];
                        int i;
                        item.TranslationTypeId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesCollectionTranslation> itList = new ConcurrentBag<iTunesCollectionTranslation>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 6)
                        return;
                    iTunesCollectionTranslation item = new iTunesCollectionTranslation();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.CollectionId = Convert.ToInt32(currentRow[1]);
                    if (!(collectionIds.Contains(item.CollectionId)))
                        return;
                    item.LanguageCode = currentRow[2];
                    bool b;
                    item.IsPronunciation = Boolean.TryParse(currentRow[3], out b) ? b : false;
                    item.Translation = currentRow[4];
                    int i;
                    item.TranslationTypeId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesCollectionType()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.collection_type;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild            
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesCollectionType");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesCollectionType> itList = new ConcurrentBag<iTunesCollectionType>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 3)
                            return;
                        iTunesCollectionType item = new iTunesCollectionType();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.CollectionTypeId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesCollectionType> itList = new ConcurrentBag<iTunesCollectionType>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 3)
                        return;
                    iTunesCollectionType item = new iTunesCollectionType();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.CollectionTypeId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }

        public static void UpdateiTunesCollectionVideo()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.collection_video;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> collectionIds = new List<int>();
            List<int> videoIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesCollectionVideo");
                collectionIds = Db.Select<int>("Select collectionid from itunescollection");
                videoIds = Db.Select<int>("Select videoid from itunesvideo");
            }


            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesCollectionVideo> itList = new ConcurrentBag<iTunesCollectionVideo>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 6)
                            return;
                        iTunesCollectionVideo item = new iTunesCollectionVideo();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.CollectionId = Convert.ToInt32(currentRow[1]);
                        if (!(collectionIds.Contains(item.CollectionId)))
                            return;
                        item.VideoId = Convert.ToInt32(currentRow[2]);
                        if (!(videoIds.Contains(item.CollectionId)))
                            return;
                        int i;
                        item.TrackNumber = Int32.TryParse(currentRow[3], out i) ? (int?)i : (int?)null;
                        item.VolumeNumber = Int32.TryParse(currentRow[4], out i) ? (int?)i : (int?)null;
                        bool b;
                        item.PreorderOnly = Boolean.TryParse(currentRow[5], out b) ? b : false;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesCollectionVideo> itList = new ConcurrentBag<iTunesCollectionVideo>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 6)
                        return;
                    iTunesCollectionVideo item = new iTunesCollectionVideo();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.CollectionId = Convert.ToInt32(currentRow[1]);
                    if (!(collectionIds.Contains(item.CollectionId)))
                        return;
                    item.VideoId = Convert.ToInt32(currentRow[2]);
                    if (!(videoIds.Contains(item.CollectionId)))
                        return;
                    int i;
                    item.TrackNumber = Int32.TryParse(currentRow[3], out i) ? (int?)i : (int?)null;
                    item.VolumeNumber = Int32.TryParse(currentRow[4], out i) ? (int?)i : (int?)null;
                    bool b;
                    item.PreorderOnly = Boolean.TryParse(currentRow[5], out b) ? b : false;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }



        #endregion Collection

        #region Genres
        public static void UpdateiTunesGenre()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.genre;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild            
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesGenre");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesGenre> itList = new ConcurrentBag<iTunesGenre>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 4)
                            return;
                        iTunesGenre item = new iTunesGenre();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.GenreId = Convert.ToInt32(currentRow[1]);
                        int i;
                        item.ParentId = Int32.TryParse(currentRow[2], out i) ? (int?)i : (int?)null;
                        item.Name = currentRow[3];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesGenre> itList = new ConcurrentBag<iTunesGenre>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 4)
                        return;
                    iTunesGenre item = new iTunesGenre();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.GenreId = Convert.ToInt32(currentRow[1]);
                    int i;
                    item.ParentId = Int32.TryParse(currentRow[2], out i) ? (int?)i : (int?)null;
                    item.Name = currentRow[3];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }

        public static void UpdateiTunesGenreArtist()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.genre_artist;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild            
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesGenreArtist");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesGenreArtist> itList = new ConcurrentBag<iTunesGenreArtist>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 4)
                            return;
                        iTunesGenreArtist item = new iTunesGenreArtist();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.GenreId = Convert.ToInt32(currentRow[1]);
                        item.ArtistId = Convert.ToInt32(currentRow[2]);
                        bool b;
                        item.IsPrimary = Boolean.TryParse(currentRow[3], out b) ? b : false;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesGenreArtist> itList = new ConcurrentBag<iTunesGenreArtist>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 4)
                        return;
                    iTunesGenreArtist item = new iTunesGenreArtist();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.GenreId = Convert.ToInt32(currentRow[1]);
                    item.ArtistId = Convert.ToInt32(currentRow[2]);
                    bool b;
                    item.IsPrimary = Boolean.TryParse(currentRow[3], out b) ? b : false;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesGenreCollection()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.genre_collection;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> collectionIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesGenreCollection");
                collectionIds = Db.Select<int>("Select collectionid from itunescollection");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesGenreCollection> itList = new ConcurrentBag<iTunesGenreCollection>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 4)
                            return;
                        iTunesGenreCollection item = new iTunesGenreCollection();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.GenreId = Convert.ToInt32(currentRow[1]);
                        item.CollectionId = Convert.ToInt32(currentRow[2]);
                        if (!(collectionIds.Contains(item.CollectionId)))
                            return;
                        bool b;
                        item.IsPrimary = Boolean.TryParse(currentRow[3], out b) ? b : false;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesGenreCollection> itList = new ConcurrentBag<iTunesGenreCollection>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 4)
                        return;
                    iTunesGenreCollection item = new iTunesGenreCollection();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.GenreId = Convert.ToInt32(currentRow[1]);
                    item.CollectionId = Convert.ToInt32(currentRow[2]);
                    if (!(collectionIds.Contains(item.CollectionId)))
                        return;
                    bool b;
                    item.IsPrimary = Boolean.TryParse(currentRow[3], out b) ? b : false;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesGenreVideo()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.genre_video;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> videoIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesGenreVideo");
                videoIds = Db.Select<int>("Select videoid from itunesvideo");
            }


            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesGenreVideo> itList = new ConcurrentBag<iTunesGenreVideo>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 4)
                            return;
                        iTunesGenreVideo item = new iTunesGenreVideo();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.GenreId = Convert.ToInt32(currentRow[1]);
                        item.VideoId = Convert.ToInt32(currentRow[2]);
                        if (!(videoIds.Contains(item.VideoId)))
                            return;
                        bool b;
                        item.IsPrimary = Boolean.TryParse(currentRow[3], out b) ? b : false;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesGenreVideo> itList = new ConcurrentBag<iTunesGenreVideo>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 4)
                        return;
                    iTunesGenreVideo item = new iTunesGenreVideo();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.GenreId = Convert.ToInt32(currentRow[1]);
                    item.VideoId = Convert.ToInt32(currentRow[2]);
                    if (!(videoIds.Contains(item.VideoId)))
                        return;
                    bool b;
                    item.IsPrimary = Boolean.TryParse(currentRow[3], out b) ? b : false;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }

                itRows.Clear();
            }
        }

        #endregion Genres


        #region Video
        public static void UpdateiTunesVideo()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.video;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild            
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesVideo");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesVideo> itList = new ConcurrentBag<iTunesVideo>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 22)
                            return;
                        iTunesVideo item = new iTunesVideo();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.VideoId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];
                        item.TitleVersion = currentRow[3];
                        item.SearchTerms = currentRow[4];
                        int i;
                        item.ParentalAdvisoryId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;
                        item.ArtistDisplayName = currentRow[6];
                        item.CollectionDisplayName = currentRow[7];
                        item.MediaTypeId = Int32.TryParse(currentRow[8], out i) ? (int?)i : (int?)null;
                        item.ViewUrl = currentRow[9];
                        item.ArtworkUrl = currentRow[10];
                        DateTime d;
                        item.OriginalReleaseDate = DateTime.TryParse(currentRow[11], out d) ? (DateTime?)d : (DateTime?)null;
                        if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                            item.OriginalReleaseDate = null;
                        item.iTunesReleaseDate = DateTime.TryParse(currentRow[12], out d) ? (DateTime?)d : (DateTime?)null;
                        if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                            item.OriginalReleaseDate = null;
                        item.StudioName = currentRow[13];
                        item.NetworkName = currentRow[14];
                        item.ContentProviderName = currentRow[15];
                        long l;
                        item.TrackLength = Int64.TryParse(currentRow[16], out l) ? (long?)l : (long?)null;
                        item.Copyright = currentRow[17];
                        item.Pline = currentRow[18];
                        item.ShortDescription = currentRow[19];
                        item.LongDescription = currentRow[20];
                        item.EpisodeProductionNumber = Int32.TryParse(currentRow[21], out i) ? (int?)i : (int?)null;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {
                        var filteredList = itList.DistinctBy(x => x.VideoId).Where(x => x.MediaTypeId == 4 || x.MediaTypeId == 6).ToList();

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }                            

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, filteredList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesVideo> itList = new ConcurrentBag<iTunesVideo>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 22)
                        return;
                    iTunesVideo item = new iTunesVideo();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.VideoId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];
                    item.TitleVersion = currentRow[3];
                    item.SearchTerms = currentRow[4];
                    int i;
                    item.ParentalAdvisoryId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;
                    item.ArtistDisplayName = currentRow[6];
                    item.CollectionDisplayName = currentRow[7];
                    item.MediaTypeId = Int32.TryParse(currentRow[8], out i) ? (int?)i : (int?)null;
                    item.ViewUrl = currentRow[9];
                    item.ArtworkUrl = currentRow[10];
                    DateTime d;
                    item.OriginalReleaseDate = DateTime.TryParse(currentRow[11], out d) ? (DateTime?)d : (DateTime?)null;
                    if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                        item.OriginalReleaseDate = null;
                    item.iTunesReleaseDate = DateTime.TryParse(currentRow[12], out d) ? (DateTime?)d : (DateTime?)null;
                    if (!(item.OriginalReleaseDate is null) && item.OriginalReleaseDate.Value.Year < 1755)
                        item.OriginalReleaseDate = null;
                    item.StudioName = currentRow[13];
                    item.NetworkName = currentRow[14];
                    item.ContentProviderName = currentRow[15];
                    long l;
                    item.TrackLength = Int64.TryParse(currentRow[16], out l) ? (long?)l : (long?)null;
                    item.Copyright = currentRow[17];
                    item.Pline = currentRow[18];
                    item.ShortDescription = currentRow[19];
                    item.LongDescription = currentRow[20];
                    item.EpisodeProductionNumber = Int32.TryParse(currentRow[21], out i) ? (int?)i : (int?)null;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {
                    var filteredList = itList.DistinctBy(x => x.VideoId).Where(x => x.MediaTypeId == 4 || x.MediaTypeId == 6).ToList();

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }                        

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, filteredList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesVideoPrice()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.video_price;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            //string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> videoIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesVideoPrice");
                videoIds = Db.Select<int>("Select videoid from itunesvideo");
            }


            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(pricingFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesVideoPrice> itList = new ConcurrentBag<iTunesVideoPrice>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 1 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 12)
                            return;
                        iTunesVideoPrice item = new iTunesVideoPrice();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.VideoId = Convert.ToInt32(currentRow[1]);
                        if (!(videoIds.Contains(item.VideoId)))
                            return;
                        Decimal dl;
                        item.RetailPrice = Decimal.TryParse(currentRow[2], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.CurrencyCode = currentRow[3];
                        item.StorefrontId = Convert.ToInt32(currentRow[4]);
                        DateTime d;
                        item.AvailabilityDate = DateTime.TryParse(currentRow[5], out d) ? (DateTime?)d : (DateTime?)null;
                        if (!(item.AvailabilityDate is null) && item.AvailabilityDate.Value.Year < 1755)
                            item.AvailabilityDate = null;
                        item.SDPrice = Decimal.TryParse(currentRow[6], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.HQPrice = Decimal.TryParse(currentRow[7], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.LCRentalPrice = Decimal.TryParse(currentRow[8], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.SDRentalPrice = Decimal.TryParse(currentRow[9], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.HDRentalPrice = Decimal.TryParse(currentRow[10], out dl) ? (Decimal?)dl : (Decimal?)null;
                        item.HDPrice = Decimal.TryParse(currentRow[11], out dl) ? (Decimal?)dl : (Decimal?)null;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {
                        var filteredList = itList.DistinctBy(x => new { x.VideoId, x.StorefrontId }).ToList();

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, filteredList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesVideoPrice> itList = new ConcurrentBag<iTunesVideoPrice>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 12)
                        return;
                    iTunesVideoPrice item = new iTunesVideoPrice();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.VideoId = Convert.ToInt32(currentRow[1]);
                    if (!(videoIds.Contains(item.VideoId)))
                        return;
                    Decimal dl;
                    item.RetailPrice = Decimal.TryParse(currentRow[2], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.CurrencyCode = currentRow[3];
                    item.StorefrontId = Convert.ToInt32(currentRow[4]);
                    DateTime d;
                    item.AvailabilityDate = DateTime.TryParse(currentRow[5], out d) ? (DateTime?)d : (DateTime?)null;
                    if (!(item.AvailabilityDate is null) && item.AvailabilityDate.Value.Year < 1755)
                        item.AvailabilityDate = null;
                    item.SDPrice = Decimal.TryParse(currentRow[6], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.HQPrice = Decimal.TryParse(currentRow[7], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.LCRentalPrice = Decimal.TryParse(currentRow[8], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.SDRentalPrice = Decimal.TryParse(currentRow[9], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.HDRentalPrice = Decimal.TryParse(currentRow[10], out dl) ? (Decimal?)dl : (Decimal?)null;
                    item.HDPrice = Decimal.TryParse(currentRow[11], out dl) ? (Decimal?)dl : (Decimal?)null;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {
                    var filteredList = itList.DistinctBy(x => new { x.VideoId, x.StorefrontId }).ToList();

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, filteredList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesVideoTranslation()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.video_translation;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            List<int> videoIds = new List<int>();
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesVideoTranslation");
                videoIds = Db.Select<int>("Select videoid from itunesvideo");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesVideoTranslation> itList = new ConcurrentBag<iTunesVideoTranslation>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 6)
                            return;
                        iTunesVideoTranslation item = new iTunesVideoTranslation();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.VideoId = Convert.ToInt32(currentRow[1]);
                        if (!(videoIds.Contains(item.VideoId)))
                            return;
                        item.LanguageCode = currentRow[2];
                        bool b;
                        item.IsPronunciation = Boolean.TryParse(currentRow[3], out b) ? b : false;
                        item.Translation = currentRow[4];
                        int i;
                        item.TranslationTypeId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesVideoTranslation> itList = new ConcurrentBag<iTunesVideoTranslation>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 6)
                        return;
                    iTunesVideoTranslation item = new iTunesVideoTranslation();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.VideoId = Convert.ToInt32(currentRow[1]);
                    if (!(videoIds.Contains(item.VideoId)))
                        return;
                    item.LanguageCode = currentRow[2];
                    bool b;
                    item.IsPronunciation = Boolean.TryParse(currentRow[3], out b) ? b : false;
                    item.Translation = currentRow[4];
                    int i;
                    item.TranslationTypeId = Int32.TryParse(currentRow[5], out i) ? (int?)i : (int?)null;

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }
        #endregion Video


        #region Common
        public static void UpdateiTunesMediaType()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.media_type;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesMediaType");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesMediaType> itList = new ConcurrentBag<iTunesMediaType>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 3)
                            return;
                        iTunesMediaType item = new iTunesMediaType();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.MediaTypeId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesMediaType> itList = new ConcurrentBag<iTunesMediaType>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 3)
                        return;
                    iTunesMediaType item = new iTunesMediaType();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.MediaTypeId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesParentalAdvisory()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.parental_advisory;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesParentalAdvisory");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesParentalAdvisory> itList = new ConcurrentBag<iTunesParentalAdvisory>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 3)
                            return;
                        iTunesParentalAdvisory item = new iTunesParentalAdvisory();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.ParentalAdvisoryId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesParentalAdvisory> itList = new ConcurrentBag<iTunesParentalAdvisory>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 3)
                        return;
                    iTunesParentalAdvisory item = new iTunesParentalAdvisory();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.ParentalAdvisoryId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesStorefront()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.storefront;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesStorefront");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesStorefront> itList = new ConcurrentBag<iTunesStorefront>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 4)
                            return;
                        iTunesStorefront item = new iTunesStorefront();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.StorefrontId = Convert.ToInt32(currentRow[1]);
                        item.CountryCode = currentRow[2];
                        item.Name = currentRow[3];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesStorefront> itList = new ConcurrentBag<iTunesStorefront>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 4)
                        return;
                    iTunesStorefront item = new iTunesStorefront();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.StorefrontId = Convert.ToInt32(currentRow[1]);
                    item.CountryCode = currentRow[2];
                    item.Name = currentRow[3];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }


        public static void UpdateiTunesTranslationType()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Filename & Table name
            var file = iTunesFileNames.translation_type;
            var table = iTunesDataSource.StringValueOfEnum(file);
            // Create diredctory ref
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            //string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            //string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            // Clear table for rebuild
            using (var Db = dbFactory.Open())
            {
                Db.ExecuteNonQuery("TRUNCATE TABLE iTunesTranslationType");
            }

            // Read file and populate list            
            List<string> itRows = new List<string>();
            string line;
            System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
            while ((line = fileReader.ReadLine()) != null)
            {
                // Get to the data
                if (line.StartsWith("#"))
                    continue;
                itRows.Add(line);
                if (itRows.Count == 100000)
                {
                    int countDown = 100000;
                    ConcurrentBag<iTunesTranslationType> itList = new ConcurrentBag<iTunesTranslationType>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != 3)
                            return;
                        iTunesTranslationType item = new iTunesTranslationType();
                        item.ExportDate = Convert.ToInt64(currentRow[0]);
                        item.TranslationTypeId = Convert.ToInt32(currentRow[1]);
                        item.Name = currentRow[2];

                        itList.Add(item);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {

                        if (itList.Count() > 0)
                        {
                            //try
                            //{
                            //    LoadRecordsHighSpeed(dt);
                            //}
                            //catch (Exception e) { }

                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                    {
                                        IvaClient.Get(new SendEmail
                                        {
                                            ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                            Subject = "iTunes Db error.",
                                            Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                        });
                                    }
                                }
                            }
                        }
                    }
                    itRows.Clear();
                }
            }

            if (itRows.Count > 0)
            {
                int countDown = itRows.Count;
                ConcurrentBag<iTunesTranslationType> itList = new ConcurrentBag<iTunesTranslationType>();
                var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                Parallel.ForEach(itRows, options, (itRow) =>
                {
                    Interlocked.Decrement(ref countDown);
                    var newRow = itRow.TrimEnd('\u0002');
                    var currentRow = newRow.Split('\u0001');
                    if (currentRow.Length != 3)
                        return;
                    iTunesTranslationType item = new iTunesTranslationType();
                    item.ExportDate = Convert.ToInt64(currentRow[0]);
                    item.TranslationTypeId = Convert.ToInt32(currentRow[1]);
                    item.Name = currentRow[2];

                    itList.Add(item);
                });

                // Copy to Db
                if (countDown == 0)
                {

                    if (itList.Count() > 0)
                    {
                        //try
                        //{
                        //    LoadRecordsHighSpeed(dt);
                        //}
                        //catch (Exception e) { }

                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = CollectorBase.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Get(new SendEmail
                                    {
                                        ToAddresses = new string[] { "techteam@internetvideoarchive.com" }.ToList(),
                                        Subject = "iTunes Db error.",
                                        Body = "There was an error during the SimpleSqlBulkCopy rebuilding the iTunes Db " + table + " table.\n\n" + e.Message
                                    });
                                }
                            }
                        }
                    }
                }
                itRows.Clear();
            }
        }

        #endregion Common

    }
}

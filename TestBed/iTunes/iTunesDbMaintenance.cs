using API.ServiceModel.DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MoreLinq;
using ServiceStack;
using ServiceStack.OrmLite;
using System.Configuration;
using Availability_Collectors_WebJob.Models.iTunes;
using System.ComponentModel;
using System.Reflection;
using System.Collections.Concurrent;
using System.Threading;
using System.Data;
using System.Data.SqlClient;

using API.ServiceModel.Availability;
using Availability_Collectors_WebJob.collectors;
using Availability_Collectors_WebJob.DataSources;
using Availability_Collectors_WebJob.Models;

namespace TestBed
{
    public class iTunesDbMaintenance
    {
        public void start()
        {


            GetiTunesMovies();

            List<Action> functions = new List<Action>();
            functions.Add(iTunesPopulateTables.CreateiTunesVideoPrice);
            //if (DateTime.Now.DayOfWeek == DayOfWeek.Thursday)
            //{
            //functions.Add(iTunesPopulateTables.CreateiTunesArtist);
            //functions.Add(iTunesPopulateTables.CreateiTunesCollection);
            //functions.Add(iTunesPopulateTables.CreateiTunesVideo);

            //functions.Add(iTunesPopulateTables.CreateiTunesArtistCollection);
            //functions.Add(iTunesPopulateTables.CreateiTunesArtistTranslation);
            //functions.Add(iTunesPopulateTables.CreateiTunesArtistType);
            //functions.Add(iTunesPopulateTables.CreateiTunesArtistVideo);

            //functions.Add(iTunesPopulateTables.CreateiTunesCollectionPrice);
            //functions.Add(iTunesPopulateTables.CreateiTunesCollectionTranslation);
            //functions.Add(iTunesPopulateTables.CreateiTunesCollectionType);
            //functions.Add(iTunesPopulateTables.CreateiTunesCollectionVideo);

            //functions.Add(iTunesPopulateTables.CreateiTunesGenre);
            //functions.Add(iTunesPopulateTables.CreateiTunesGenreArtist);
            //functions.Add(iTunesPopulateTables.CreateiTunesGenreCollection);
            //functions.Add(iTunesPopulateTables.CreateiTunesGenreVideo);

            //functions.Add(iTunesPopulateTables.CreateiTunesVideoPrice);
            //functions.Add(iTunesPopulateTables.CreateiTunesVideoTranslation);

            //functions.Add(iTunesPopulateTables.CreateiTunesRole);
            //functions.Add(iTunesPopulateTables.CreateiTunesMediaType);
            //functions.Add(iTunesPopulateTables.CreateiTunesParentalAdvisory);
            //functions.Add(iTunesPopulateTables.CreateiTunesStorefront);
            //functions.Add(iTunesPopulateTables.CreateiTunesTranslationType);
            //}
            //else
            //{
            //    functions.Add(iTunesUpdateTables.UpdateiTunesArtist);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesCollection);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesVideo);

            //    functions.Add(iTunesUpdateTables.UpdateiTunesArtistCollection);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesArtistTranslation);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesArtistType);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesArtistVideo);

            //    functions.Add(iTunesUpdateTables.UpdateiTunesCollectionPrice);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesCollectionTranslation);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesCollectionType);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesCollectionVideo);

            //    functions.Add(iTunesUpdateTables.UpdateiTunesGenre);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesGenreArtist);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesGenreCollection);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesGenreVideo);

            //    functions.Add(iTunesUpdateTables.UpdateiTunesVideoPrice);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesVideoTranslation);

            //    functions.Add(iTunesUpdateTables.UpdateiTunesRole);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesMediaType);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesParentalAdvisory);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesStorefront);
            //    functions.Add(iTunesUpdateTables.UpdateiTunesTranslationType);
            //}

            foreach (Action func in functions)
                func();
        }

        public void GetiTunesMovies()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);
            int Modifier = 5019;
            

            var response = new List<ProviderMovie>();
            List<iTunesVideo> itMovies = new List<iTunesVideo>();

            var CreateSettings = new GlobalCreateRecordSettings { };
            var MatchSettings = new GlobalMatchSettings { LimitToTheatrical = false };
            var source = new iTunesDataSource()
            {
                RecordLimit = -1,
                CreateRecordSettings = CreateSettings,
                MatchSettings = MatchSettings,
                Source = AvailabilitySource.iTunes,
                MaxThreads = 1,
                DaysBack = 2    //  How far back do we want deletes and new content?
            };

            try
            {
                // Get the new movies and import / update
                var movieCollector = new MovieCollector(source);
                var allMovies = movieCollector.GetAllMovies();
                movieCollector.ProcessMovies(allMovies);
            }
            catch (Exception e) { }

            using (var Db = dbFactory.Open())
            {
                var moviesSql = Db.From<iTunesVideo>().Where(x => x.MediaTypeId == 6);
                //var moviesSql = Db.From<iTunesVideo>().Where(x => x.Name == "Jumanji" && x.MediaTypeId == 6);
                itMovies = Db.Select<iTunesVideo>(moviesSql);
            }

            ConcurrentBag<ProviderMovie> pmList = new ConcurrentBag<ProviderMovie>();
            var options = new ParallelOptions { MaxDegreeOfParallelism = 10 };
            Parallel.ForEach(itMovies, options, (movie) =>
            {
                var itMovie = new ProviderMovie();

                itMovie.Title = movie.Name;
                itMovie.Year = movie.OriginalReleaseDate?.Year;
                itMovie.Source = AvailabilitySource.iTunes;
                itMovie.Modifier = Modifier;
                itMovie.Url = movie.ViewUrl;
                itMovie.PosterUrl = movie.ArtworkUrl;
                itMovie.Description = movie.LongDescription;
                itMovie.ProviderUniqueId = movie.VideoId.ToString();
                itMovie.Created = DateTime.Now;
                itMovie.Availabilities = new List<MovieAvailability>();

                List<iTunesArtistVideo> iContribs = new List<iTunesArtistVideo>();
                using (var Db = dbFactory.Open())
                {
                    var artistSql = Db.From<iTunesArtistVideo>().Where(x => x.VideoId == movie.VideoId);
                    iContribs = Db.Select<iTunesArtistVideo>(artistSql);

                    var artistIds = iContribs.Select(x => x.ArtistId).ToList();
                    var artistNameSql = Db.From<iTunesArtist>().Where(x => artistIds.Contains(x.ArtistId));
                    var iContribNames = Db.Select<iTunesArtist>(artistNameSql);

                    itMovie.Actors = iContribNames.Where(x => x.ArtistTypeId == 6).Select(x => x.Name).ToList();
                    itMovie.Directors = itMovie.Actors;
                }

                List<iTunesVideoPrice> offers = new List<iTunesVideoPrice>();
                using (var Db = dbFactory.Open())
                {
                    var artistSql = Db.From<iTunesVideoPrice>().Where(x => x.VideoId == movie.VideoId);
                    offers = Db.Select<iTunesVideoPrice>(artistSql);
                }
                if (offers.Count == 0)
                    return;

                foreach (var offer in offers)
                {
                    if (offer.RetailPrice == null)
                        continue;
                    using (var Db = dbFactory.Open())
                    {
                        var Links = new List<OfferLink>();
                        Links = new List<OfferLink>();
                        Links.Add(new OfferLink { Url = movie.ViewUrl, Platform = API.ServiceModel.Availability.Platform.Web });
                        var storefrontSql = Db.From<iTunesStorefront>().Where(x => x.StorefrontId == offer.StorefrontId);
                        var country = Db.Select<iTunesStorefront>(storefrontSql).Select(x => x.CountryCode).FirstOrDefault();

                        Enum.TryParse(offer.CurrencyCode, out CurrencyType cc);
                        var itMA = new MovieAvailability()
                        {
                            Provider = Provider.iTunes,
                            DeliveryMethod = DeliveryMethod.OnDemand,
                            Currency = cc,
                            PreSale = false,
                            Country = country,
                            Links = Links,
                            Created = DateTime.Now.ToUniversalTime(),
                            StartDate = offer.AvailabilityDate,
                            ExpirationDate = null,
                            Modifier = Modifier,
                            Modified = DateTime.Now.ToUniversalTime()
                        };
                        if (offer.SDPrice != null)
                        {
                            MovieAvailability itMA1 = itMA;
                            itMA1.Price = Decimal.Round(offer.SDPrice.Value, 2);
                            itMA1.Quality = VideoQuality.SD;
                            itMA1.OfferType = OfferType.Buy;
                            itMovie.Availabilities.Add(itMA1);
                        }
                        if (offer.SDRentalPrice != null)
                        {
                            MovieAvailability itMA2 = itMA;
                            itMA2.Price = Decimal.Round(offer.SDRentalPrice.Value, 2);
                            itMA2.Quality = VideoQuality.SD;
                            itMA2.OfferType = OfferType.Rent;
                            itMovie.Availabilities.Add(itMA2);
                        }
                        if (offer.HDRentalPrice != null)
                        {
                            MovieAvailability itMA3 = itMA;
                            itMA3.Price = Decimal.Round(offer.HDRentalPrice.Value, 2);
                            itMA3.Quality = VideoQuality.HD;
                            itMA3.OfferType = OfferType.Rent;
                            itMovie.Availabilities.Add(itMA3);
                        }
                        if (offer.HDPrice != null)
                        {
                            MovieAvailability itMA4 = itMA;
                            itMA4.Price = Decimal.Round(offer.HDPrice.Value, 2);
                            itMA4.Quality = VideoQuality.HD;
                            itMA4.OfferType = OfferType.Buy;
                            itMovie.Availabilities.Add(itMA4);
                        }
                    }
                }
                pmList.Add(itMovie);
            });
            response = pmList.ToList();
            // For PopulateDeletedMovies
            //CurrentiTunesIds = response.Select(x => x.ProviderUniqueId).ToList();

            //return response;
        }


        #region Legacy Generic Type Build
        public void startold()
        {
            var dbFactory = new OrmLiteConnectionFactory(ConfigurationManager.AppSettings["connectionstring.itunes"], SqlServer2012Dialect.Provider);

            // Create diredctory refs
            DateTime today = DateTime.Now;
            DateTime fullDay = DateTime.Now;
            while (fullDay.DayOfWeek != DayOfWeek.Wednesday)
            {
                fullDay = fullDay.AddDays(-1);
            }
            //string fileDate = today.Year.ToString("0000") + today.Month.ToString("00") + (today.Day - 2).ToString("00");
            //string fileDate = fullDay.Year.ToString("0000") + fullDay.Month.ToString("00") + fullDay.Day.ToString("00");
            string fileDate = "20180411";
            string baseFilePath = @"D:\\iTunes\itunes" + fileDate + @"\";
            string pricingFilePath = @"D:\\iTunes\pricing" + fileDate + @"\";

            foreach (iTunesFileNames file in Enum.GetValues(typeof(iTunesFileNames)))
            {
                // Discover Type we are working with and truncate table
                if (!(file == iTunesFileNames.collection))
                    continue;
                var table = StringValueOfEnum(file);
                using (var Db = dbFactory.Open())
                {
                    Db.ExecuteNonQuery("TRUNCATE TABLE " + table);
                }

                // instatiate type and get list of properties
                Type type = Type.GetType("Availability_Collectors_WebJob.Models.iTunes." + StringValueOfEnum(file) + ", Availability-Collectors-WebJob");
                // Create instance and get properties
                dynamic ito = Activator.CreateInstance(type);
                var props = ito.GetType().GetProperties();
                int propNum = 0;
                if (props[0].Name == "Id")
                    propNum = props.Length - 1;
                else
                    propNum = props.Length;
                // Create List
                //Type listType = typeof(List<>).MakeGenericType(new[] { type });
                //var itList = (IList)Activator.CreateInstance(listType);

                // Read file and populate list
                //var itRows = GetRows(baseFilePath + file);
                List<string> itRows = new List<string>();
                string line;
                System.IO.StreamReader fileReader = new System.IO.StreamReader(baseFilePath + file);
                while ((line = fileReader.ReadLine()) != null)
                {
                    // Get to the data
                    if (line.StartsWith("#"))
                        continue;
                    itRows.Add(line);
                    if (itRows.Count == 10000)
                    {
                        int countDown = 10000;
                        ConcurrentBag<object> itList = new ConcurrentBag<object>();
                        var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                        Parallel.ForEach(itRows, options, (itRow) =>
                        {
                            Interlocked.Decrement(ref countDown);

                            // Building objects dynamically
                            dynamic newItem = Activator.CreateInstance(type);
                            var newRow = itRow.TrimEnd('\u0002');
                            var currentRow = newRow.Split('\u0001');
                            if (currentRow.Length != propNum)
                                return;
                            int idx = 0;
                            foreach (var prop in props)
                            {
                                if (prop.Name == "Id")
                                    continue;
                                object data = null;
                                string val = currentRow[idx];
                                if (val == "")
                                {
                                    idx++;
                                    continue;
                                }
                                string x = prop.PropertyType.FullName;
                                Type pType = Type.GetType(prop.PropertyType.FullName);
                                if (pType.Name == "Boolean")
                                {
                                    data = Convert.ChangeType(Convert.ToInt32(val), pType);
                                }
                                else if (pType.FullName.Contains("DateTime"))
                                {
                                    DateTime d;
                                    val = val.Replace(" ", "-");
                                    if (DateTime.TryParse(val, out d))
                                    {
                                        DateTime? nd = d;
                                        if (nd.Value.Year < 1755)
                                            data = null;
                                        else
                                        {
                                            Type t = Nullable.GetUnderlyingType(pType);
                                            data = Convert.ChangeType(d, t);
                                        }
                                    }
                                    else
                                        data = null;
                                }
                                else if (pType.Name.StartsWith("Nullable"))
                                {
                                    Type t = Nullable.GetUnderlyingType(pType);
                                    data = Convert.ChangeType(val, t);
                                }
                                else
                                {
                                    data = Convert.ChangeType(val, pType);
                                }

                                type.GetProperty(prop.Name).SetValue(newItem, data);
                                idx++;
                            }
                            itList.Add(newItem);
                        });

                        // Copy to Db
                        if (countDown == 0)
                        {
                            // This code works, but requires Iva.SimpleSqlBulkCopy Nuget package from the Iva local feed
                            using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                            {
                                try
                                {
                                    ssbc.SqlBulkCopy.BulkCopyTimeout = 0;
                                    ssbc.WriteToServer(table, itList);
                                }
                                catch (Exception e)
                                {
                                    using (var IvaClient = Program.GetClient(new TimeSpan(0, 10, 0)))
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
                        itRows.Clear();
                    }
                }

                if (itRows.Count > 0)
                {
                    int countDown = itRows.Count;
                    ConcurrentBag<object> itList = new ConcurrentBag<object>();
                    var options = new ParallelOptions { MaxDegreeOfParallelism = 5 };
                    Parallel.ForEach(itRows, options, (itRow) =>
                    {
                        Interlocked.Decrement(ref countDown);
                        // Get to the data
                        if (itRow.StartsWith("#"))
                            return;

                        // Building objects dynamically
                        dynamic newItem = Activator.CreateInstance(type);
                        var newRow = itRow.TrimEnd('\u0002');
                        var currentRow = newRow.Split('\u0001');
                        if (currentRow.Length != propNum)
                            return;
                        int idx = 0;
                        foreach (var prop in props)
                        {
                            if (prop.Name == "Id")
                                continue;
                            object data = null;
                            string val = currentRow[idx];
                            if (val == "")
                            {
                                idx++;
                                continue;
                            }
                            string x = prop.PropertyType.FullName;
                            Type pType = Type.GetType(prop.PropertyType.FullName);
                            if (pType.Name == "Boolean")
                            {
                                data = Convert.ChangeType(Convert.ToInt32(val), pType);
                            }
                            else if (pType.FullName.Contains("DateTime"))
                            {
                                DateTime d;
                                val = val.Replace(" ", "-");
                                if (DateTime.TryParse(val, out d))
                                {
                                    DateTime? nd = d;
                                    if (nd.Value.Year < 1755)
                                        data = null;
                                    else
                                    {
                                        Type t = Nullable.GetUnderlyingType(pType);
                                        data = Convert.ChangeType(d, t);
                                    }
                                }
                                else
                                    data = null;
                            }
                            else if (pType.Name.StartsWith("Nullable"))
                            {
                                Type t = Nullable.GetUnderlyingType(pType);
                                data = Convert.ChangeType(val, t);
                            }
                            else
                            {
                                data = Convert.ChangeType(val, pType);
                            }

                            type.GetProperty(prop.Name).SetValue(newItem, data);
                            idx++;
                        }
                        itList.Add(newItem);
                    });

                    // Copy to Db
                    if (countDown == 0)
                    {
                        // This code works, but requires Iva.SimpleSqlBulkCopy Nuget package from the Iva local feed
                        using (var ssbc = new System.Data.SqlClient.SimpleSqlBulkCopy(System.Configuration.ConfigurationManager.AppSettings["connectionstring.itunes"]))
                        {
                            try
                            {
                                ssbc.WriteToServer(table, itList);
                            }
                            catch (Exception e)
                            {
                                using (var IvaClient = Program.GetClient(new TimeSpan(0, 10, 0)))
                                {
                                    IvaClient.Send(new SendEmail
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
        #endregion Legacy Generic Type Build


        private void LoadRecordsHighSpeed(DataTable dt)
        {
            var connString = ConfigurationManager.AppSettings["connectionstring.itunes"];
            using (var conn = new SqlConnection(connString))
            {
                conn.Open();
                var cmd = new SqlCommand();
                cmd.CommandTimeout = 120;
                cmd.CommandText = "iTunes_Add_Collection_HighSpeed";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Connection = conn;
                var param = new SqlParameter();
                param.ParameterName = "@iTunesCollectionData";
                param.SqlDbType = SqlDbType.Structured;
                cmd.Parameters.Add(param);

                cmd.Parameters["@iTunesCollectionData"].Value = dt;
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }




        public static string StringValueOfEnum(Enum value)
        {
            FieldInfo fi = value.GetType().GetField(value.ToString());
            DescriptionAttribute[] attributes = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (attributes.Length > 0)
            {
                return attributes[0].Description;
            }
            else
            {
                return value.ToString();
            }
        }

    }

    public class CountryCodeTranslation
    {

        public string Alpha2 { get; set; }

        public string Alpha3 { get; set; }

        public int Numeric { get; set; }

        public string CountryName { get; set; }
    }


}
using API.ServiceModel.DTO.TvMedia;
using API.ServiceModel.TvMedia;
using MoreLinq;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace TestBed
{
    public class TvMediaChannelLineups
    {

        public void start()
        {
            // Get API client
            var client = Program.GetClient(new TimeSpan(0, 1, 0));
            //Get Redis Client
            //IRedisClientsManager RedisManager = new RedisManagerPool(ConfigurationManager.AppSettings["connectionstring.redis"]);
            //IRedisClient redis = RedisManager.GetClient();


            //var resp = new SaveChannelLineupsResponse();
            var key = ConfigurationManager.AppSettings["TvMedia.ApiKey"];
            List<TvMediaLineup> stations = new List<TvMediaLineup>();

            var IvaClient = new ServiceStack.JsonHttpClient(Program.IvaAPI);
            var luIDs = new List<string>();
            int stationNum = 0;
            using (IvaClient)
            {
                try
                {
                    var countries = IvaClient.Get(new GetTvMediaCountries { TvMediaApiKey = key });
                    foreach (var cid in countries.Select(x => x.countryID))
                    {
                        var regions = IvaClient.Get(new GetTvMediaRegions { TvMediaApiKey = key, CountryID = cid });
                        foreach (var aid in regions.Select(x => x.regionID))
                        {
                            var areas = IvaClient.Get(new GetTvMediaServiceAreas { TvMediaApiKey = key, CountryID = cid, RegionID = aid });
                            foreach (var lid in areas.Select(x => x.areaID))
                            {

                                var lineUpIds = IvaClient.Get(new GetTvMediaLineupsByAreaID { TvMediaApiKey = key, CountryID = cid, RegionID = aid, AreaID = lid });
                                foreach (var id in lineUpIds.Select(x => x.lineupID))
                                {
                                    if (id != null)
                                    {
                                        luIDs.Add(id);
                                    }
                                }
                            }
                        }
                    }
                    var uniqueIDs = luIDs.Distinct();
                    var ct = uniqueIDs.Count();
                    foreach (var uniqueID in uniqueIDs)
                    {
                        //using (StreamWriter w = new StreamWriter(fs, Encoding.UTF8))
                        //{
                        //    w.WriteLine(DateTime.Now.ToString() + " Episode Cache error: " + imdbid + "\nFaulty scrape");
                        //}
                        try
                        {
                            var lineUp = IvaClient.Get(new GetTvMediaLineupByID { TvMediaApiKey = key, LineupID = uniqueID });
                            stations.Add(lineUp);
                            stationNum += lineUp.stations?.Count() ?? default(int);
                        }
                        catch (Exception e) { }
                    }
                }
                catch (Exception e) { }
            }

            // All Area stations only (Stations duped across areas)
            var uniqueStations = new List<Station>();
            stations.ForEach(x => uniqueStations.AddRange(x.stations.ToList().DistinctBy(y => y.stationID)));
            // Unique stations only
            var uStations = new List<Station>();
            uStations = uniqueStations.DistinctBy(y => y.stationID).ToList();

            // Holds ALL Station + Area flattened (Stations duped across areas)
            List<TvMediaLineupStations> lStations = new List<TvMediaLineupStations>();
            foreach (var station in stations)
            {
                foreach (var stat in station.stations)
                {
                    var lStat = new TvMediaLineupStations();
                    lStat.lineupID = station.lineupID;
                    lStat.lineupName = station.lineupName;
                    lStat.lineupType = station.lineupType;
                    lStat.providerID = station.providerID;
                    lStat.providerName = station.providerName;
                    lStat.serviceArea = station.serviceArea;
                    lStat.country = station.country;

                    lStat.number = lStat.number;
                    lStat.channelNumber = lStat.channelNumber;
                    lStat.subChannelNumber = lStat.subChannelNumber;
                    lStat.stationID = lStat.stationID;
                    lStat.name = lStat.name;
                    lStat.callsign = lStat.callsign;
                    lStat.network = lStat.network;
                    lStat.stationType = lStat.stationType;
                    lStat.NTSC_TSID = lStat.NTSC_TSID;
                    lStat.DTV_TSID = lStat.DTV_TSID;
                    lStat.Twitter = lStat.Twitter;
                    lStat.webLink = lStat.webLink;
                    lStat.logoFilename = lStat.logoFilename;
                    lStat.stationHD = lStat.stationHD;

                    lStations.Add(lStat);
                }
            }
            DataTable TVMediastations = new DataTable();
            TVMediastations = lStations.ToDataTable();


            // Store Lineups to table with station arrays ToJson
            DataTable TVM_Lineups = new DataTable();
            TVM_Lineups.Columns.Add("LineupID");
            TVM_Lineups.Columns.Add("LineupName");
            TVM_Lineups.Columns.Add("LineupType");
            TVM_Lineups.Columns.Add("ProviderID");
            TVM_Lineups.Columns.Add("ProviderName");
            TVM_Lineups.Columns.Add("ServiceArea");
            TVM_Lineups.Columns.Add("Country");
            TVM_Lineups.Columns.Add("Stations");

            foreach (var station in stations)
            {
                DataRow lineUpRow = TVM_Lineups.NewRow();
                lineUpRow["LineupID"] = station.lineupID;
                lineUpRow["LineupName"] = station.lineupName;
                lineUpRow["LineupType"] = station.lineupType;
                lineUpRow["ProviderID"] = station.providerID;
                lineUpRow["ProviderName"] = station.providerName;
                lineUpRow["ServiceArea"] = station.serviceArea;
                lineUpRow["Country"] = station.country;
                lineUpRow["Stations"] = station.stations.ToJson();

                TVM_Lineups.Rows.Add(lineUpRow);
            }



            //Start OUTPUT********************************************************
            string localDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            dynamic package = new OfficeOpenXml.ExcelPackage();
            dynamic worksheet1 = package.Workbook.Worksheets.Add("TVMedia Channel Lineups");

            worksheet1.Cells["A1"].LoadFromDataTable(TVM_Lineups, true);
            //worksheet1.Cells.AutoFitColumns()
            worksheet1.Cells.Style.WrapText = true;


            FileInfo f = new FileInfo(localDirectory + "\\TVMedia Channel Lineups.xlsx");
            if (f.Exists)
                f.Delete();
            package.SaveAs(f);


        }
    }
}

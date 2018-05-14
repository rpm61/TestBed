using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;
using API.ServiceModel.DTO.TvMedia;
using API.ServiceModel.TvMedia;
using Fiddler;
using HtmlAgilityPack;
using MoreLinq;
using ServiceStack;

namespace TestBed
{
    public class Program
    {
        public static string IvaAPI = "http://192.168.1.173";  // hogsmede
        //public static string IvaAPI = "https://free.iva-api.com";  // public not going through the portal.
        //public static string IvaAPI = "http://localhost:63012";


        static void Main(string[] args)
        {
            Console.WriteLine("TestBed engaged!!");

            //TvMediaChannelLineups process = new TvMediaChannelLineups();
            //GracenoteIDs process = new GracenoteIDs();
            iTunesDbMaintenance process = new iTunesDbMaintenance();

            process.start();
            
        }

        // Get API client
        public static ServiceStack.JsonHttpClient GetClient(TimeSpan Timeout)
        {
            var IvaClient = new ServiceStack.JsonHttpClient(IvaAPI);

            if (IvaAPI.Contains("free"))
            {
                IvaClient.SetCredentials("EE2dev", "^#!jJ=u920#{./P");
            }

            var c = IvaClient.GetHttpClient();
            c.Timeout = Timeout;
            return IvaClient;
        }



    }
}


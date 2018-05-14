using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Availability_Collectors_WebJob.Models
{ 

    public class HBOIdList
    {
        public string id { get; set; } 
        public int statusCode { get; set; }
        public Headers headers { get; set; }
        public Body body { get; set; }
    }

    public class Headers
    {
        public string ETag { get; set; }
        public string CacheControl { get; set; }
    }

    public class Body
    {
        public References references { get; set; }
        public Titles titles { get; set; }
        public Images images { get; set; }
        public string ratingCode { get; set; }
        public int duration { get; set; }
        public DateTime firstOfferedDate { get; set; }
        public bool isFree { get; set; }
        public string playbackMarkerId { get; set; }
        public string sortString { get; set; }
    }

    public class References
    {
        public string[] items { get; set; }
        public string viewable { get; set; }
    }

    public class Titles
    {
        public string _short { get; set; }
        public string full { get; set; }
    }

    public class Images
    {
        public string tile { get; set; }
        public string tilezoom { get; set; }
        public string background { get; set; }
        public string backgroundburnedin { get; set; }
        public string tileburnedin { get; set; }
        public string logo { get; set; }
        public string logopadded { get; set; }
        public string placeholder { get; set; }
    }

}

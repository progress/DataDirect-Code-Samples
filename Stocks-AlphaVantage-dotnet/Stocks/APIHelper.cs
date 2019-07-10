using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;

namespace oanet.damip
{
    class APIHelper
    {
        static HttpClient client = new HttpClient();
        private Dictionary<string, string> eqlConditions;
        private string customProperty;
        public APIHelper(Dictionary<string, string> eqlConditions, string customProperty)
        {
            this.eqlConditions = eqlConditions;
            this.customProperty = customProperty;
            
        }

        public JObject getDataFromAPI()
        {
            string requestURL = buildURL();
            WebClient c = new WebClient();
            var data = c.DownloadString(requestURL);
            JObject rootAlpha = JObject.Parse(data);
            return rootAlpha;
        }

        

        public string buildURL()
        {
            string baseURL = "https://www.alphavantage.co/query";
            var uriBuilder = new UriBuilder(baseURL);
            var paramValues = HttpUtility.ParseQueryString(uriBuilder.Query);


            foreach (KeyValuePair<string, string> entry in eqlConditions)
            {
                paramValues[entry.Key] = entry.Value;
            }
            paramValues["apikey"] = customProperty.Split('=')[1].ToString();
            uriBuilder.Query = paramValues.ToString();
            return uriBuilder.ToString();

        }

        
    }
}

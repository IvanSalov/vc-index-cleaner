using Nest;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                throw new ArgumentException("Wrong number of arguments, sorry very much");
            }

            var elasticSearchUrl = args[0];
            var indexName = args[1];
            var dbConnectionString = args[2];
            var processDelete = args.Length > 3 && args[3] == "d";

            var ids = RetrieveIdsFromElasticSearchIndex(elasticSearchUrl, indexName);
            var absentIds = RetrieveAbsentIdsFromDatabase(dbConnectionString, ids);

            Console.WriteLine("Absent ids:");

            foreach (var id in absentIds)
            {
                Console.Write($"{id} ");
            }

            if (processDelete)
            {
                DeleteFromElasticSearchIndex(elasticSearchUrl, indexName, absentIds);
            }
        }

        private static void DeleteFromElasticSearchIndex(string elasticSearchUrl, string indexName, List<string> absentIds)
        {
            var node = new Uri(elasticSearchUrl);

            var settings = new ConnectionSettings(node)
                .DefaultIndex(indexName);

            var client = new ElasticClient(settings);

            foreach(var id in absentIds)
            {
                var delteRequest = new DeleteRequest(indexName, "Product", id);
                client.Delete(delteRequest);
            }
        }

        private static List<string> RetrieveAbsentIdsFromDatabase(string dbConnectionString, List<string> ids)
        {
            var absentIds = new List<string>();

            using (var connection = new SqlConnection(dbConnectionString))
            {
                connection.Open();
                foreach (var id in ids)
                {
                    var query = connection.CreateCommand();
                    query.CommandText = $"SELECT Id FROM Item WHERE Id = '{id}'";
                    var reader = query.ExecuteReader();
                    if (!reader.HasRows)
                    {
                        absentIds.Add(id);
                    }
                    reader.Close();
                }
            }

            return absentIds;
        }

        private static List<string> RetrieveIdsFromElasticSearchIndex(string elasticSearchUrl, string indexName)
        {
            var node = new Uri(elasticSearchUrl);

            var settings = new ConnectionSettings(node)
                .DefaultIndex(indexName);

            var client = new ElasticClient(settings);

            var ids = new List<string>(16000);
            int skip = 0;
            int take = 1000;

            var searchRequest = new SearchRequest(indexName);
            searchRequest.Scroll = new Time(60000);
            searchRequest.From = skip;
            searchRequest.Size = take;
            searchRequest.Source = false;

            var response = client.Search<object>(searchRequest);

            ids.AddRange(response.Hits.Select(h => h.Id));

            while (response.Hits.Count > 0)
            {
                var scrollRequest = new ScrollRequest(response.ScrollId, new Time(60000));
                response = client.Scroll<object>(scrollRequest);
                ids.AddRange(response.Hits.Select(h => h.Id));
            }

            return ids;
        }
    }
}

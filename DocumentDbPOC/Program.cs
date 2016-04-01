using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace DocumentDbPOC
{
    class Program
    {
        //https://feedback.azure.com/forums/263030-documentdb/suggestions/6693091-be-able-to-do-partial-updates-on-document

        private const string EndpointUrl = "Endpoint Url";
        private const string AuthorizationKey = "Authorization Key";


        static void Main(string[] args)
        {
            try
            {
                GetStartedDemo().Wait();
            }
            catch (Exception e)
            {
                var baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
                Console.ReadLine();
            }
        }

        private static async Task GetStartedDemo()
        {
            // Create a new instance of the DocumentClient
            var client = new DocumentClient(new Uri(EndpointUrl), AuthorizationKey);

            // Check to verify a database with the id=FamilyRegistry does not exist
            var database = await CreateDatabase(client);

            var documentCollection = await CreateDocumentCollection(client, database);

            //await CreateDocument(client, database, documentCollection);
            //await SeedDatabase(client, database, documentCollection);

            //GetQueries(client, database, documentCollection);

            //await DeleteQueries(client, database, documentCollection);

            //await DeleteDatabase(client, database);

            // Write the new collection's id to the console
            Console.WriteLine(documentCollection.Id);
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
            Console.Clear();
        }



        private static async Task DeleteQueries(DocumentClient client, Database database, DocumentCollection documentCollection)
        {
            var andersenFamily = client.CreateDocumentQuery("dbs/" + database.Id + "/colls/" + documentCollection.Id)
                .Where(f => f.Id == "AndersenFamily")
                .AsEnumerable()
                .FirstOrDefault();

            //if (andersenFamily != null) await client.DeleteDocumentAsync(andersenFamily.SelfLink);

            await client.CreateDocumentAsync("dbs/" + database.Id + "/colls/" + documentCollection.Id, andersenFamily);
        }

        private static async Task DeleteDatabase(DocumentClient client, Database database)
        {
            // Clean up/delete the database 
            await client.DeleteDatabaseAsync("dbs/" + database.Id);
            client.Dispose();
        }

        private static void GetQueries(DocumentClient client, Database database, DocumentCollection documentCollection)
        {
            // Query the documents using DocumentDB SQL for the Andersen family.
            var families = client.CreateDocumentQuery("dbs/" + database.Id + "/colls/" + documentCollection.Id,
                "SELECT * " +
                "FROM Families f " +
                "WHERE f.id = \"AndersenFamily\"");

            foreach (var family in families)
            {
                Console.WriteLine("\tRead {0} from SQL", family);
            }

            // Query the documents using LINQ for the Andersen family.
            families =
                from f in client.CreateDocumentQuery("dbs/" + database.Id + "/colls/" + documentCollection.Id)
                where f.Id == "AndersenFamily"
                select f;

            foreach (var family in families)
            {
                Console.WriteLine("\tRead {0} from LINQ", family);
            }

            // Query the documents using LINQ lambdas for the Andersen family.
            families = client.CreateDocumentQuery("dbs/" + database.Id + "/colls/" + documentCollection.Id)
                .Where(f => f.Id == "AndersenFamily")
                .Select(f => f);

            foreach (var family in families)
            {
                Console.WriteLine("\tRead {0} from LINQ query", family);
            }
        }

        private static async Task SeedDatabase(DocumentClient client, Database database, DocumentCollection documentCollection)
        { 
            for (var i = 0; i < 100; i++)
            {
                var family = new Family
                {
                    Id = "SomeFamily" + i,
                    LastName = "FamilyName" + i,
                    Parents = new Parent[]
                   {
                        new Parent {FirstName = "Father" +i},
                        new Parent {FirstName = "Mother" + i}
                   },
                    Children = new Child[]
                   {
                        new Child
                        {
                            FirstName = "Child" + i,
                            Gender = "female",
                            Grade = 5,
                            Pets = new Pet[]
                            {
                                new Pet {GivenName = "Pet" + i}
                            }
                        }
                   },
                    Address = new Address { State = "WA", County = "King", City = "City" + i },
                    IsRegistered = true
                };

                await client.CreateDocumentAsync("dbs/" + database.Id + "/colls/" + documentCollection.Id, family);
            }

            for (var j = 0; j < 100; j++)
            {
                var store = new Store
                {
                    Id = "StoreId" + j,
                    Name = "Store" + j
                };

                for (var k = 0; k < 20; k++)
                {
                    var product = new Product { Id = "Product" + k };
                    store.Products.Add(product);
                }

                await client.CreateDocumentAsync("dbs/" + database.Id + "/colls/" + documentCollection.Id, store);
            }

        
        }


        private static async Task CreateDocument(DocumentClient client, Database database, DocumentCollection documentCollection)
        {
            var document =
                client.CreateDocumentQuery("dbs/" + database.Id + "/colls/" + documentCollection.Id)
                    .Where(d => d.Id == "AndersenFamily")
                    .AsEnumerable()
                    .FirstOrDefault();

            // If the document does not exist, create a new document
            if (document == null)
            {
                // Create the Andersen Family document
                Family andersonFamily = new Family
                {
                    Id = "AndersenFamily",
                    LastName = "Andersen",
                    Parents = new Parent[]
                    {
                        new Parent {FirstName = "Thomas"},
                        new Parent {FirstName = "Mary Kay"}
                    },
                    Children = new Child[]
                    {
                        new Child
                        {
                            FirstName = "Henriette Thaulow",
                            Gender = "female",
                            Grade = 5,
                            Pets = new Pet[]
                            {
                                new Pet {GivenName = "Fluffy"}
                            }
                        }
                    },
                    Address = new Address { State = "WA", County = "King", City = "Seattle" },
                    IsRegistered = true
                };

                // id based routing for the first argument, "dbs/FamilyRegistry/colls/FamilyCollection"
                await client.CreateDocumentAsync("dbs/" + database.Id + "/colls/" + documentCollection.Id, andersonFamily);
            }

            // Check to verify a document with the id=AndersenFamily does not exist
            // colls is prepended to the id to identify the parent resource: collections, along with the rest of the resource path: dbs/FamilyRegistry
            document =
                client.CreateDocumentQuery("dbs/" + database.Id + "/colls/" + documentCollection.Id)
                    .Where(d => d.Id == "WakefieldFamily")
                    .AsEnumerable()
                    .FirstOrDefault();

            if (document == null)
            {
                // Create the WakeField document
                Family wakefieldFamily = new Family
                {
                    Id = "WakefieldFamily",
                    Parents = new Parent[]
                    {
                        new Parent {FamilyName = "Wakefield", FirstName = "Robin"},
                        new Parent {FamilyName = "Miller", FirstName = "Ben"}
                    },
                    Children = new Child[]
                    {
                        new Child
                        {
                            FamilyName = "Merriam",
                            FirstName = "Jesse",
                            Gender = "female",
                            Grade = 8,
                            Pets = new Pet[]
                            {
                                new Pet {GivenName = "Goofy"},
                                new Pet {GivenName = "Shadow"}
                            }
                        },
                        new Child
                        {
                            FamilyName = "Miller",
                            FirstName = "Lisa",
                            Gender = "female",
                            Grade = 1
                        }
                    },
                    Address = new Address { State = "NY", County = "Manhattan", City = "NY" },
                    IsRegistered = false
                };

                // id based routing for the first argument, "dbs/FamilyRegistry/colls/FamilyCollection"
                await client.CreateDocumentAsync("dbs/" + database.Id + "/colls/" + documentCollection.Id, wakefieldFamily);
            }
        }

        private static async Task<DocumentCollection> CreateDocumentCollection(DocumentClient client, Database database)
        {
            var documentCollection =
                client.CreateDocumentCollectionQuery("dbs/" + database.Id)
                    .Where(c => c.Id == "DbCollectionsWrapper")
                    .AsEnumerable()
                    .FirstOrDefault();

            // If the document collection does not exist, create a new collection
            if (documentCollection == null)
            {
                documentCollection = await client.CreateDocumentCollectionAsync("dbs/" + database.Id,
                    new DocumentCollection
                    {
                        Id = "DbCollectionsWrapper"
                    });
            }
            return documentCollection;
        }

        private static async Task<Database> CreateDatabase(DocumentClient client)
        {
            var database = client.CreateDatabaseQuery().Where(db => db.Id == "DocumentDbPoc").AsEnumerable().FirstOrDefault();

            // If the database does not exist, create a new database
            if (database == null)
            {
                database = await client.CreateDatabaseAsync(
                    new Database
                    {
                        Id = "DocumentDbPoc"
                    });
            }
            return database;
        }

        internal sealed class Parent
        {
            public string FamilyName { get; set; }

            public string Type => "Parent";

            public string FirstName { get; set; }
        }

        internal sealed class Child
        {
            public string FamilyName { get; set; }
            public string FirstName { get; set; }
            public string Gender { get; set; }
            public int Grade { get; set; }
            public Pet[] Pets { get; set; }
        }

        internal sealed class Pet
        {
            public string GivenName { get; set; }
            public string Type => "Pet";
        }

        internal sealed class Address
        {
            public string State { get; set; }
            public string Type => "Address";
            public string County { get; set; }
            public string City { get; set; }
        }

        internal sealed class Family
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }
            public string Type => "Family";
            public string LastName { get; set; }
            public Parent[] Parents { get; set; }
            public Child[] Children { get; set; }
            public Address Address { get; set; }
            public bool IsRegistered { get; set; }
        }

        internal sealed class Product
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }
            public string Type => "Product";
        }

        internal sealed class Store
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }
            public string Type => "Store";
            public string Name { get; set; }
            public List<Product> Products { get; set; }

            public Store()
            {
                Products = new List<Product>();
            }
        }

        internal sealed class DbWrapper
        {
            public List<Family> Families { get; set; }
            public List<Store> Stores { get; set; }

            public DbWrapper()
            {
                this.Families = new List<Family>();
                this.Stores = new List<Store>();
            }
        }
    }
}

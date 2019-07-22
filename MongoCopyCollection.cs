using System;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoCopy
{
	class MongoCopyCollection
	{
		public static void run(string srcMongoConnectionString, string destMongoConnectionString, string collectionName, string dateField, string[] excludeFields)
		{
            BsonDefaults.GuidRepresentation = GuidRepresentation.Standard;

            ProjectionDefinition<BsonDocument> projectionDefinition = null;
            foreach (var s in excludeFields) projectionDefinition = projectionDefinition != null ? projectionDefinition.Exclude(s) : Builders<BsonDocument>.Projection.Exclude(s);

            var srcClient = new MongoClient(srcMongoConnectionString);
			var srcDB = srcClient.GetDatabase(MongoUrl.Create(srcMongoConnectionString).DatabaseName);
			
			var destClient = new MongoClient(destMongoConnectionString);
			var destDB = destClient.GetDatabase(MongoUrl.Create(destMongoConnectionString).DatabaseName);

			var srcCollection = srcDB.GetCollection<BsonDocument>(collectionName);
			var destCollection = destDB.GetCollection<BsonDocument>(collectionName);

            var fromDate = getMaxDate(destCollection, dateField);
			
			var count = getCount(srcCollection, dateField, fromDate);
			Console.WriteLine("Source collection '" + collectionName + "' size: " + count);

			if (count == 0) return;

			long inserted = 0;

			foreach (BsonValue docID in queryDocuments(srcCollection, dateField, fromDate).Project(x => x["_id"]).ToEnumerable())
			{
                var filter = Builders<BsonDocument>.Filter.Eq("_id", docID);

                BsonDocument doc;
                try
				{
                    doc = projectionDefinition != null
                            ? srcCollection.Find(filter).Project(projectionDefinition).First()
                            : srcCollection.Find(filter).First();
                }
				catch (InvalidOperationException e)
				{
					Console.WriteLine("Error during process document _id = " + docID);
                    Console.WriteLine("Exception message: " + e.Message);
                    return;
                }

                try
				{
					destCollection.InsertOne(doc);
				}
				catch (MongoWriteException e)
				{
					if (!e.ToString().Contains("E11000 duplicate key error collection")) throw;
				}
				inserted++;

				if (inserted % 100 == 0) Console.WriteLine(inserted + " / " + count + " (" + Math.Round((double)inserted / count * 100) + "%)");
			}

			Console.WriteLine(inserted + " / " + count + " (" + Math.Round((double)inserted / count * 100) + "%)");
		}

		static BsonValue getMaxDate(IMongoCollection<BsonDocument> collection, string dateField)
		{
			if (!string.IsNullOrEmpty(dateField))
			{
				return collection.Find(Builders<BsonDocument>.Filter.Empty)
							.Sort(Builders<BsonDocument>.Sort.Descending(dateField))
							.FirstOrDefault()?[dateField];
			}
			return null;
		}

		static long getCount(IMongoCollection<BsonDocument> collection, string dateField, BsonValue fromDate)
		{
			if (!string.IsNullOrEmpty(dateField) && fromDate != null)
			{
				return collection.CountDocuments(Builders<BsonDocument>.Filter.Gt(dateField, fromDate));
			}
			else
			{
				return collection.CountDocuments(Builders<BsonDocument>.Filter.Empty);
			}
		}

		static IFindFluent<BsonDocument, BsonDocument> queryDocuments(IMongoCollection<BsonDocument> collection, string dateField, BsonValue fromDate)
		{
			if (fromDate != null)
			{
				return collection.Find(Builders<BsonDocument>.Filter.Gt(dateField, fromDate)).Sort(Builders<BsonDocument>.Sort.Ascending(dateField));
			}
			else
			{
				return !string.IsNullOrEmpty(dateField) 
							? collection.Find(Builders<BsonDocument>.Filter.Empty).Sort(Builders<BsonDocument>.Sort.Ascending(dateField))
							: collection.Find(Builders<BsonDocument>.Filter.Empty);
			}
		}
	}
}

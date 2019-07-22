using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace MongoCopy
{
	class MongoId
	{
		[BsonId] public ObjectId _id { get; set; }
	}
}

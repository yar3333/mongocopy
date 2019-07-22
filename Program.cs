using System;
using System.Linq;

namespace MongoCopy
{
	class Program
	{
		static void Main(string[] args)
		{
            var excludeFields = args.Where(x => x.StartsWith("-exclude:")).Select(x => x.Substring("-exclude:".Length)).ToArray();
            var regularArgs = args.Where(x => !x.StartsWith("-exclude:")).ToArray();

            if (regularArgs.Length != 3 && regularArgs.Length != 4)
            {
                Console.WriteLine("Using:");
                Console.WriteLine("    MongoCopy <src_mongo_uri> <dest_mongo_uri> <collection_name> [ <sort_field_name> ] [ -exclude:<exclude_field> ]");
                return;
            }

            MongoCopyCollection.run(regularArgs[0], regularArgs[1], regularArgs[2], regularArgs.Length == 4 ? regularArgs[3] : null, excludeFields);
		}
	}
}

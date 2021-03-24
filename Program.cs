using System;

namespace SphinxXMLParser
{
	class Program
	{
		static void Main(string[] args)
		{
			var parser = new Parser();
			parser.Parse().GetAwaiter().GetResult();
		}
	}
}

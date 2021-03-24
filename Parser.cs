using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml;
using System.Xml.Schema;

namespace SphinxXMLParser
{
    class Parser
    {
        private readonly XmlDocument outputDocument;
        private int counter = 0;
        private const string docStart = @"<sphinx:docset xmlns:sphinx=""bogus"">" +
"<sphinx:schema>" +
@"<sphinx:field name=""subject""/>" +
@"<sphinx:field name=""content""/>" +
@"<sphinx:attr name=""title"" type=""string""/>" +
//@"<sphinx:attr name=""published"" type=""timestamp""/>" +
//@"<sphinx:attr name=""author_id"" type=""int"" bits=""16"" default=""1""/>" +
"</sphinx:schema>";
        private const string docEnd = "</sphinx:docset>";

        private readonly ActionBlock<(string, string)> transformingBlock;
        private readonly ActionBlock<string> savingBlock;

        private const int MAX_BUFFER_SIZE = 104857600;
        private readonly StringBuilder buffer = new StringBuilder(MAX_BUFFER_SIZE, MAX_BUFFER_SIZE * 2);

        private long outputFileLength = 0;

        public Parser()
        {
            outputDocument = new XmlDocument();
            outputDocument.LoadXml(
                docStart +
                docEnd
                );
            transformingBlock = new ActionBlock<(string, string)>(AppendNewDocumentNode, new ExecutionDataflowBlockOptions { BoundedCapacity = -1, MaxDegreeOfParallelism = -1, EnsureOrdered = false });
            savingBlock = new ActionBlock<string>(WriteOutput, new ExecutionDataflowBlockOptions { BoundedCapacity = -1, MaxDegreeOfParallelism = 1, MaxMessagesPerTask = 1, EnsureOrdered = false });
        }

        public async Task Parse()
        {
            var stream = new FileStream(Config.INPUT_FILE_PATH, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
            await Process(stream);
        }

        private void AppendNewDocumentNode((string, string) input)
        {
            var (title, content) = input;

            var subjectNode = outputDocument.CreateNode("element", "subject", "");
            subjectNode.InnerText = title;
            var contentNode = outputDocument.CreateNode("element", "content", "");
            contentNode.InnerText = content;
            var titleNode = outputDocument.CreateNode("element", "title", "");
            titleNode.InnerText = title;

            var documentNode = outputDocument.CreateElement("document");
            documentNode.SetAttribute("id", NextId().ToString());

            documentNode.AppendChild(subjectNode);
            documentNode.AppendChild(contentNode);
            documentNode.AppendChild(titleNode);

            var nodeOuterXml = new StringBuilder(documentNode.OuterXml, documentNode.OuterXml.Length)
                .Replace("{{", "")
                .Replace("}}", "")
                .Replace("[[", "")
                .Replace("]]", "")
                .Replace("document>", "sphinx:document>")
                .Replace("<document", "<sphinx:document")
                .ToString();

            savingBlock.Post(nodeOuterXml);
        }

        private async Task Process(Stream stream)
        {
            XmlReaderSettings settings = new XmlReaderSettings
            {
                Async = true
            };

            await File.WriteAllTextAsync(Config.OUTPUT_FILE_PATH, docStart);

            using (XmlReader reader = XmlReader.Create(stream, settings))
            {
                while (await reader.ReadAsync())
                {
                    if (reader.IsStartElement() && IsPage(reader.Name))
                    {
                        var (title, content) = await ParsePage(reader);
                        transformingBlock.Post((title, content));
                    }
                }
            }
            transformingBlock.Complete();
            await transformingBlock.Completion;
            savingBlock.Complete();
            await savingBlock.Completion;

            await File.AppendAllTextAsync(Config.OUTPUT_FILE_PATH, buffer.ToString() + docEnd);
        }

        private void WriteOutput(string documentNodeText)
        {
            buffer.Append(documentNodeText);
            if (buffer.Length > MAX_BUFFER_SIZE)
            {
                File.AppendAllText(Config.OUTPUT_FILE_PATH, buffer.ToString());
                outputFileLength += buffer.Length;
                buffer.Clear();
            }
        }

        private int NextId()
        {
            return Interlocked.Increment(ref counter);
        }

        private bool IsPage(string name)
        {
            return name == "page";
        }

        private bool StartsPageContent(XmlReader reader)
        {
            return reader.Name == "text";
        }

        private bool StartsPageSubject(XmlReader reader)
        {
            return reader.Name == "title";
        }

        private bool IsPageSubjectEnd(XmlReader reader)
        {
            return reader.NodeType == XmlNodeType.EndElement && reader.Name == "title";
        }

        private bool IsPageContentEnd(XmlReader reader)
        {
            return reader.NodeType == XmlNodeType.EndElement && reader.Name == "text";
        }

        private async Task<(string, string)> ParsePage(XmlReader reader)
        {
            string title = "", content = "";
            await reader.ReadAsync();
            for (; !(reader.NodeType == XmlNodeType.EndElement && IsPage(reader.Name)); await reader.ReadAsync())
            {
                if (StartsPageSubject(reader))
                {
                    for (; !IsPageSubjectEnd(reader); await reader.ReadAsync())
                    {
                        if (reader.NodeType == XmlNodeType.Text)
                        {
                            title = reader.Value;
                        }
                    }
                }
                if (StartsPageContent(reader))
                {
                    for (; !IsPageContentEnd(reader); await reader.ReadAsync())
                    {
                        if (reader.NodeType == XmlNodeType.Text)
                        {
                            content = reader.Value;
                        }
                    }
                }
            }

            return (title, content);
        }
    }
}

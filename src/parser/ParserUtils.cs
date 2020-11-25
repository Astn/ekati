using Antlr4.Runtime;
using cli.antlr;
using parser_grammer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace parser
{
    public class ParserUtils
    {
        public static UnbufferedTokenStream makeStream(string text)
        {
            var sr = new StringReader(text);
            var ins = new AntlrInputStream(sr);
            var lex = new AHGHEELexer(ins);
            return new UnbufferedTokenStream(lex);
        }
        public static UnbufferedTokenStream makeAHGHEEStream(string text)
        {
            var sr = new StringReader(text);
            var ins = new AntlrInputStream(sr);
            var lex = new AHGHEELexer(ins);
            return new UnbufferedTokenStream(lex);
        }
        public static UnbufferedTokenStream makeNTRIPLESStream(Stream text)
        {
            var ins = new AntlrInputStream(text);
            var lex = new NTRIPLESLexer(ins);
            return new UnbufferedTokenStream(lex);
        }
        public static UnbufferedTokenStream makeNTRIPLESStream(TextReader text)
        {
            var ins = new AntlrInputStream(text);
            var lex = new NTRIPLESLexer(ins);
            return new UnbufferedTokenStream(lex);
        }
        // this is code yanked from the UI.
        // It was parsing text and sending some data to the server, and 
        // when the server returned items, it was dealing with those items.
        // this could be usefull to refactor and use somewhere else.

        //private void AttachLimitStep(Ekati.Protocol.Grpc.Query query)
        //{
        //    var limitStep = new Ekati.Protocol.Grpc.Step
        //    {
        //        Limit = new LimitFilter
        //        {
        //            Value = Limit
        //        }
        //    };

        //    if (query.Step != null)
        //    {
        //        var lastStep = query.Step;
        //        while (lastStep.Next != null)
        //        {
        //            lastStep = lastStep.Next;
        //        }

        //        // we only attach if the user didn't put their own limit step on the query.
        //        if (lastStep.OperatorCase != Step.OperatorOneofCase.Limit)
        //        {
        //            lastStep.Next = limitStep;
        //        }
        //    }
        //    else
        //    {
        //        query.Step = limitStep;
        //    }

        //}


        //static void ParseStuff(string Source)
        //{
        //    var parser = new AHGHEEParser(Client.makeStream(Source));
        //    parser.BuildParseTree = true;
        //    parser.AddParseListener(listener: new Listener(async (nodes) =>
        //    {
        //        try
        //        {


        //            var pi = new ProgressItem
        //            {
        //                Message = "Putting Nodes",
        //                Count = 0,
        //                InProgress = true
        //            };
        //            ProgressItems.Add(pi);
        //            this.StateHasChanged();
        //            foreach (var node in nodes)
        //            {
        //                pi.Count++;
        //                await wat.PutAsync(node);
        //                pi.Count--;
        //            }
        //            this.StateHasChanged();
        //            pi.InProgress = false;
        //            this.StateHasChanged();
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"ex {ex} stack: {ex.StackTrace}");
        //        }
        //    }, async (nids, step) =>
        //    {
        //        var q = new Ekati.Grpc.Query();
        //        q.Iris.AddRange(nids.Select(x => x.Iri));
        //        q.Step = step;
        //        AttachLimitStep(q);

        //        var pi = new ProgressItem
        //        {
        //            Message = "Getting Nodes",
        //            Count = 0,
        //            InProgress = true
        //        };
        //        ProgressItems.Add(pi);

        //        this.StateHasChanged();
        //        var stream = wat.Get(q);
        //        var head = await stream.ResponseHeadersAsync;

        //        this.StateHasChanged();
        //        await stream.ResponseStream.ForEachAsync(async node =>
        //        {
        //            if (this.Nodes.Find(n => n.Id.Equals(node.Id)) == null)
        //            {
        //                this.Nodes.Add(node);
        //            }
        //            pi.Count++;
        //        });
        //        pi.InProgress = false;
        //        this.StateHasChanged();
        //    }, () => { },
        //        async (loadType, path) =>
        //        {
        //            var pi = new ProgressItem
        //            {
        //                Message = "Loading Nodes: " + path,
        //                Count = 1,
        //                InProgress = true
        //            };
        //            ProgressItems.Add(pi);
        //            this.StateHasChanged();
        //            var loadProgress = wat.Load(new LoadFile
        //            {
        //                Type = loadType,
        //                Path = path.Trim('\"')
        //            });
        //            await loadProgress.ResponseStream.ForEachAsync(async progress =>
        //            {
        //                pi.Count = progress.Progress;
        //                pi.OfTotal = progress.Length;
        //                this.StateHasChanged();
        //            });
        //            this.StateHasChanged();
        //        }));
        //    parser.AddErrorListener(new ErrorListener());
        //    AHGHEEParser.CommandContext cc = null;

        //    for (; ; cc = parser.command())
        //    {
        //        if (cc?.exception != null
        //            //&& cc.exception.GetType() != typeof(Antlr4.Runtime.InputMismatchException)
        //            //&& cc.exception.GetType() != typeof(Antlr4.Runtime.NoViableAltException)
        //            )
        //        {
        //            var pi = new ProgressItem
        //            {
        //                Message = $"{cc.exception.Message} - found {cc.exception.OffendingToken.Text} at Line {cc.exception.OffendingToken.Line} offset at {cc.exception.OffendingToken.StartIndex}",
        //                Count = 0,
        //                InProgress = true
        //            };
        //            ProgressItems.Add(pi);
        //        }

        //        if (parser.CurrentToken.Type == TokenConstants.Eof)
        //        {
        //            break;
        //        }
        //    }
        //}
    }
}

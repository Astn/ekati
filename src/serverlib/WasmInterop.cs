using System;
using Wasmtime;
using Xunit.Abstractions;

namespace Ahghee.Grpc
{
    public class WasmInterop
    {
        private readonly dynamic _instance;

        public WasmInterop(dynamic instance)
        {
            _instance = instance;
        }

        public void Run()
        {
            _instance.run();
        }
    }

    public class RunGlobalExample:IDisposable
    {
        private Module mo;
        private dynamic instance;

        public RunGlobalExample(ITestOutputHelper output)
        {
            var host = new Wasmtime.Host();
            Called = false;
            // make a function available to wasm.
            var initialValue = 1;
            var glob = host.DefineMutableGlobal<Int32>("", "global", 1);
            host.DefineFunction(
                "",
                "print_global",
                () =>
                {
                    Called = true;
                    output.WriteLine("The global value is " + glob.Value.ToString());
                });

            mo = host.LoadModuleText("global.wat");
            instance = host.Instantiate(mo);
        }

        public void Run()
        {
            instance.run(20);
        }
        public bool Called { get; set; }

        public void Dispose()
        {
            instance.Dispose();
            mo.Dispose();
        }
    }
    
    public class RunMemoryExample: IDisposable
    {
        private Host _host;
        private Module _module;
        private dynamic _instance;

        public RunMemoryExample(ITestOutputHelper output)
        {
            _host = new Host();

            _host.DefineFunction(
                "",
                "log",
                (Caller caller, int address, int length) =>
                {
                    var bytes = caller.GetMemory("mem").Span;
                    var message = caller.GetMemory("mem").ReadString(address, length);
                    output.WriteLine($"Message from WebAssembly: {message}");
                    Called = true;
                }
            );

            _module = _host.LoadModuleText("memory.wat");

            _instance = _host.Instantiate(_module);
            _instance.run();
        }
        public void Run()
        {
            _instance.run();
        }
        public bool Called { get; set; }
        public void Dispose()
        {
            _instance.Dispose();
            _module.Dispose();
            _host.Dispose();
        }
    }
}
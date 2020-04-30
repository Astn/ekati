module WasmTest

open Ahghee.Grpc
open Ahghee.Grpc
open System
open Xunit
open Xunit.Abstractions


type MyTests(output:ITestOutputHelper) =
    
    [<Fact>]
    member __.``Can load and run a wat file`` () =
        let host = new Wasmtime.Host()
        let mutable called = false
        // make a function available to wasm.
        host.DefineFunction(
                               "",
                               "hello",
                               new Action(fun () ->
                                   called <- true
                                   output.WriteLine("hello from wasm"))
                           )
        use mo = host.LoadModuleText("hello.wat")
        use instance = host.Instantiate(mo)
        let interop = new WasmInterop(instance)
        interop.Run()
        
        Assert.True(called)  

    [<Fact>]
    member __.``Can wat a mutable global`` () =
        
        use ex = new RunGlobalExample(output)
        ex.Run()
        Assert.True(ex.Called)
    
    [<Fact>]
    member __.``Can wat a memory example`` () =
        
        use ex = new RunMemoryExample(output)
        ex.Run()
        Assert.True(ex.Called)    
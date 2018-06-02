module Program = 
  open System.Reflection
  open Tests
  open Xunit.Abstractions

  type outs() =
    interface ITestOutputHelper with 
      member x.WriteLine (s:string) = printf "%A\n" s
      member x.WriteLine ((format:string), ([<System.ParamArrayAttribute>] args : obj[])) : unit = printf "%A\n" format
        
      

  let [<EntryPoint>] main _ = 
    let tester = new outs() :> ITestOutputHelper
    let mt = MyTests(tester)
    mt.``Multiple calls to add for the same nodeId results in all the fragments being linked`` "StorageType.GrpcFile" 2
    printf "Done"
    0

namespace EventStoreIO

open System
open System.Diagnostics
open System.IO

module Program =

    [<EntryPoint>]
    let main argv = 

        match argv |> List.ofArray |> List.map Endpoint.parse with

        | Some source :: Some target :: [] ->

            printfn ""
            printfn "SOURCE\n------\n%A" source
            printfn ""
            printfn "TARGET\n------\n%A" target
            printfn ""

            let watch = Stopwatch.StartNew()
            let count, last = ref 0, ref 0
            
            let intercept(index,_) =
                let elapsed = int (watch.Elapsed.TotalSeconds / 5.2)
                if elapsed > !last then
                    printfn "@ %s events transferred: %d" (DateTime.Now.ToString("hh:mm:ss.fff")) index
                    last := elapsed
                count := 1+index

            Stream.transferWithIntercept source target intercept

            printfn ""
            printfn "COMPLETED"
            printfn "---------"
            printfn "%d events transferred in %A" !count watch.Elapsed
            printfn ""

        | _ -> 

            let name = Path.GetFileName(Process.GetCurrentProcess().MainModule.FileName)

            printfn ""
            printfn "USAGE: %s source target" name
            printfn ""
            printfn "      source/target can be:"
            printfn ""
            printfn "      --file=[path-to-file.json]"
            printfn "      --gzip=[path-to-compressed-file.json.gz]"
            printfn "      --host=[username[:password]@]hostname[:port][/stream_name]"
            printfn ""
            printfn "            e.g. admin:changeit@localhost:1113/destination-a31613b3a13"
            printfn ""
            printfn " import a json file:"
            printfn "      %s --file=stream-a31613b3e13.json --host=localhost" name
            printfn ""
            printfn " import a json file to a specific stream:"
            printfn "      %s --file=stream-a31613b3e13.json --host=admin:changeit@localhost:1113/copied-a31613b3a13" name
            printfn ""
            printfn " export all to a json file:"
            printfn "      %s --host=localhost --file=exported.json" name
            printfn ""
            printfn " export a specific stream from localhost to file:"
            printfn "      %s --host=/source_stream --file=exported.json" name
            printfn ""
        
        0
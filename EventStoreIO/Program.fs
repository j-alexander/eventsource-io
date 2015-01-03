namespace EventStoreIO

open System

module Program =

    [<EntryPoint>]
    let main argv = 

        match argv |> List.ofArray with

        | "export" :: file :: host :: stream :: [] ->
            
            EventStore.connect host 1113 "admin" "changeit"
            |> Stream.export stream file

        | "import" :: file :: host :: tail ->

            let stream = List.tryHead tail

            EventStore.connect host 1113 "admin" "changeit"
            |> Stream.import file stream 

        | _ -> 
            printfn ""
            printfn "EventStoreIO export targetFile sourceHost sourceStream"
            printfn "EventStoreIO import sourceFile targetHost [targetStream - overrides the input stream value]"
            printfn ""
            printfn " e.g. EventStoreIO export .\\stream-a31613b3e13.json localhost stream-a31613b3e13"
            printfn "      EventStoreIO import .\\stream-a31613b3e13.json localhost"
            printfn "      EventStoreIO import .\\stream-a31613b3e13.json localhost copied-a31613b3e13"
            printfn ""

        0
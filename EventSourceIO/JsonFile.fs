namespace EventSourceIO

open System
open System.IO
open System.IO.Compression
open System.Text
open EventStore.ClientAPI
open FSharp.Data

module JsonFile =

    let private toJson (event : Event) =
        try
            [| yield "type", event.Type |> JsonValue.String
               yield "stream", event.Stream |> JsonValue.String
               yield "date", event.Date.ToUniversalTime().ToString("o") |> JsonValue.String
               match event.Data |> Encoding.UTF8.GetString with
               | x when (x |> String.IsNullOrWhiteSpace) -> ()
               | x -> yield "data", x |> JsonValue.Parse
               match event.Metadata |> Encoding.UTF8.GetString with
               | x when (x |> String.IsNullOrWhiteSpace) -> ()
               | x -> yield "metadata", x |> JsonValue.Parse
            |] |> JsonValue.Record |> Some
        with e ->
            printfn "Skipping: %s " (event.Data |> Encoding.UTF8.GetString)
            printfn "Error: %A" e
            None

    let private fromJson (json : JsonValue) =
        { Type = json.["type"].AsString()
          Stream = json.["stream"].AsString()
          Date =
            match json.TryGetProperty("date") with
            | None -> DateTime.Now
            | Some x -> x.AsDateTime()
          Data =
            match json.TryGetProperty("data") with
            | None -> Array.empty
            | Some x -> x.ToString() |> Encoding.UTF8.GetBytes
          Metadata =
            match json.TryGetProperty("metadata") with
            | None -> Array.empty
            | Some x -> x.ToString() |> Encoding.UTF8.GetBytes }

    let private minimize (json : JsonValue) =
        json.ToString(JsonSaveOptions.DisableFormatting)

    let write (target : FileInfo) (events : seq<Event>) =
        File.WriteAllLines(target.FullName, events |> Seq.choose(toJson) |> Seq.map(minimize))

    let read (source : FileInfo) : seq<Event> =
        File.ReadLines(source.FullName) |> Seq.map(JsonValue.Parse>>fromJson)


    module Compressed =

        let write (target : FileInfo) (events : seq<Event>) =
            use stream = File.OpenWrite(target.FullName)
            use gzip = new GZipStream(stream, CompressionLevel.Optimal)
            use writer = new StreamWriter(gzip)

            for json in events |> Seq.choose(toJson) |> Seq.map(minimize) do
                writer.WriteLine(json)
                writer.Flush()

        let read (source : FileInfo) : seq<Event> =
            seq {
                use stream = File.OpenRead(source.FullName)
                use gzip = new GZipStream(stream, CompressionMode.Decompress)
                use reader = new StreamReader(gzip)
                
                while not reader.EndOfStream do
                    yield reader.ReadLine() |> JsonValue.Parse |> fromJson
            }
            
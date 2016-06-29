namespace EventSourceIO

open System
open System.IO
open System.IO.Compression
open System.Text
open EventStore.ClientAPI
open FSharp.Data

module JsonFile =


    let private toJson =
            
        let encodeFieldAs json text = function
            | null | [||] -> []
            | bytes ->
                match bytes |> Encoding.UTF8.GetString with
                | x when String.IsNullOrWhiteSpace x -> []
                | x ->
                    try [ json, JsonValue.Parse x ]
                    with e ->
                        [ text, JsonValue.String x ]

        fun (event : Event) ->
            [| yield "type", event.Type |> JsonValue.String
               yield "stream", event.Stream |> JsonValue.String
               yield "date", event.Date.ToUniversalTime().ToString("o") |> JsonValue.String

               yield! encodeFieldAs "data" "text" event.Data
               yield! encodeFieldAs "metadata" "metatext" event.Metadata
            |] |> JsonValue.Record


    let private fromJson =

        let decodeDate (data:JsonValue) =
            match data.TryGetProperty("date") with
            | None -> DateTime.Now
            | Some x ->
                try x.AsDateTime()
                with e -> DateTime.Now

        let decodeFieldAs json text (data:JsonValue) =
            match data.TryGetProperty(json) with
            | Some x -> Encoding.UTF8.GetBytes (x.ToString())
            | None ->
                match data.TryGetProperty(text) with
                | Some (JsonValue.String x) -> Encoding.UTF8.GetBytes x
                | _ -> [||]

        fun (json : JsonValue) ->
            { Type = json.["type"].AsString()
              Stream = json.["stream"].AsString()
              Date = decodeDate json
              Data = decodeFieldAs "data" "text" json
              Metadata = decodeFieldAs "metadata" "metatext" json }


    let private minimize (json : JsonValue) =
        json.ToString(JsonSaveOptions.DisableFormatting)

    let write (target : FileInfo) (events : seq<Event>) =
        File.WriteAllLines(target.FullName, events |> Seq.map(toJson >> minimize))

    let read (source : FileInfo) : seq<Event> =
        File.ReadLines(source.FullName) |> Seq.map(JsonValue.Parse>>fromJson)


    module Compressed =

        let write (target : FileInfo) (events : seq<Event>) =
            use stream = File.OpenWrite(target.FullName)
            use gzip = new GZipStream(stream, CompressionLevel.Optimal)
            use writer = new StreamWriter(gzip)

            for json in events |> Seq.map(toJson >> minimize) do
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
            
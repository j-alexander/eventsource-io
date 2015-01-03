namespace EventStoreIO

open System
open System.IO
open System.Text
open EventStore.ClientAPI
open FSharp.Data

module Stream =

    let export (sourceStream : string)
               (targetFile : string)
               (connection : IEventStoreConnection) =

        File.WriteAllLines(targetFile,
            EventStore.read connection sourceStream 0
            |> AsyncSeq.map(fun event ->
                async {
                    let record =
                        [| yield "type", event.Event.EventType |> JsonValue.String
                           yield "stream", event.Event.EventStreamId |> JsonValue.String
                           match event.Event.Data |> Encoding.UTF8.GetString with
                           | x when (x |> String.IsNullOrWhiteSpace) -> ()
                           | x -> yield "data", x |> JsonValue.Parse
                           match event.Event.Metadata |> Encoding.UTF8.GetString with
                           | x when (x |> String.IsNullOrWhiteSpace) -> ()
                           | x -> yield "metadata", x |> JsonValue.Parse
                        |] |> JsonValue.Record
                    return record.ToString(JsonSaveOptions.DisableFormatting)
                })
            |> AsyncSeq.toSeq)

    let import (sourceFile : string)
               (targetStream : string option)
               (connection : IEventStoreConnection) =
        
        File.ReadLines(sourceFile)
        |> Seq.filter(String.IsNullOrWhiteSpace >> not)
        |> Seq.map JsonValue.Parse
        |> Seq.iter (fun json ->
            let eventType = json.["type"].AsString()
            let eventStream =
                match targetStream with
                | Some stream -> stream
                | None -> json.["stream"].AsString()
            let eventData = 
                match json.TryGetProperty("data") with
                | None -> Array.empty
                | Some x -> x.ToString() |> Encoding.UTF8.GetBytes
            let eventMetadata =
                match json.TryGetProperty("metadata") with
                | None -> Array.empty
                | Some x -> x.ToString() |> Encoding.UTF8.GetBytes

            let result =
                EventStore.write connection eventStream -2 eventType eventData eventMetadata
            ())

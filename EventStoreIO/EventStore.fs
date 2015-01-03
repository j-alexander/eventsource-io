namespace EventStoreIO

open System
open System.Diagnostics
open System.Net
open System.Net.Sockets
open EventStore.ClientAPI
open EventStore.ClientAPI.SystemData

module EventStore =

    let connect (host : string)
                (port : int)
                (username : string)
                (password : string) =
        let settings =
            let credentials = new UserCredentials(username, password)
            ConnectionSettings.Create().SetDefaultUserCredentials(credentials).Build()
        let endpoint =
            let address = 
                host
                |> Dns.GetHostAddresses
                |> Seq.find(fun x -> x.AddressFamily = AddressFamily.InterNetwork)
            new IPEndPoint(address, port)
        let connection = EventStoreConnection.Create(settings, endpoint)
        connection.ConnectAsync().Wait()
        connection

    let rec read (connection : IEventStoreConnection)
                 (stream : string)
                 (from : int) =
        asyncSeq {
            let sliceTask = connection.ReadStreamEventsForwardAsync(stream, from, 1000, true)
            let! slice = sliceTask |> Async.AwaitTask
            match slice.Status with
            | SliceReadStatus.StreamDeleted -> failwith (sprintf "Stream %s at (%d) was deleted." stream from)
            | SliceReadStatus.StreamNotFound -> failwith (sprintf "Stream %s at (%d) was not found." stream from)
            | SliceReadStatus.Success ->
                if slice.Events.Length > 0 then
                    for event in slice.Events do
                        yield event
                    yield! read connection stream slice.NextEventNumber
            | x -> failwith (sprintf "Stream %s at (%d) produced undocumented response: %A" stream from x)
        }

    let write (connection : IEventStoreConnection)
              (stream : string)
              (expectedVersion : int)
              (eventType : string)
              (eventData : byte[])
              (eventMetadata : byte[]) =
        let eventId = Guid.NewGuid()
        let eventData = new EventData(eventId, eventType, true, eventData, eventMetadata)
        connection.AppendToStreamAsync(stream, expectedVersion, eventData)
        |> Async.AwaitTask
        |> Async.RunSynchronously
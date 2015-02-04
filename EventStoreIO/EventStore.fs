namespace EventStoreIO

open System
open System.Diagnostics
open System.Net
open System.Net.Sockets
open EventStore.ClientAPI
open EventStore.ClientAPI.SystemData

module EventStore =

    type HostInfo = {
        Username : string
        Password : string
        Name : string
        Stream : string option
        Port : int
    }

    let connect (host : HostInfo) : IEventStoreConnection =
        let settings =
            let credentials = new UserCredentials(host.Username, host.Password)
            ConnectionSettings.Create().SetDefaultUserCredentials(credentials).Build()
        let endpoint =
            let address = 
                host.Name
                |> Dns.GetHostAddresses
                |> Seq.find(fun x -> x.AddressFamily = AddressFamily.InterNetwork)
            new IPEndPoint(address, host.Port)
        let connection = EventStoreConnection.Create(settings, endpoint)
        connection.ConnectAsync().Wait()
        connection

    let rec private readStream (connection : IEventStoreConnection)
                               (stream : string)
                               (from : int) : seq<Event> =
        seq {
            let sliceTask = connection.ReadStreamEventsForwardAsync(stream, from, 1000, true)
            let slice = sliceTask |> Async.AwaitTask |> Async.RunSynchronously
            match slice.Status with
            | SliceReadStatus.StreamDeleted -> failwith (sprintf "Stream %s at (%d) was deleted." stream from)
            | SliceReadStatus.StreamNotFound -> failwith (sprintf "Stream %s at (%d) was not found." stream from)
            | SliceReadStatus.Success ->
                if slice.Events.Length > 0 then
                    for resolvedEvent in slice.Events do
                        yield { Type = resolvedEvent.Event.EventType
                                Stream = resolvedEvent.Event.EventStreamId
                                Data = resolvedEvent.Event.Data
                                Metadata = resolvedEvent.Event.Metadata }
                    yield! readStream connection stream slice.NextEventNumber
            | x -> failwith (sprintf "Stream %s at (%d) produced undocumented response: %A" stream from x)
        }

    let private writeEvent (connection : IEventStoreConnection)
                           (targetStream : string option)
                           (event : Event) =
        let eventId = Guid.NewGuid()
        let eventStream = 
            match targetStream with
            | None -> event.Stream
            | Some stream -> stream
        let eventData = new EventData(eventId, event.Type, true, event.Data, event.Metadata)
        connection.AppendToStreamAsync(eventStream, -2, eventData)
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> ignore

    let read (host : HostInfo) : seq<Event>=
        let connection = connect host
        let stream = match host.Stream with Some x -> x | None -> "$all"
        readStream connection stream 0

    let write (host : HostInfo) : seq<Event> -> unit =
        let connection = connect host
        Seq.iter (writeEvent connection host.Stream)
        


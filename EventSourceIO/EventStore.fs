namespace EventSourceIO

open System
open System.Diagnostics
open System.Net
open System.Net.Sockets
open NLog.FSharp
open EventStore.ClientAPI
open EventStore.ClientAPI.SystemData

type private NLogLogger(log:Logger) =
    let format f a = String.Format(f,a)
    interface ILogger with
        member x.Debug(f,a) =   log.Debug            "%s" (format f a)
        member x.Debug(e,f,a) = log.DebugException e "%s" (format f a)
        member x.Error(f,a) =   log.Error            "%s" (format f a)
        member x.Error(e,f,a) = log.ErrorException e "%s" (format f a)
        member x.Info(f,a) =    log.Info             "%s" (format f a)
        member x.Info(e,f,a) =  log.InfoException e  "%s" (format f a)

module EventStore =

    let log = new Logger()

    type HostInfo = {
        Username : string
        Password : string
        Name : string
        Port : int
        Stream : string option
        From : int option
    }

    let connect (host : HostInfo) : IEventStoreConnection =
        let settings =
            ConnectionSettings
                .Create()
                .SetDefaultUserCredentials(new UserCredentials(host.Username, host.Password))
                .UseCustomLogger(new NLogLogger(log))
                .Build()
        let endpoint =
            let address = 
                host.Name
                |> Dns.GetHostAddresses
                |> Seq.find(fun x -> x.AddressFamily = AddressFamily.InterNetwork)
            new IPEndPoint(address, host.Port)
        let connection = EventStoreConnection.Create(settings, endpoint)
        connection.ConnectAsync().Wait()
        connection

    let rec private readAll (connection : IEventStoreConnection)
                            (from : Position) : seq<Event> =
        seq {
            let sliceTask = connection.ReadAllEventsForwardAsync(from, 100, true)
            let slice = sliceTask |> Async.AwaitTask |> Async.RunSynchronously
            if slice.Events.Length > 0 then
                for resolvedEvent in slice.Events do
                    yield { Type = resolvedEvent.Event.EventType
                            Stream = resolvedEvent.Event.EventStreamId
                            Date = resolvedEvent.Event.Created
                            Data = resolvedEvent.Event.Data
                            Metadata = resolvedEvent.Event.Metadata }
                yield! readAll connection slice.NextPosition
        }

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
                                Date = resolvedEvent.Event.Created
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

    let read (host : HostInfo) : seq<Event> =
        let connection = connect host
        let stream = match host.Stream with Some x -> x | None -> "$all"
        let from = match host.From with Some x -> x | None -> 0
        match stream with
        | "$all" -> readAll connection (Position.Start)
        | stream -> readStream connection stream from

    let write (host : HostInfo) : seq<Event> -> unit =
        let connection = connect host
        Seq.iter (writeEvent connection host.Stream)
        


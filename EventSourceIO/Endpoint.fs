namespace EventSourceIO

open System
open System.Diagnostics
open System.IO


type Endpoint =
    | Json of FileInfo
    | GZip of FileInfo
    | EventStore of EventStore.HostInfo
    | AzureQueue of AzureQueue.QueueInfo
    | Kafka of Kafka.ClusterInfo


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Endpoint =

    let private split (separators : string) (input : string) = 
        input.Split(separators |> Array.ofSeq) |> List.ofArray
    let private remove (text : string) (input : string) =
        input.Replace(text, "")
    let private lower (input : string) =
        input.ToLowerInvariant()
 
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Json =

        let defaults = new FileInfo(sprintf "export-%d.json" (DateTime.Now.Ticks))

        /// [path-to-file.json]
        let parse(input:string) : FileInfo option=
            try new FileInfo(input) |> Some
            with _ ->
                printfn ""
                printfn "ERROR: Unable to intepret file '%s'." input
                printfn ""
                None

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module GZip =
        
        let defaults = new FileInfo(sprintf "export-%d.json.gz" (DateTime.Now.Ticks))

        /// [path-to-compressed-file.json.gz]
        let parse = Json.parse

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module EventStore =
        
        open EventStore

        let defaults = { Username="admin"
                         Password="changeit"
                         Name="localhost";
                         Port=1113
                         Stream=None
                         From=None }

        /// username[:password]
        let parseCredentials(input:string)(host:HostInfo option) : HostInfo option =
            match host, input |> split ":" with
            | Some host, username :: password :: [] ->
                { host with Username=username; Password=password } |> Some
            | Some host, username :: [] ->
                printfn "Enter password (%s@%s):" username host.Name
                { host with Username=username; Password=Console.ReadLine()} |> Some
            | _ -> None

        /// hostname
        let parseName(name:string)(host:HostInfo option) : HostInfo option =
            match host, String.IsNullOrWhiteSpace(name) with
            | Some host, false -> { host with Name=name } |> Some
            | host, _ -> host

        /// from
        let parseFrom(input:string)(host:HostInfo) : HostInfo option =
            match input |> Int32.TryParse with
            | true, from -> { host with From=Some from } |> Some
            | _ -> host |> Some

        /// stream[+port]
        let parseStream(stream:string)(host:HostInfo option) : HostInfo option =
            match host, String.IsNullOrWhiteSpace(stream), stream |> split "+" with
            | Some host, false, stream :: [] ->
                { host with Stream=Some stream } |> Some
            | Some host, false, stream :: port :: _ ->
                { host with Stream=Some stream } |> parseFrom port
            | host, _, _ -> host

        /// port
        let parsePort(input:string)(host:HostInfo option) : HostInfo option =
            match host, input |> Int32.TryParse with
            | Some host, (true, port) -> { host with Port=port } |> Some
            | _ -> host

        /// hostname[:port][/stream_name]
        let parseNamePortAndStream(input:string)(host:HostInfo option) : HostInfo option =
            match input |> split ":/" with
            | name :: port :: stream :: [] ->
                host |> parseName name |> parsePort port |> parseStream stream
            | name :: port :: [] when (input.Contains(":")) ->
                host |> parseName name |> parsePort port
            | name :: stream :: [] ->
                host |> parseName name |> parseStream stream
            | name :: [] ->
                host |> parseName name
            | [] ->
                host
            | unknown ->
                printfn ""
                printfn "ERROR: Unable to parse EventStore host name: %A" unknown
                printfn ""
                None
        
        /// [username[:password]@]hostname[:port][/stream_name]
        let parse(input:string) : HostInfo option=
            match input |> split "@" with
            | credentials :: host :: [] ->
                defaults |> Some |> parseNamePortAndStream host |> parseCredentials credentials
            | host :: [] ->
                defaults |> Some |> parseNamePortAndStream host
            | unknown ->
                printfn ""
                printfn "ERROR: Unable to parse EventStore host information: %A" unknown
                printfn ""
                None

    [<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
    module Kafka =

        open Kafka

        let defaultHost = { Name = "localhost"; Port = 9092 }
        let defaults = { Hosts = [defaultHost]; Topic = "all" }

        /// port
        let parsePort(input:string)(host:HostInfo option) : HostInfo option =
            match host, input |> Int32.TryParse with
            | Some host, (true, port) -> { host with Port=port } |> Some
            | _ -> host

        // name
        let parseName(name:string)(host:HostInfo option) : HostInfo option =
            match host, String.IsNullOrWhiteSpace(name) with
            | Some host, false -> { host with Name = name } |> Some
            | _ -> None

        /// name[:port]
        let parseNameAndPort (host:string) : HostInfo option =
            match host |> split ":" with
            | name :: [] -> defaultHost |> Some |> parseName name
            | name :: port :: [] -> defaultHost |> Some |> parseName name |> parsePort port
            | _ -> None

        /// hostname[:port][,hostname2[:port2][,hostname3[:port3][...]]]
        let parseHosts(hosts:string)(cluster:ClusterInfo option) : ClusterInfo option =
            match cluster, hosts |> split "," |> List.choose parseNameAndPort with
            | Some cluster, x :: xs -> { cluster with Hosts = x :: xs } |> Some
            | _ -> cluster

        /// topic
        let parseTopic(topic:string)(cluster:ClusterInfo option) : ClusterInfo option =
            match cluster, String.IsNullOrWhiteSpace(topic) with
            | Some cluster, false -> { cluster with Topic = topic } |> Some
            | _ -> None

        /// hostname[:port][,hostname2[:port2][,hostname3[:port3][...]]]/topic
        let parse(input:string) : ClusterInfo option =
            match input |> split "/" with
            | hosts :: topic :: [] -> defaults |> Some |> parseHosts hosts |> parseTopic topic
            | unknown ->
                printfn ""
                printfn "ERROR: Unable to parse Kafka host information: %A" unknown
                printfn ""
                None


    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module AzureQueue =

        open AzureQueue

        /// queue@connectionstring
        let parse(input:string) : QueueInfo option =
            match input |> split "@" with
            | name :: connectionString :: [] when name |> String.IsNullOrWhiteSpace |> not ->
                { QueueInfo.Name = name; QueueInfo.ConnectionString = connectionString } |> Some
            | unknown ->
                printfn ""
                printfn "ERROR: Unable to parse AzureQueue information: %A" unknown
                printfn ""
                None



    let private (|EventStoreOption|JsonOption|GZipOption|KafkaOption|AzureQueueOption|None|) (input:string) =
        match input |> lower |> remove "-" with
        | "j" | "json" -> JsonOption
        | "g" | "gzip" -> GZipOption
        | "e" | "eventstore" -> EventStoreOption
        | "q" | "azurequeue" -> AzureQueueOption
        | "k" | "kafka" -> KafkaOption
        | _ -> None

    let parse(input:string) : Endpoint option =
        match input |> split "=" with
        | JsonOption :: value :: [] -> value |> Json.parse |> Option.map(Endpoint.Json)
        | GZipOption :: value :: [] -> value |> GZip.parse |> Option.map(Endpoint.GZip)
        | KafkaOption :: value :: [] -> value |> Kafka.parse |> Option.map(Endpoint.Kafka)
        | EventStoreOption :: value :: [] -> value |> EventStore.parse |> Option.map(Endpoint.EventStore)
        | JsonOption :: [] -> Json.defaults |> Endpoint.Json |> Some
        | GZipOption :: [] -> GZip.defaults |> Endpoint.GZip |> Some
        | KafkaOption :: [] -> Kafka.defaults |> Endpoint.Kafka |> Some
        | EventStoreOption :: [] -> EventStore.defaults |> Endpoint.EventStore |> Some
        | AzureQueueOption :: x -> x |> String.concat "=" |> AzureQueue.parse |> Option.map(Endpoint.AzureQueue)
        | _ -> None

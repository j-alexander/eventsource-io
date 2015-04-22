namespace EventStoreIO

open System
open System.Diagnostics
open System.IO
open EventStore
open EventStore.ClientAPI


type Endpoint =
    | File of FileInfo
    | GZip of FileInfo
    | Host of HostInfo


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Endpoint =

    let private split (separators : string) (input : string) = 
        input.Split(separators |> Array.ofSeq) |> List.ofArray
    let private remove (text : string) (input : string) =
        input.Replace(text, "")
 
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module File =

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
        let parse = File.parse

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Host =

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
                printfn "ERROR: Unable to parse host name: %A" unknown
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
                printfn "ERROR: Unable to parse host information: %A" unknown
                printfn ""
                None

    let private (|HostOption|FileOption|GZipOption|None|) (input:string) =
        match input |> remove "-" with
        | "f" | "file" -> FileOption
        | "g" | "gzip" -> GZipOption
        | "h" | "host" -> HostOption
        | _ -> None

    let parse(input:string) : Endpoint option =
        match input |> split "=" with
        | FileOption :: value :: [] -> value |> File.parse |> Option.map(Endpoint.File)
        | GZipOption :: value :: [] -> value |> GZip.parse |> Option.map(Endpoint.GZip)
        | HostOption :: value :: [] -> value |> Host.parse |> Option.map(Endpoint.Host)
        | FileOption :: [] -> File.defaults |> Endpoint.File |> Some
        | GZipOption :: [] -> GZip.defaults |> Endpoint.GZip |> Some
        | HostOption :: [] -> Host.defaults |> Endpoint.Host |> Some
        | _ -> None

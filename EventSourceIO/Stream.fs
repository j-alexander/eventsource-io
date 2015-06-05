namespace EventSourceIO

module Stream =

    let createSource =
        function
        | Endpoint.EventStore host -> EventStore.read host
        | Endpoint.Json file -> JsonFile.read file
        | Endpoint.GZip file -> JsonFile.Compressed.read file
        | Endpoint.Kafka cluster -> Kafka.read cluster

    let createTarget =
        function
        | Endpoint.EventStore host -> EventStore.write host
        | Endpoint.Json file -> JsonFile.write file
        | Endpoint.GZip file -> JsonFile.Compressed.write file
        | Endpoint.Kafka cluster -> Kafka.write cluster

    let transferWithIntercept (source:Endpoint) (target:Endpoint) (fn : int*Event -> unit) =
        let intercept i x =
            fn(i,x)
            x

        createSource source |> Seq.mapi intercept |> createTarget target
        
    let transfer (source:Endpoint) (target:Endpoint) =
        transferWithIntercept source target ignore
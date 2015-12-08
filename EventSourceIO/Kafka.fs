﻿namespace EventSourceIO

open System
open System.Net
open System.Text
open KafkaNet
open KafkaNet.Model
open KafkaNet.Protocol

module Kafka =

    type HostInfo = {
        Name : string
        Port : int
    }

    type ClusterInfo = {
        Hosts : HostInfo list
        Topic : string
    }

    
    type Offsets = Map<int, int64>

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Offsets =

        let private pairwise (l : Offsets, r : Offsets) = 
            let joiner partition (l,_) (_,r) = (l,r)
            let l = l |> Map.map (fun _ x -> x, -1L)
            let r = r |> Map.map (fun _ x -> -1L, x)
            Map.join joiner l r

        let incomplete =
            pairwise
            >> Map.exists(fun _ (current, required) -> current + 1L < required)

        let byPartition (consumer : Consumer) =
            consumer.GetTopicOffsetAsync
            >> Async.AwaitTask
            >> Async.RunSynchronously
            >> Seq.choose(fun x -> 

                match x.Error, Seq.toList x.Offsets with
                | 0s, [ finish; start ] when (start < finish) -> Some (x.PartitionId, (start, finish))
                | _ -> None)

            >> Map.ofSeq

    let route (hosts : HostInfo seq) =
        let hosts = [| for x in hosts -> new Uri(sprintf "http://%s:%d" x.Name x.Port) |]
        new BrokerRouter(new KafkaOptions(hosts))
            
    let read (cluster : ClusterInfo) =
        seq {
            let topic = cluster.Topic
            use router = cluster.Hosts |> route
            
            let start, finish : Offsets*Offsets =
                let consumer = new Consumer(new ConsumerOptions(topic, router))
                let partitions = Offsets.byPartition consumer topic
                
                partitions |> Map.map(fun partition (start,_) -> start),
                partitions |> Map.map(fun partition (_,finish) -> finish)

            use consumer = 
                let start = [| for partition, offset in start |> Map.toSeq -> new OffsetPosition(partition, offset) |]
                new Consumer(new ConsumerOptions(topic, router), start)
                

            let events = consumer.Consume().GetEnumerator()

            let rec loop (current : Offsets) =
                seq {
                    if Offsets.incomplete(current, finish) && events.MoveNext() then

                        let event = events.Current
                        let eventType = event.Key |> function | null | [||] -> topic
                                                              | key -> Encoding.UTF8.GetString key

                        let partition = event.Meta.PartitionId
                        let offset = event.Meta.Offset

                        yield { Type = eventType
                                Stream = topic
                                Date = DateTime.Now
                                Data = event.Value
                                Metadata = Array.empty }
                        yield! current |> Map.add partition offset |> loop
                }

            yield! loop (start)
        }
        
    let write (cluster : ClusterInfo) (events : seq<Event>) : unit =
        use router = cluster.Hosts |> route
        use producer = new Producer(router)

        let events =
            seq {
                for event in events ->
                    new Message(Encoding.UTF8.GetString event.Data, event.Type)
            }

        producer.SendMessageAsync(cluster.Topic, events)
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> ignore
        
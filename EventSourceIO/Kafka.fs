namespace EventSourceIO

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

        let byPartition (consumer : Consumer) : string -> Offsets =
            consumer.GetTopicOffsetAsync
            >> Async.AwaitTask
            >> Async.RunSynchronously
            >> Seq.map(fun x -> x.PartitionId, x.Offsets |> Seq.head)
            >> Map.ofSeq

    let route (hosts : HostInfo seq) =
        let hosts = [| for x in hosts -> new Uri(sprintf "http://%s:%d" x.Name x.Port) |]
        new BrokerRouter(new KafkaOptions(hosts))
            
    let read (cluster : ClusterInfo) =
        seq {
            let topic = cluster.Topic
            use router = cluster.Hosts |> route

            let required =
                let consumer = new Consumer(new ConsumerOptions(topic, router))
                Offsets.byPartition consumer topic 

            use consumer = 
                let start = [| for partition in required |> Map.keys -> new OffsetPosition(partition,0L) |]
                new Consumer(new ConsumerOptions(topic, router), start)

            let events = consumer.Consume().GetEnumerator()

            let rec loop (current : Offsets) =
                seq {
                    if Offsets.incomplete(current, required) && events.MoveNext() then
                        let event = events.Current

                        let eventType =
                            if event.Key = null then topic
                            elif event.Key |> Array.isEmpty then topic
                            else event.Key |> Encoding.UTF8.GetString

                        let partition = event.Meta.PartitionId
                        let offset = event.Meta.Offset

                        yield { Type = eventType
                                Stream = topic
                                Data = event.Value
                                Metadata = Array.empty }
                        yield! current |> Map.add partition offset |> loop
                }

            yield! loop (Map.empty)
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
        
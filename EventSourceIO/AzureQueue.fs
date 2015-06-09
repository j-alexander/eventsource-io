namespace EventSourceIO

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Queue

module AzureQueue =

    type QueueInfo = {
        Name : string
        ConnectionString : string
    }

    let private connect(queue : QueueInfo) : CloudQueue =
        let account = CloudStorageAccount.Parse(queue.ConnectionString)
        let client = account.CreateCloudQueueClient()
        let queue = client.GetQueueReference(queue.Name)
        queue.CreateIfNotExists() |> ignore
        queue
        
    let read (queue : QueueInfo) : seq<Event> =
        let queue = connect queue
        let rec dequeue() =
            try let message = queue.GetMessage()
                queue.DeleteMessage(message)
                seq {
                    yield { Event.Type = queue.Name
                            Event.Stream = queue.Name
                            Event.Data = message.AsBytes
                            Event.Metadata = Array.empty }
                    yield! dequeue()
                }
            with _ -> Seq.empty
        dequeue()

    let write (queue : QueueInfo) (events : seq<Event>) : unit =
        let queue = connect queue
        for event in events do
            queue.AddMessage(new CloudQueueMessage(event.Data))

        
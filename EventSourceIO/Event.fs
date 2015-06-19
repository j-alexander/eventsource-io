namespace EventSourceIO

open System

type Event = {
    Type : string
    Stream : string
    Date : DateTime
    Data : byte[]
    Metadata : byte[]
}
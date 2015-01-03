eventstore-io
=============

a tool for the command line interface to read eventstore streams into json files, or specially formatted json files into eventstore streams

download a .NET 4.5 (client profile) binary from the master source tree here:

https://github.com/j-alexander/eventstore-io/blob/master/EventStoreIO.exe?raw=true


example usage:
```
EventStoreIO export targetFile sourceHost sourceStream
EventStoreIO import sourceFile targetHost [targetStream - overrides the input stream value]

 e.g. EventStoreIO export .\\stream-a31613b3e13.json localhost stream-a31613b3e13
      EventStoreIO import .\\stream-a31613b3e13.json localhost
      EventStoreIO import .\\stream-a31613b3e13.json localhost copied-a31613b3e13
```

events are stored in minimized json, new-line separated, according to the following format:
```JSON
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
...
```
data and metadata fields are inline, json-formatted, in order to facilitate simple editing

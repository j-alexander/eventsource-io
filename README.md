eventstore-io
=============

a tool for the command line interface to read eventstore streams into json files, or specially formatted json files into eventstore streams

download a .NET 4.5 (client profile) binary from the master source tree here:

https://github.com/j-alexander/eventstore-io/blob/master/EventStoreIO.exe?raw=true


```

USAGE: EventStoreIO.exe source target

      source/target can be:

      --file=[path-to-file.json]
      --gzip=[path-to-compressed-file.json.gz]
      --host=[username[:password]@]hostname[:port][/stream_name]

            e.g. admin:changeit@localhost:1113/destination-a31613b3a13

 import a json file:
      EventStoreIO.exe --file=stream-a31613b3e13.json --host=localhost

 import a json file to a specific stream:
      EventStoreIO.exe --file=stream-a31613b3e13.json --host=admin:changeit@localhost:1113/copied-a31613b3a13

 export all to a json file:
      EventStoreIO.exe --host=localhost --file=exported.json

 export a specific stream from localhost to file:
      EventStoreIO.exe --host=/source_stream --file=exported.json
	  
```

events are stored in minimized json, new-line separated, according to the following format:
```JSON
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
...
```
data and metadata fields are inline, json-formatted, in order to facilitate simple editing

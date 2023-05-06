# sumjson

Summarizes the content of multiple similarly shaped JSON objects. 

## usage

The objects must be one after another, not part of an overlapping array.

Correct:
```
{}
{}
{}
{}
```

Incorrect:
```
[
    {},
    {},
    {},
    {}
]
```

Although the giant array would work, the result would be an empty summary.

## limitations

At this time the algorithm runs using O(n) amount of memory with regard to the input. Processing a 21MiB JSON file takes about 7s on my 14" 2023 Apple MacBook Pro, M2 Pro CPU with 32GB of RAM.

## license

MIT.
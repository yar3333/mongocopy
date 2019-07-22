# MongoCopy

Windows utility to copy collection from one mongo instance to another.
Utility use specified field to sort documents and can continue after interrupt.


## Example (here `timestamp` is a field name to sort by)

```sh
MongoCopy.exe mongodb://source:27017 mongodb://destination:27017 myCollection timestamp
```

# verdaccio-az-blob

Azure blob storage plugin for Verdaccio 6

In Verdaccio config: 
```yaml
...

store:
   az-blob:
     account: my-account
     accountKey: My-53cr3t-k3Y    # either key or a name of env variable with a key
     container: my-container-with-npm
```
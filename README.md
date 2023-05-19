# verdaccio-az-blob

In Verdaccio config: 
```yaml
...

store:
   az-blob:
     account: my-account
     accountKey: My-53cr3t-k3Y    # either specifu key...
     accountKeyENV: AZ_BLOB_KEY   # ...or use env variable with it
     packagesContainerName: my-container-with-npm
     secretContainerName: my-container-with-npm-secret #optional, in case if packages container needs to be public
```
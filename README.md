# verdaccio-az-blob

In Verdaccio config: 
```yaml
...

store:
   az-blob:
     account: my-account
     accountKey: My-53cr3t-k3Y
     packagesContainerName: my-container-with-npm
     secretContainerName: my-container-with-npm-secret #optional, in case if packages container is public
```
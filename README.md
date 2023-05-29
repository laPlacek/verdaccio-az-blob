# verdaccio-az-blob

Azure blob storage plugin for Verdaccio 6

In Verdaccio config: 
```yaml
...

store:
  az-blob:
    account: my-account
    accountKey: My-53cr3t-k3Y
    container: my-container-with-npm

# or with env variables

store:
  az-blob:
    account: MY_ACCOUNT_NAME
    accountKey: ACCOUNT_KEY
    container: SOME_CONTAINER_NAME
```
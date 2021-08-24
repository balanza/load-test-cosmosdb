# CosmosDB resource usage test
Performs several query against a cosmos db instance to compare RU usage

### Usage
```sh
yarn install --frozen-lockfile
yarn build

COSMOSDB_URI=cosmos uri> \ 
COSMOSDB_KEY=<cosmos key> \
COSMOSDB_NAME=<cosmos db name>  \
yarn start <total number of documents> <documents per page>
```

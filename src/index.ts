/* eslint-disable functional/no-let */
import { CosmosClient } from "@azure/cosmos";
import { ulid } from "ulid";

const cosmosDbUri = process.env.COSMOSDB_URI;
const cosmosDbName = process.env.COSMOSDB_NAME;
const cosmosDbKey = process.env.COSMOSDB_KEY;
const cosmosDbContainerName = ulid(); // random name

if (!cosmosDbUri || !cosmosDbName || !cosmosDbKey || !cosmosDbContainerName) {
  console.log({
    cosmosDbUri,
    cosmosDbName,
    cosmosDbKey,
    cosmosDbContainerName,
  });
  throw new Error("Missing config");
}

const nOfDocuments = Number(process.argv[2]) || 100;
const pageSize = Number(process.argv[3]) || 10;
const partitionKeyName = "my_partition_key";
const partitionKey = ulid(); // random value;

const cosmosdbClient = new CosmosClient({
  endpoint: cosmosDbUri,
  key: cosmosDbKey,
});
const cosmosdbInstance = cosmosdbClient.database(cosmosDbName);
const containerResp = await cosmosdbInstance.containers.create({
  id: cosmosDbContainerName,
});
const container = containerResp.container;

// eslint-disable-next-line functional/no-let
for (let i = 0; i < nOfDocuments; i++) {
  await container.items.create({
    id: ulid(),
    index: i,
    [partitionKeyName]: partitionKey,
  });
  console.log(`Document ${i} created`);
}

// test 1: query by continuation token
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const queryByContinuationToken = (continuationToken?: string) => {
  console.log("queryByContinuationToken", continuationToken);
  return container.items
    .query(
      {
        parameters: [
          {
            name: "@pk",
            value: partitionKey,
          },
        ],
        query: `SELECT * FROM m WHERE m.${partitionKeyName} = @pk ORDER BY m._ts DESC`,
      },
      {
        //   continuationToken,
        maxItemCount: 10,
      }
    )
    .getAsyncIterator();
};

let c = 0;
let lastContinuationToken;
while (true) {
  const iterable = queryByContinuationToken(lastContinuationToken);
  //const iterable = container.items.readAll().getAsyncIterator();
  const iterator = iterable[Symbol.asyncIterator]();

  const result = await iterator.next();

  const f = result.value;
  lastContinuationToken = f.continuationToken;
  console.log(
    "--->",
    f.continuationToken,
    f.continuation,
    lastContinuationToken
  );

  if (!f.hasMoreResults) {
    console.log("exit", !!f, f?.hasMoreResults);
    break;
  }

  c++;
}

// test 2: query by id range

// test 3: query by date range

// clean up test
await cosmosdbInstance.container(cosmosDbContainerName).delete();

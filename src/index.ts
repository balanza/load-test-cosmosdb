/* eslint-disable sort-keys */
/* eslint-disable functional/immutable-data */
/* eslint-disable functional/no-let */
import * as fs from "fs";
import * as path from "path";
import { CosmosClient, FeedResponse } from "@azure/cosmos";
import { ulid } from "ulid";
import { mean, median, percentile } from "stats-lite";

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

const nOfDocuments = Number(process.argv[2]) || 200;
const pageSize = Number(process.argv[3]) || 10;
const resultFileName = process.argv[4] || "/tmp/results.csv";
const partitionKeyName = "my_partition_key";
const partitionKey = ulid(); // random value;

const cosmosdbClient = new CosmosClient({
  endpoint: cosmosDbUri,
  key: cosmosDbKey,
});
const cosmosdbInstance = cosmosdbClient.database(cosmosDbName);
const containerResp = await cosmosdbInstance.containers.create({
  id: cosmosDbContainerName,
  partitionKey: `/${partitionKeyName}`,
});
const container = containerResp.container;

// eslint-disable-next-line functional/no-let
for (let i = 0; i < nOfDocuments; i++) {
  await container.items.create({
    id: ulid(),
    index: i,
    [partitionKeyName]: partitionKey,
  });
  // console.log(`Document ${i} created`);
}

// test 1: query by continuation token
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const queryByContinuationToken = (
  continuationToken?: string
): AsyncIterable<FeedResponse<unknown>> =>
  container.items
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
        continuationToken,
        maxItemCount: pageSize,
      }
    )
    .getAsyncIterator();

const queryByLastId = (lastId?: string) =>
  container.items
    .query(
      lastId
        ? {
            parameters: [
              {
                name: "@pk",
                value: partitionKey,
              },
              {
                name: "@lastId",
                value: lastId,
              },
            ],
            query: `SELECT * FROM m WHERE m.${partitionKeyName} = @pk and m.id < @lastId ORDER BY m._ts DESC`,
          }
        : {
            parameters: [
              {
                name: "@pk",
                value: partitionKey,
              },
            ],
            query: `SELECT * FROM m WHERE m.${partitionKeyName} = @pk ORDER BY m._ts DESC`,
          },
      {
        maxItemCount: pageSize,
      }
    )
    .getAsyncIterator();

const queryByLastDate = (lastDate?: string) =>
  container.items
    .query(
      lastDate
        ? {
            parameters: [
              {
                name: "@pk",
                value: partitionKey,
              },
              {
                name: "@lastDate",
                value: lastDate,
              },
            ],
            query: `SELECT * FROM m WHERE m.${partitionKeyName} = @pk and m._ts < @lastDate ORDER BY m._ts DESC`,
          }
        : {
            parameters: [
              {
                name: "@pk",
                value: partitionKey,
              },
            ],
            query: `SELECT * FROM m WHERE m.${partitionKeyName} = @pk ORDER BY m._ts DESC`,
          },
      {
        maxItemCount: pageSize,
      }
    )
    .getAsyncIterator();

interface T {
  readonly name: string;
  readonly query: (
    next: string | undefined
  ) => AsyncIterable<FeedResponse<unknown>>;
  readonly getNext: (f: FeedResponse<unknown>) => string | undefined;
}

const promises = [
  {
    name: "queryByContinuationToken",
    query: queryByContinuationToken,
    getNext: (f: FeedResponse<unknown>): string | undefined =>
      f.continuationToken,
  },
  {
    name: "queryByLastId",
    query: queryByLastId,
    getNext: (f: FeedResponse<unknown>): string | undefined =>
      // @ts-ignore
      f.resources[f.resources.length - 1].id,
  },
  {
    name: "queryByLastDate",
    query: queryByLastDate,
    getNext: (f: FeedResponse<unknown>): string | undefined =>
      // @ts-ignore
      f.resources[f.resources.length - 1]._ts,
  },
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
]
  .map(
    ({ name, query, getNext }: T) =>
      async (): Promise<Record<string, any>> => {
        let next: string | undefined;
        // eslint-disable-next-line @typescript-eslint/array-type,functional/prefer-readonly-type
        const stats = { name, rc: [] as Array<number>, el: 0 };
        while (true) {
          const iterable = query(next);

          const iterator = iterable[Symbol.asyncIterator]();

          const result = await iterator.next();

          const f: FeedResponse<unknown> = result.value;
          // @ts-ignore
          const res = f.resources[f.resources.length - 1] as any;
          if (!res) {
            console.log("exit", !!f, f?.hasMoreResults);
            break;
          }

          stats.rc.push(f.requestCharge);
          stats.el += f.resources.length;

          next = getNext(f);

          if (!f.hasMoreResults) {
            console.log("exit", !!f, f?.hasMoreResults);
            break;
          }
        }

        return {
          name,
          count:
            stats.el === nOfDocuments
              ? "ok"
              : `differ by ${stats.el - nOfDocuments}`,
          ruFirst: Number(stats.rc[0]),
          // ruMode: mode(stats.rc),
          ruMedian: median(stats.rc),
          ruMean: mean(stats.rc),
          ruPercentile85: percentile(stats.rc, 0.85),
          ruPercentile95: percentile(stats.rc, 0.95),
        };
      }
  )
  .map((p) => p());

const gStats = await Promise.all(promises);

await Promise.all(
  gStats
    .map((e) => [
      nOfDocuments,
      pageSize,
      e.name,
      e.ruFirst,
      e.ruMedian,
      e.ruPercentile85,
      e.ruPercentile95,
    ])
    .map((e) => e.join(";"))
    .map((e) =>
      fs.promises.appendFile(
        path.resolve(`${resultFileName}`),
        `${e}\n`,
        "utf-8"
      )
    )
);

// clean up test
await cosmosdbInstance.container(cosmosDbContainerName).delete();

import { QueryEngine } from "@comunica/query-sparql";
import { Store, Writer } from "n3";
import { rdfParser } from "rdf-parse";
import { Readable } from "stream";

/**
 * Collect a stream into an array.
 */
async function streamToArray(stream) {
  return await new Promise((resolve, reject) => {
    const items = [];
    stream.on("data", (item) => items.push(item));
    stream.on("error", reject);
    stream.on("end", () => resolve(items));
  });
}

function mustEnv(name) {
  const v = process.env[name];
  if (!v || !v.trim()) throw new Error(`Missing env var: ${name}`);
  return v;
}

async function fetchRdfToStore(url) {
  // Ask nicely for common RDF serializations
  const res = await fetch(url, {
    headers: {
      accept: [
        "text/turtle",
        "application/n-triples",
        "application/ld+json",
        "application/rdf+xml",
        "application/trig",
        "application/n-quads",
        "application/octet-stream;q=0.1",
        "*/*;q=0.01",
      ].join(", "),
    },
  });

  if (!res.ok) {
    throw new Error(`Failed to fetch RDF: ${res.status} ${res.statusText} (${url})`);
  }

  // Turn WHATWG ReadableStream into Node stream
  const nodeStream = Readable.fromWeb(res.body);

  // rdf-parse auto-detects by content-type + some sniffing
  const contentType = res.headers.get("content-type")?.split(";")[0]?.trim() || "text/turtle";
  const quadStream = rdfParser.parse(nodeStream, { contentType, baseIRI: url });

  const quads = await streamToArray(quadStream);
  const store = new Store(quads);
  return store;
}

async function quadsToTurtle(quads) {
  return await new Promise((resolve, reject) => {
    const writer = new Writer({ format: "text/turtle" });
    writer.addQuads(quads);
    writer.end((err, ttl) => (err ? reject(err) : resolve(ttl)));
  });
}

async function main() {
  const URL = mustEnv("URL");
  let QUERY = mustEnv("Query");
  const STORE = mustEnv("STORE");

  // Optional: choose how to write into the graph store
  // PUT = replace default graph; POST = append
  const METHOD = (process.env.METHOD || "POST").toUpperCase();
  if (!["PUT", "POST"].includes(METHOD)) {
    throw new Error(`METHOD must be PUT or POST (got: ${METHOD})`);
  }

  console.error(`[1/3] Fetching RDF from: ${URL}`);
  const sourceStore = await fetchRdfToStore(URL);
  console.error(`[1/3] Loaded quads: ${sourceStore.size}`);

  console.error(`[2/3] Evaluating SPARQL query...`);
  const engine = new QueryEngine();

  // For CONSTRUCT / DESCRIBE, queryQuads returns a quad stream
  const resultQuadStream = await engine.queryQuads(QUERY, {
    sources: [sourceStore],
  });

  const resultQuads = await streamToArray(resultQuadStream);
  console.error(`[2/3] Result quads: ${resultQuads.length}`);

  const ttl = await quadsToTurtle(resultQuads);

  console.error(`[3/3] Writing results to store: ${STORE} (METHOD=${METHOD})`);
  const writeRes = await fetch(STORE, {
    method: METHOD,
    headers: {
      "content-type": "text/turtle",
    },
    body: ttl,
  });

  if (!writeRes.ok) {
    const body = await writeRes.text().catch(() => "");
    throw new Error(`Failed to write to STORE: ${writeRes.status} ${writeRes.statusText}\n${body}`);
  }

  console.error(`[3/3] Done.`);
}

main().catch((e) => {
  console.error(`ERROR: ${e?.stack || e}`);
  process.exit(1);
});

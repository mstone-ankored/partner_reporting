import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { Pool } from "@neondatabase/serverless";

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is required");
  const pool = new Pool({ connectionString: url });

  const dir = path.resolve(__dirname, "..", "migrations");
  const files = (await readdir(dir)).filter((f) => f.endsWith(".sql")).sort();

  for (const f of files) {
    const sql = await readFile(path.join(dir, f), "utf8");
    console.log(`applying ${f}`);
    await pool.query(sql);
  }
  await pool.end();
  console.log("migrations complete");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

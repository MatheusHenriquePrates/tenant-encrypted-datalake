import duckdb from 'duckdb';

export type Row = Record<string, unknown>;

export async function rowsToParquetBuffer(rows: Row[], tableName: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(':memory:');
    const conn = db.connect();

    if (rows.length === 0) {
      db.close();
      reject(new Error('Cannot create Parquet from empty rows'));
      return;
    }

    const columns = Object.keys(rows[0]!);
    const tmpTable = `tmp_${tableName.replace(/[^a-zA-Z0-9_]/g, '_')}_${Date.now()}`;
    const tmpFile = `/tmp/${tmpTable}.parquet`;

    // Create table from first row to infer types
    const placeholders = columns.map(() => '?').join(', ');
    const colNames = columns.map((c) => `"${c}"`).join(', ');

    conn.run(`CREATE TABLE ${tmpTable} AS SELECT * FROM (VALUES (${placeholders})) AS t(${colNames}) WHERE 1=0`, (err) => {
      if (err) {
        // Fallback: VARCHAR columns
        const colDefs = columns.map((c) => `"${c}" VARCHAR`).join(', ');
        conn.run(`CREATE TABLE ${tmpTable} (${colDefs})`, (createErr) => {
          if (createErr) { db.close(); reject(createErr); return; }
          insertAndExport();
        });
        return;
      }
      insertAndExport();
    });

    function insertAndExport() {
      const stmt = conn.prepare(`INSERT INTO ${tmpTable} (${columns.map((c) => `"${c}"`).join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`);

      for (const row of rows) {
        const values = columns.map((c) => {
          const val = row[c];
          if (val === null || val === undefined) return null;
          if (val instanceof Date) return val.toISOString();
          if (typeof val === 'object') return JSON.stringify(val);
          return val;
        });
        stmt.run(...(values as (string | number | boolean | null)[]));
      }

      stmt.finalize(() => {
        conn.run(`COPY ${tmpTable} TO '${tmpFile}' (FORMAT PARQUET, COMPRESSION ZSTD)`, (exportErr) => {
          if (exportErr) { db.close(); reject(exportErr); return; }

          import('node:fs').then(({ readFileSync, unlinkSync }) => {
            const buffer = readFileSync(tmpFile);
            try { unlinkSync(tmpFile); } catch { /* ignore */ }
            db.close();
            resolve(buffer);
          });
        });
      });
    }
  });
}

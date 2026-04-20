"use server";

import { revalidatePath } from "next/cache";
import { z } from "zod";
import { auth } from "@/lib/auth";
import { pool, APP_SCHEMA } from "@/lib/db";
import { NOTION_SOURCES } from "@/lib/notion-targets";

const allowedSources = new Set(NOTION_SOURCES.map((s) => s.name));

const UpsertSchema = z.object({
  id: z.string().uuid().optional(),
  source_table: z.string().refine((s) => allowedSources.has(s), "Unknown source"),
  notion_database_id: z.string().min(10),
  enabled: z.boolean().default(true),
  filter_json: z.record(z.string(), z.unknown()).default({}),
  column_map_json: z.record(z.string(), z.string()).default({}),
});

export async function upsertTarget(formData: FormData) {
  const session = await auth();
  if (!session?.user) throw new Error("Not signed in");

  const raw = {
    id: (formData.get("id") as string) || undefined,
    source_table: formData.get("source_table") as string,
    notion_database_id: String(formData.get("notion_database_id") || "").trim(),
    enabled: formData.get("enabled") === "on" || formData.get("enabled") === "true",
    filter_json: safeJson(formData.get("filter_json")),
    column_map_json: safeJson(formData.get("column_map_json")),
  };
  const parsed = UpsertSchema.parse(raw);

  if (parsed.id) {
    await pool().query(
      `update ${APP_SCHEMA}.notion_sync_targets
       set notion_database_id=$2, enabled=$3, filter_json=$4, column_map_json=$5, updated_at=now()
       where id=$1`,
      [parsed.id, parsed.notion_database_id, parsed.enabled, parsed.filter_json, parsed.column_map_json],
    );
  } else {
    await pool().query(
      `insert into ${APP_SCHEMA}.notion_sync_targets
         (source_table, notion_database_id, enabled, filter_json, column_map_json, created_by)
       values ($1, $2, $3, $4, $5,
         (select id from ${APP_SCHEMA}.users where email=$6))`,
      [
        parsed.source_table,
        parsed.notion_database_id,
        parsed.enabled,
        parsed.filter_json,
        parsed.column_map_json,
        session.user.email,
      ],
    );
  }
  revalidatePath("/settings/notion");
}

export async function deleteTarget(id: string) {
  const session = await auth();
  if (!session?.user) throw new Error("Not signed in");
  await pool().query(
    `delete from ${APP_SCHEMA}.notion_sync_targets where id = $1`,
    [id],
  );
  revalidatePath("/settings/notion");
}

function safeJson(v: FormDataEntryValue | null): Record<string, unknown> {
  if (!v) return {};
  try {
    const parsed = JSON.parse(String(v));
    return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

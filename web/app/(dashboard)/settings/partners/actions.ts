"use server";

import { revalidatePath } from "next/cache";
import { z } from "zod";
import { auth } from "@/lib/auth";
import { pool, APP_SCHEMA } from "@/lib/db";

const app = APP_SCHEMA.replace(/[^a-zA-Z0-9_]/g, "");

const UpsertSchema = z.object({
  partner_id: z.string().min(1).max(64),
  as_of_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "Use YYYY-MM-DD"),
  total_customer_count: z.coerce.number().int().min(0),
  notes: z.string().max(500).optional().nullable(),
});

export async function upsertCustomerCount(formData: FormData) {
  const session = await auth();
  if (!session?.user) throw new Error("Not signed in");

  const parsed = UpsertSchema.parse({
    partner_id: formData.get("partner_id"),
    as_of_date: formData.get("as_of_date"),
    total_customer_count: formData.get("total_customer_count"),
    notes: (formData.get("notes") as string | null) || null,
  });

  await pool().query(
    `insert into ${app}.partner_customer_counts
       (partner_id, as_of_date, total_customer_count, notes, created_by)
     values ($1, $2, $3, $4,
       (select id from ${app}.users where email = $5))
     on conflict (partner_id, as_of_date)
     do update set
       total_customer_count = excluded.total_customer_count,
       notes                = excluded.notes,
       updated_at           = now()`,
    [
      parsed.partner_id,
      parsed.as_of_date,
      parsed.total_customer_count,
      parsed.notes,
      session.user.email,
    ],
  );
  revalidatePath("/settings/partners");
}

export async function deleteCustomerCount(id: string) {
  const session = await auth();
  if (!session?.user) throw new Error("Not signed in");

  await pool().query(
    `delete from ${app}.partner_customer_counts where id = $1`,
    [id],
  );
  revalidatePath("/settings/partners");
}

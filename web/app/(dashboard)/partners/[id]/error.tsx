"use client";

import { useEffect } from "react";

export default function PartnerDetailError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[partners/[id]] render error", {
      message: error.message,
      digest: error.digest,
      stack: error.stack,
    });
  }, [error]);

  return (
    <div className="p-6">
      <h1 className="text-lg font-semibold mb-2">Couldn&apos;t load this partner</h1>
      <p className="text-sm text-muted mb-1">
        {error.message || "An unexpected error occurred."}
      </p>
      {error.digest ? (
        <p className="text-xs text-muted mb-4">digest: {error.digest}</p>
      ) : null}
      <button
        onClick={reset}
        className="text-sm text-accent hover:underline"
      >
        Try again
      </button>
    </div>
  );
}

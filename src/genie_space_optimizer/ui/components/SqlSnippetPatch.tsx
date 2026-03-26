interface SqlSnippetPatchProps {
  patch: Record<string, unknown>;
}

const TYPE_LABELS: Record<string, { label: string; className: string }> = {
  measures: { label: "Measure", className: "bg-blue-100 text-blue-800" },
  filters: { label: "Filter", className: "bg-amber-100 text-amber-800" },
  expressions: { label: "Dimension", className: "bg-purple-100 text-purple-800" },
};

export function SqlSnippetPatch({ patch }: SqlSnippetPatchProps) {
  let command: Record<string, unknown> = {};
  try {
    command =
      typeof patch.command === "string"
        ? JSON.parse(patch.command)
        : (patch.command as Record<string, unknown>) || {};
  } catch {
    command = {};
  }

  const snippet =
    (command.snippet as Record<string, unknown>) ||
    (patch.sql_snippet as Record<string, unknown>) ||
    (patch.patch as Record<string, unknown>) ||
    {};
  const snippetType =
    (command.snippet_type as string) || (patch.snippet_type as string) || "unknown";
  const op = (command.op as string) || "add";

  const displayName =
    (snippet.display_name as string) || (snippet.alias as string) || "Unnamed";
  const sql = Array.isArray(snippet.sql)
    ? (snippet.sql as string[]).join("\n")
    : (snippet.sql as string) || "";
  const synonyms = (snippet.synonyms as string[]) || [];
  const instruction = Array.isArray(snippet.instruction)
    ? (snippet.instruction as string[]).join(" ")
    : (snippet.instruction as string) || "";

  const typeInfo = TYPE_LABELS[snippetType] ?? {
    label: snippetType,
    className: "bg-gray-100 text-gray-800",
  };
  const opLabel =
    op === "add" ? "Add" : op === "update" ? "Update" : op === "remove" ? "Remove" : op;

  return (
    <div className="space-y-2 rounded border bg-white p-3 text-xs">
      <div className="flex items-center gap-2">
        <span
          className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${typeInfo.className}`}
        >
          {typeInfo.label}
        </span>
        <span className="font-medium">
          {opLabel}: {displayName}
        </span>
      </div>

      {sql && (
        <pre className="overflow-auto rounded bg-elevated/40 p-2 text-[10px] font-mono">
          {sql}
        </pre>
      )}

      {synonyms.length > 0 && (
        <div className="flex flex-wrap gap-1">
          <span className="text-muted">Synonyms:</span>
          {synonyms.map((s, i) => (
            <span key={i} className="rounded bg-gray-100 px-1.5 py-0.5 text-[10px]">
              {s}
            </span>
          ))}
        </div>
      )}

      {instruction && <p className="text-muted italic">{instruction}</p>}
    </div>
  );
}

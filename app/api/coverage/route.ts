import { NextResponse } from "next/server";
import { buildCoverageForHandle } from "../../lib/coverage";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const handle = searchParams.get("handle") ?? "";
  const maxPages = Number(searchParams.get("maxPages") ?? "30");

  if (!handle.trim()) {
    return NextResponse.json({ error: "Eventernote handle is required." }, { status: 400 });
  }
  if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > 100) {
    return NextResponse.json({ error: "maxPages must be an integer between 1 and 100." }, { status: 400 });
  }

  try {
    const coverage = await buildCoverageForHandle(handle, maxPages);
    return NextResponse.json(coverage);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown coverage analysis error.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

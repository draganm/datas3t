# Replace templ Web UI with SolidJS SPA

## Problem

The current web UI uses Go templ templates for server-side rendering. The handler fetches all datas3ts and all their dataranges synchronously before rendering, blocking the page until everything is ready. There are no loading states, and the CDN-loaded Chart.js adds ~200KB. The UI is a single page with no room for client-side interactivity.

## Solution

Replace the templ-based UI with a SolidJS single-page application that:
- Loads instantly and fetches data asynchronously via the existing `/api/v1/*` REST endpoints
- Shows loading states per component (table loads first, charts fill in independently)
- Uses uPlot (~35KB) instead of Chart.js (~200KB)
- Gets built by Vite, output committed to git, embedded in the Go binary via `go:embed`

## Architecture

### Build Pipeline

```
ui/src/ → vite build → httpapi/webui/dist/ → go:embed → single binary
```

- SolidJS source lives in `ui/` at the project root
- Vite outputs to `httpapi/webui/dist/` (committed to git, same as current templ-generated files)
- Go embeds `dist/` via `//go:embed all:dist` (uses `all:` prefix to include subdirectories like `dist/assets/`)
- `go:generate` directive runs `npm ci && npm run build` in `ui/`
- Developers run `nix develop -c go generate ./httpapi/webui/...` and commit the results

### Serving

The Go handler becomes a pure static file server with SPA fallback:
- Requests for existing files (JS, CSS, assets) are served directly from the embedded filesystem
- All other requests (SPA routes) fall back to `index.html`
- No `*server.Server` or `*slog.Logger` dependencies — the handler is stateless

### Data Flow

1. Browser loads `index.html` + JS bundle from embedded assets
2. `Dashboard` component mounts, fetches `GET /api/v1/datas3ts`
3. Table renders immediately with datas3t metadata
4. Each `DatarangeSizeChart` component independently fetches `GET /api/v1/dataranges?datas3t_name=X`
5. Charts render as data arrives — no blocking

## Component Structure

```
App.tsx                    — Layout: container + heading
└── Dashboard.tsx          — createResource(fetchDatas3ts), handles loading/error/empty
    └── Datas3tTable.tsx   — Renders table rows with <For each={}>
        └── DatarangeSizeChart.tsx — Per-row: createResource(fetchDataranges), renders uPlot bar chart

EmptyState.tsx             — "No datas3ts found" placeholder
FormatBytes.ts             — Human-readable byte formatting utility
```

## File Structure

### New: `ui/` (SolidJS project root)

```
ui/
  package.json              — solid-js, uplot, vite, tailwindcss v4, vite-plugin-solid
  package-lock.json         — Committed for reproducible builds
  tsconfig.json
  vite.config.ts            — outDir: ../httpapi/webui/dist, proxy /api to Go in dev
  index.html                — Vite entry HTML
  src/
    index.tsx               — render(<App />, root)
    App.tsx
    styles/
      index.css             — @import "tailwindcss" (Tailwind v4)
    api/
      types.ts              — TypeScript interfaces matching Go JSON structs
      client.ts             — fetch wrappers for /api/v1/datas3ts and /api/v1/dataranges
    components/
      Dashboard.tsx
      Datas3tTable.tsx
      DatarangeSizeChart.tsx
      EmptyState.tsx
      FormatBytes.ts
```

### Modified: `httpapi/webui/`

```
httpapi/webui/
  handler.go     — REWRITTEN: static file server with SPA fallback, returns http.Handler
  generate.go    — REWRITTEN: //go:generate sh -c "cd ../../ui && npm ci && npm run build"
  embed.go       — NEW: //go:embed all:dist
  dist/          — Vite build output (COMMITTED to git)
    index.html
    assets/
      *.js
      *.css
```

**Deleted:** `components.templ`, `components_templ.go`

### Modified: `httpapi/http_api.go`

Line 21-22 changes from:
```go
webHandler := webui.NewHandler(s, log)
mux.HandleFunc("GET /", webHandler.IndexPage)
```
To:
```go
mux.Handle("GET /", webui.NewHandler())
```

## API Endpoints and Types

The SolidJS app consumes two endpoints with different response shapes:

- `GET /api/v1/datas3ts` — returns a **bare JSON array**: `Datas3tInfo[]`
- `GET /api/v1/dataranges?datas3t_name=X` — returns a **wrapped object**: `{ dataranges: DatarangeInfo[] }`

TypeScript interfaces (mirrors of Go structs):

```typescript
interface Datas3tInfo {
  datas3t_name: string;
  bucket_name: string;
  datarange_count: number;
  total_datapoints: number;
  lowest_datapoint: number;
  highest_datapoint: number;
  total_bytes: number;
}

interface DatarangeInfo {
  datarange_id: number;
  data_object_key: string;
  index_object_key: string;
  min_datapoint_key: number;
  max_datapoint_key: number;
  size_bytes: number;
}

interface ListDatarangesResponse {
  dataranges: DatarangeInfo[];
}
```

## Build Integration

### Nix (`flake.nix`)

Add `nodejs` to the devShell packages list.

### Dockerfile

Remove `RUN go generate ./...`. The Dockerfile only runs `go build` since all generated files (both sqlc database code and frontend dist/) are committed to git. No Node.js or sqlc needed in Docker.

### CI (`.github/workflows/run-tests-and-push-to-registry.yml`)

No changes needed. The existing workflow already:
1. Runs `nix develop --command go generate ./...` (will now also build the frontend since Node.js is in the flake)
2. Checks `git status --porcelain` for uncommitted changes
3. Runs tests

### `.gitignore`

Add `ui/node_modules/`. Do NOT gitignore `httpapi/webui/dist/` — it is committed.

### `go.mod`

Remove `github.com/a-h/templ/cmd/templ` from the `tool` section. Run `go mod tidy`.

## Development Workflow

### Frontend hot-reload development

1. `nix develop` — enter devshell (provides Go + Node.js)
2. Terminal 1: `go run ./cmd/datas3t server` — start Go API on port 8765
3. Terminal 2: `cd ui && npm run dev` — Vite dev server on port 3000
4. Open `http://localhost:3000` — Vite proxies `/api/*` to Go backend
5. Edit SolidJS code — HMR updates instantly

### Full rebuild

1. `nix develop -c go generate ./httpapi/webui/...` — builds frontend
2. `go run ./cmd/datas3t server` — serves embedded SPA at `http://localhost:8765`

## Dependencies

### npm (in `ui/package.json`)

| Package | Purpose |
|---------|---------|
| `solid-js` | UI framework |
| `uplot` | Lightweight charting |
| `tailwindcss` v4 | CSS utility framework |
| `@tailwindcss/vite` | Tailwind Vite plugin (v4 pattern) |
| `vite` | Build tool |
| `vite-plugin-solid` | SolidJS JSX transform |
| `typescript` | Type checking |

### Go (removed)

| Package | Reason |
|---------|--------|
| `github.com/a-h/templ` | No longer used — replaced by SolidJS |

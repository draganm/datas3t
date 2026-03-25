import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [solidPlugin(), tailwindcss()],
  build: {
    outDir: "../httpapi/webui/dist",
    emptyOutDir: true,
  },
  server: {
    port: 3000,
    proxy: {
      "/api": "http://localhost:8765",
    },
  },
});

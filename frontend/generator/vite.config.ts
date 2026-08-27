import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [svelte()],
  base: "./",
  server: {
    proxy: {
      "/api": {
        target: process.env.QCG_API_TARGET || process.env.VITE_QCG_API_TARGET || "http://127.0.0.1:8080",
        changeOrigin: true,
      },
      "/healthz": {
        target: process.env.QCG_API_TARGET || process.env.VITE_QCG_API_TARGET || "http://127.0.0.1:8080",
        changeOrigin: true,
      },
    },
  },
  build: {
    target: "es2022",
    outDir: "../../generators/generator/ui",
    emptyOutDir: true,
  },
});

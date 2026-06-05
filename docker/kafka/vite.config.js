import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath } from 'url'
import path from 'path'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// Detect if we're in Docker build (/build/kafka) or local dev (repo/docker/kafka)
const inDockerBuild = __dirname.startsWith('/build/')
const dashboardsPath = inDockerBuild
  ? path.resolve(__dirname, '../dashboards')     // Docker: /build/kafka → /build/dashboards
  : path.resolve(__dirname, '../../dashboards')  // Local: repo/docker/kafka → repo/dashboards

export default defineConfig({
  plugins: [react()],
  base: process.env.VITE_BASE ?? '/',
  resolve: {
    dedupe: ['react', 'react-dom', 'recharts'],
    alias: {
      '@dashboards': dashboardsPath,
    },
  },
  server: {
    fs: { allow: ['../../../dashboards', '.'] },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})

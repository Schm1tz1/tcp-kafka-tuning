import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/tcp-kafka-tuning/kafka/',
  resolve: {
    dedupe: ['react', 'react-dom', 'recharts'],
  },
  server: {
    fs: { allow: ['../../../dashboards', '.'] },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})

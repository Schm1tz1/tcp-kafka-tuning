import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/tcp-kafka-tuning/tcp/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
